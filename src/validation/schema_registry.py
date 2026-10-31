"""
schema_registry.py — Central YANG Schema Registry and Model Parser.
Loads .yang files, compiles yangson.DataModel for formal RFC 7950 / RFC 7951
validation, tracks revisions, and compiles node constraints.
"""
import glob
import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import yangson

logger = logging.getLogger("schema_registry")


@dataclass
class LeafConstraint:
    name: str
    yang_type: str
    mandatory: bool = False
    range_min: Optional[float] = None
    range_max: Optional[float] = None
    length_min: Optional[int] = None
    length_max: Optional[int] = None
    pattern: Optional[str] = None
    enums: Set[str] = field(default_factory=set)
    units: Optional[str] = None
    description: Optional[str] = None


@dataclass
class YangCompiledModule:
    name: str
    namespace: str
    prefix: str
    revision: str
    leafs: Dict[str, LeafConstraint] = field(default_factory=dict)
    containers: Set[str] = field(default_factory=set)
    lists: Dict[str, str] = field(default_factory=dict)  # list_name -> key_leaf


class SchemaRegistry:
    """
    Manages loaded YANG models and provides formal yangson.DataModel validation
    along with fast constraint lookups and RFC 8525 YANG Library export.
    """

    def __init__(self, schemas_dir: str):
        self.schemas_dir = schemas_dir
        self.modules: Dict[str, Dict[str, YangCompiledModule]] = {}  # module -> {rev: YangCompiledModule}
        self.device_schema_map: Dict[str, List[Tuple[str, str]]] = {}
        self.dm: Optional[yangson.DataModel] = None
        self.formal_yang_validation: bool = False

        self._load_all_schemas()
        self._init_yangson()

    def _init_yangson(self):
        try:
            yl_path = os.path.join(self.schemas_dir, "yang-library.json")
            if os.path.exists(yl_path):
                with open(yl_path, "r", encoding="utf-8") as f:
                    yl_content = f.read()
                self.dm = yangson.DataModel(yl_content, [self.schemas_dir])
                self.formal_yang_validation = True
                self._enrich_from_yangson_dm()
                logger.info("[SchemaRegistry] Formal yangson.DataModel successfully initialized.")
        except Exception as e:
            self.dm = None
            self.formal_yang_validation = False
            logger.warning(f"[SchemaRegistry] Yangson initialization warning: {e}")

    def _enrich_from_yangson_dm(self):
        """
        Dynamically introspects yangson DataModel AST to populate containers,
        lists, keys, and leaf constraints for registered modules.
        """
        if not self.dm:
            return

        try:
            for top_node in self.dm.schema.children:
                mod_name = top_node.ns
                mod = self.get_module(mod_name)
                if not mod:
                    continue

                if hasattr(top_node, "children"):
                    mod.containers.add(top_node.name)
                    self._walk_schema_children(top_node.children, mod)
        except Exception as ex:
            logger.debug(f"[SchemaRegistry] AST enrichment note: {ex}")

    def _walk_schema_children(self, children: List[Any], mod: YangCompiledModule):
        for child in children:
            cname = getattr(child, "name", "")
            ctype_name = type(child).__name__

            if "ContainerNode" in ctype_name:
                mod.containers.add(cname)
                if hasattr(child, "children"):
                    self._walk_schema_children(child.children, mod)
            elif "ListNode" in ctype_name:
                key_name = child.keys[0] if getattr(child, "keys", None) else ""
                mod.lists[cname] = key_name
                if hasattr(child, "children"):
                    self._walk_schema_children(child.children, mod)
            elif "LeafNode" in ctype_name:
                if cname not in mod.leafs:
                    mod.leafs[cname] = LeafConstraint(
                        name=cname,
                        yang_type="string",
                        mandatory=getattr(child, "mandatory", False),
                        description=getattr(child, "description", None),
                    )

    def _load_all_schemas(self):
        yang_files = glob.glob(os.path.join(self.schemas_dir, "*.yang"))
        for yf in yang_files:
            try:
                self._parse_and_register_yang(yf)
            except Exception as e:
                logger.warning(f"[SchemaRegistry] Warning: could not parse {yf}: {e}")

    def _parse_and_register_yang(self, file_path: str):
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        mod_match = re.search(r"module\s+([\w\-]+)\s*\{", content)
        if not mod_match:
            return
        mod_name = mod_match.group(1)

        ns_match = re.search(r'namespace\s+"([^"]+)";', content)
        prefix_match = re.search(r"prefix\s+([\w\-]+);", content)
        rev_match = re.search(r"revision\s+([\d\-]+)\s*\{", content)

        namespace = ns_match.group(1) if ns_match else ""
        prefix = prefix_match.group(1) if prefix_match else ""
        revision = rev_match.group(1) if rev_match else "unknown"

        compiled = YangCompiledModule(
            name=mod_name,
            namespace=namespace,
            prefix=prefix,
            revision=revision,
        )

        # Dynamic regex parsing for containers, lists, and leaves
        containers = re.findall(r"container\s+([\w\-]+)\s*\{", content)
        for c in containers:
            compiled.containers.add(c)

        lists = re.findall(r"list\s+([\w\-]+)\s*\{[^}]*?key\s+\"([\w\-]+)\";", content, re.DOTALL)
        for lname, lkey in lists:
            compiled.lists[lname] = lkey

        # Dynamic leaf extraction
        leaf_pattern = re.compile(r"leaf\s+([\w\-]+)\s*\{([^}]+(?:\}[^}]+)*)\}", re.DOTALL)
        for match in leaf_pattern.finditer(content):
            lname = match.group(1)
            lbody = match.group(2)

            t_match = re.search(r"type\s+([\w\-:]+)", lbody)
            raw_type = t_match.group(1) if t_match else "string"
            base_type = raw_type.split(":")[-1]

            mand = bool(re.search(r"mandatory\s+true\s*;", lbody))
            units_m = re.search(r'units\s+"([^"]+)"\s*;', lbody)
            units = units_m.group(1) if units_m else None

            desc_m = re.search(r'description\s+"([^"]+)"\s*;', lbody, re.DOTALL)
            desc = desc_m.group(1) if desc_m else None

            # Ranges
            rng_min, rng_max = None, None
            rng_m = re.search(r'range\s+"([^"]+)"', lbody)
            if rng_m:
                parts = rng_m.group(1).split("..")
                try:
                    rng_min = float(parts[0])
                    rng_max = float(parts[1]) if len(parts) > 1 else rng_min
                except ValueError:
                    pass

            # Lengths
            len_min, len_max = None, None
            len_m = re.search(r'length\s+"([^"]+)"', lbody)
            if len_m:
                parts = len_m.group(1).split("..")
                try:
                    len_min = int(parts[0])
                    len_max = int(parts[1]) if len(parts) > 1 else len_min
                except ValueError:
                    pass

            # Enums
            enums = set(re.findall(r"enum\s+([\w\-]+)\s*\{", lbody))

            compiled.leafs[lname] = LeafConstraint(
                name=lname,
                yang_type=base_type,
                mandatory=mand,
                range_min=rng_min,
                range_max=rng_max,
                length_min=len_min,
                length_max=len_max,
                enums=enums,
                units=units,
                description=desc,
            )

        self.modules.setdefault(mod_name, {})[revision] = compiled

    def get_module(self, module_name: str, revision: Optional[str] = None) -> Optional[YangCompiledModule]:
        revs = self.modules.get(module_name)
        if not revs:
            return None
        if revision and revision in revs:
            return revs[revision]
        # Return latest revision
        latest_rev = max(revs.keys())
        return revs[latest_rev]

    def resolve_modules_for_data(self, datastore_data: Dict[str, Any]) -> List[Tuple[str, YangCompiledModule]]:
        """
        RFC 7951: Keys in the root object are prefixed by module name: e.g. 'ietf-interfaces:interfaces'
        """
        results = []
        for key in datastore_data.keys():
            mod_prefix = key.split(":")[0] if ":" in key else key
            mod = self.get_module(mod_prefix)
            if mod:
                results.append((key, mod))
        return results

    def get_rfc8525_library(self) -> Dict[str, Any]:
        """
        Returns RFC 8525 YANG Library metadata JSON structure.
        """
        yl_path = os.path.join(self.schemas_dir, "yang-library.json")
        if os.path.exists(yl_path):
            with open(yl_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if "ietf-yang-library:yang-library" in data:
                    return data["ietf-yang-library:yang-library"]
                return data
        return {}

    def list_loaded_schemas(self) -> List[Dict[str, Any]]:
        schemas_info = []
        for mod_name, revs in self.modules.items():
            for rev, mod in revs.items():
                schemas_info.append({
                    "module": mod_name,
                    "revision": rev,
                    "namespace": mod.namespace,
                    "leaf_count": len(mod.leafs),
                    "containers": list(mod.containers),
                })
        return schemas_info

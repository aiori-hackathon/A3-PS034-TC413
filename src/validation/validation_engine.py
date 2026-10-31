"""
validation_engine.py — Complete 6-Stage YANG Schema Validation Pipeline.
Supports RFC 7951 / yangson formal snapshot validation and limited RFC 8072 YANG Patch on-change handling.
"""
import json
import re
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import yangson
from yangson.exceptions import (
    YangsonException,
    YangTypeError,
    SchemaError,
    RawTypeError,
    RawMemberError,
    SemanticError,
    ValidationError,
)

from .envelope_validator import EnvelopeValidator
from .schema_registry import LeafConstraint, SchemaRegistry, YangCompiledModule
from .validation_result import (
    ValidationErrorDetail,
    ValidationReport,
    ValidationStage,
    ValidationStatus,
)


class ValidationEngine:
    """
    Executes 6-stage formal YANG validation for incoming telemetry streams:
      Stage 1: Syntax Validation (JSON parsing, top-level type)
      Stage 2: Envelope Validation (RFC 8641 / RFC 8639 push notification envelope)
      Stage 3: Schema Resolution (identifies relevant YANG module & revision)
      Stage 4: Structural Validation (containers, list keying, node hierarchy via yangson)
      Stage 5: Type & Constraint Validation (ranges, enums, length, types via yangson)
      Stage 6: Semantic Validation (NMDA operational rules & cross-field semantics)
    """

    def __init__(self, registry: SchemaRegistry):
        self.registry = registry

    def validate(self, raw_input: Union[bytes, str, Dict[str, Any]]) -> Tuple[bool, Optional[Dict[str, Any]], ValidationReport]:
        start_time = time.perf_counter()
        report = ValidationReport(
            is_valid=False,
            status=ValidationStatus.INVALID,
        )

        # -------------------------------------------------------------
        # STAGE 1: Syntax Validation
        # -------------------------------------------------------------
        parsed_data = None
        if isinstance(raw_input, (bytes, bytearray)):
            try:
                parsed_data = json.loads(raw_input.decode("utf-8"))
            except Exception as e:
                report.failed_at_stage = ValidationStage.SYNTAX
                report.errors.append(ValidationErrorDetail(
                    stage=ValidationStage.SYNTAX,
                    path="/",
                    error_code="JSON_SYNTAX_ERROR",
                    message=f"Corrupted or invalid JSON payload: {str(e)}",
                    actual="malformed bytes",
                ))
                report.duration_ms = (time.perf_counter() - start_time) * 1000
                return False, None, report
        elif isinstance(raw_input, str):
            try:
                parsed_data = json.loads(raw_input)
            except Exception as e:
                report.failed_at_stage = ValidationStage.SYNTAX
                report.errors.append(ValidationErrorDetail(
                    stage=ValidationStage.SYNTAX,
                    path="/",
                    error_code="JSON_SYNTAX_ERROR",
                    message=f"Corrupted or invalid JSON string: {str(e)}",
                    actual=raw_input[:100],
                ))
                report.duration_ms = (time.perf_counter() - start_time) * 1000
                return False, None, report
        elif isinstance(raw_input, dict):
            parsed_data = raw_input
        else:
            report.failed_at_stage = ValidationStage.SYNTAX
            report.errors.append(ValidationErrorDetail(
                stage=ValidationStage.SYNTAX,
                path="/",
                error_code="UNSUPPORTED_INPUT_TYPE",
                message=f"Input must be bytes, string, or dict, got {type(raw_input).__name__}",
            ))
            report.duration_ms = (time.perf_counter() - start_time) * 1000
            return False, None, report

        if not isinstance(parsed_data, dict):
            report.failed_at_stage = ValidationStage.SYNTAX
            report.errors.append(ValidationErrorDetail(
                stage=ValidationStage.SYNTAX,
                path="/",
                error_code="TOP_LEVEL_NOT_OBJECT",
                message="Top level payload must be a JSON object",
                actual=type(parsed_data).__name__,
            ))
            report.duration_ms = (time.perf_counter() - start_time) * 1000
            return False, None, report

        report.stages_passed.append(ValidationStage.SYNTAX)

        # -------------------------------------------------------------
        # STAGE 2: Envelope Validation (RFC 8641 / RFC 8639)
        # -------------------------------------------------------------
        env_ok, envelope_meta, env_err = EnvelopeValidator.validate(parsed_data)
        if not env_ok or envelope_meta is None:
            report.failed_at_stage = ValidationStage.ENVELOPE
            if env_err:
                report.errors.append(env_err)
            report.duration_ms = (time.perf_counter() - start_time) * 1000
            return False, None, report

        report.subscription_id = envelope_meta["subscription_id"]
        report.event_time = envelope_meta["event_time"]
        report.stages_passed.append(ValidationStage.ENVELOPE)

        push_type = envelope_meta["type"]

        # -------------------------------------------------------------
        # BRANCH A: On-Change Subscriptions with limited YANG Patch
        # -------------------------------------------------------------
        if push_type == "push-change-update":
            return self._validate_yang_patch_on_change(envelope_meta, start_time, report)

        # -------------------------------------------------------------
        # BRANCH B: Periodic Snapshot Datastore Contents
        # -------------------------------------------------------------
        datastore_data = envelope_meta["datastore_data"]

        # STAGE 3: Schema Resolution
        resolved_modules = self.registry.resolve_modules_for_data(datastore_data)
        if not resolved_modules:
            report.failed_at_stage = ValidationStage.RESOLUTION
            report.errors.append(ValidationErrorDetail(
                stage=ValidationStage.RESOLUTION,
                path="/ietf-notification:notification/ietf-yang-push:push-update/datastore-contents",
                error_code="UNKNOWN_YANG_SCHEMA",
                message=f"No matching YANG schema registered for root prefixes: {list(datastore_data.keys())}",
                expected="Known registered YANG module (e.g., ietf-interfaces, custom-telemetry)",
                actual=list(datastore_data.keys()),
            ))
            report.duration_ms = (time.perf_counter() - start_time) * 1000
            return False, None, report

        report.schema_module = ", ".join([mod.name for _, mod in resolved_modules])
        report.stages_passed.append(ValidationStage.RESOLUTION)

        # STAGE 4 & 5: Formal yangson validation + supplementary checks
        stage_errors: List[ValidationErrorDetail] = []

        # 1. Primary: Formal yangson DataModel Validation
        run_formal_validation = (
            self.registry.formal_yang_validation
            and self.registry.dm is not None
        )

        if run_formal_validation:
            try:
                # RFC 7951: counter64/uint64 must be JSON strings; decimal64 must be JSON string
                inst = self.registry.dm.from_raw(datastore_data)
                inst.validate()
            except YangsonException as ye:
                err_str = str(ye)
                err_path = "/"

                # Extract exact node path from {path} format in yangson exception message
                path_match = re.match(r"^\{([^}]+)\}", err_str)
                if path_match:
                    err_path = path_match.group(1)
                elif err_str.startswith("/"):
                    err_path = err_str.split()[0]
                elif hasattr(ye, "path"):
                    err_path = str(getattr(ye, "path", "/"))

                # Classify error type to the accurate validation stage and error code
                if isinstance(ye, RawMemberError):
                    stage_errors.append(ValidationErrorDetail(
                        stage=ValidationStage.STRUCTURE,
                        path=err_path,
                        error_code="UNEXPECTED_SCHEMA_MEMBER",
                        message=f"YANG structural error (unexpected child element): {err_str}",
                    ))
                elif isinstance(ye, SchemaError) and "missing-data" in err_str:
                    stage_errors.append(ValidationErrorDetail(
                        stage=ValidationStage.STRUCTURE,
                        path=err_path,
                        error_code="MISSING_MANDATORY_LEAF",
                        message=f"YANG structural error (missing mandatory data): {err_str}",
                    ))
                elif isinstance(ye, SchemaError):
                    stage_errors.append(ValidationErrorDetail(
                        stage=ValidationStage.STRUCTURE,
                        path=err_path,
                        error_code="SCHEMA_STRUCTURAL_ERROR",
                        message=f"YANG structural error: {err_str}",
                    ))
                elif "enumeration" in err_str:
                    stage_errors.append(ValidationErrorDetail(
                        stage=ValidationStage.CONSTRAINTS,
                        path=err_path,
                        error_code="ENUM_VIOLATION",
                        message=f"YANG constraint error (invalid enumeration value): {err_str}",
                    ))
                elif "expected uint" in err_str:
                    stage_errors.append(ValidationErrorDetail(
                        stage=ValidationStage.CONSTRAINTS,
                        path=err_path,
                        error_code="UNSIGNED_CONSTRAINT_VIOLATION",
                        message=f"YANG constraint error (unsigned integer constraint violated): {err_str}",
                    ))
                elif "not in range" in err_str:
                    stage_errors.append(ValidationErrorDetail(
                        stage=ValidationStage.CONSTRAINTS,
                        path=err_path,
                        error_code="RANGE_VIOLATION",
                        message=f"YANG constraint error (value outside range bounds): {err_str}",
                    ))
                elif isinstance(ye, (YangTypeError, RawTypeError)):
                    stage_errors.append(ValidationErrorDetail(
                        stage=ValidationStage.CONSTRAINTS,
                        path=err_path,
                        error_code="TYPE_MISMATCH",
                        message=f"YANG type error: {err_str}",
                    ))
                else:
                    stage_errors.append(ValidationErrorDetail(
                        stage=ValidationStage.CONSTRAINTS,
                        path=err_path,
                        error_code="YANG_VALIDATION_ERROR",
                        message=f"YANG validation error: {err_str}",
                    ))
            except Exception as ex:
                stage_errors.append(ValidationErrorDetail(
                    stage=ValidationStage.CONSTRAINTS,
                    path="/",
                    error_code="YANG_UNEXPECTED_VALIDATION_ERROR",
                    message=f"Unexpected error in formal YANG validation: {str(ex)}",
                ))

        # 2. Supplementary diagnostic checks (range overflow, negative unsigned, etc.)
        for root_key, module in resolved_modules:
            content_tree = datastore_data[root_key]
            if module.name == "ietf-interfaces":
                self._validate_ietf_interfaces(content_tree, module, f"/{root_key}", stage_errors)
            elif module.name == "custom-telemetry":
                self._validate_custom_telemetry(content_tree, module, f"/{root_key}", stage_errors)

        if stage_errors:
            has_structural = any(e.stage == ValidationStage.STRUCTURE for e in stage_errors)
            report.failed_at_stage = ValidationStage.STRUCTURE if has_structural else ValidationStage.CONSTRAINTS
            report.errors.extend(stage_errors)
            report.duration_ms = (time.perf_counter() - start_time) * 1000
            return False, envelope_meta, report

        report.stages_passed.append(ValidationStage.STRUCTURE)
        report.stages_passed.append(ValidationStage.CONSTRAINTS)

        # STAGE 6: Semantic Validation
        semantic_errors: List[ValidationErrorDetail] = []
        self._validate_semantics(datastore_data, semantic_errors)

        if semantic_errors:
            report.failed_at_stage = ValidationStage.SEMANTICS
            report.errors.extend(semantic_errors)
            report.duration_ms = (time.perf_counter() - start_time) * 1000
            return False, envelope_meta, report

        report.stages_passed.append(ValidationStage.SEMANTICS)
        report.is_valid = True
        report.status = ValidationStatus.VALID
        report.duration_ms = (time.perf_counter() - start_time) * 1000

        return True, envelope_meta, report

    def _validate_yang_patch_on_change(
        self, envelope_meta: Dict[str, Any], start_time: float, report: ValidationReport
    ) -> Tuple[bool, Optional[Dict[str, Any]], ValidationReport]:
        """
        Limited RFC 8072 YANG Patch on-change handler for push-change-update.
        Validates target path, edit operation, and applied payload values.
        """
        report.schema_module = "ietf-yang-patch"
        report.stages_passed.append(ValidationStage.RESOLUTION)

        datastore_changes = envelope_meta.get("datastore_data", {})
        yang_patch = datastore_changes.get("ietf-yang-patch:yang-patch")
        if not yang_patch or not isinstance(yang_patch, dict):
            report.failed_at_stage = ValidationStage.STRUCTURE
            report.errors.append(ValidationErrorDetail(
                stage=ValidationStage.STRUCTURE,
                path="/ietf-notification:notification/ietf-yang-push:push-change-update/datastore-changes",
                error_code="MISSING_YANG_PATCH",
                message="push-change-update must contain 'ietf-yang-patch:yang-patch' object",
            ))
            report.duration_ms = (time.perf_counter() - start_time) * 1000
            return False, envelope_meta, report

        patch_id = yang_patch.get("patch-id")
        if not patch_id:
            report.failed_at_stage = ValidationStage.STRUCTURE
            report.errors.append(ValidationErrorDetail(
                stage=ValidationStage.STRUCTURE,
                path="/ietf-yang-patch:yang-patch/patch-id",
                error_code="MISSING_PATCH_ID",
                message="YANG Patch must contain a non-empty 'patch-id'",
            ))
            report.duration_ms = (time.perf_counter() - start_time) * 1000
            return False, envelope_meta, report

        edits = yang_patch.get("edit")
        if not isinstance(edits, list) or not edits:
            report.failed_at_stage = ValidationStage.STRUCTURE
            report.errors.append(ValidationErrorDetail(
                stage=ValidationStage.STRUCTURE,
                path="/ietf-yang-patch:yang-patch/edit",
                error_code="EMPTY_PATCH_EDIT",
                message="YANG Patch must contain at least one edit operation in 'edit' array",
            ))
            report.duration_ms = (time.perf_counter() - start_time) * 1000
            return False, envelope_meta, report

        valid_oper_enums = {"up", "down", "testing", "unknown", "dormant", "not-present", "lower-layer-down"}

        for idx, edit in enumerate(edits):
            edit_path = f"/ietf-yang-patch:yang-patch/edit[{idx}]"
            if not isinstance(edit, dict):
                report.failed_at_stage = ValidationStage.STRUCTURE
                report.errors.append(ValidationErrorDetail(
                    stage=ValidationStage.STRUCTURE,
                    path=edit_path,
                    error_code="INVALID_EDIT_OBJECT",
                    message="Edit entry must be a JSON object",
                ))
                report.duration_ms = (time.perf_counter() - start_time) * 1000
                return False, envelope_meta, report

            op = edit.get("operation")
            if op not in ("replace", "merge", "create", "delete", "remove"):
                report.failed_at_stage = ValidationStage.CONSTRAINTS
                report.errors.append(ValidationErrorDetail(
                    stage=ValidationStage.CONSTRAINTS,
                    path=f"{edit_path}/operation",
                    error_code="UNSUPPORTED_PATCH_OPERATION",
                    message=f"Operation '{op}' is not a valid RFC 8072 operation (replace, merge, create, delete, remove)",
                ))
                report.duration_ms = (time.perf_counter() - start_time) * 1000
                return False, envelope_meta, report

            target = edit.get("target", "")
            value_obj = edit.get("value", {})

            # Check target path support
            if "/oper-status" in target:
                oper_val = value_obj.get("oper-status")
                if oper_val not in valid_oper_enums:
                    report.failed_at_stage = ValidationStage.CONSTRAINTS
                    report.errors.append(ValidationErrorDetail(
                        stage=ValidationStage.CONSTRAINTS,
                        path=f"{edit_path}/value/oper-status",
                        error_code="ENUM_VIOLATION",
                        message=f"Value '{oper_val}' is not a valid oper-status enumeration",
                        expected=sorted(list(valid_oper_enums)),
                        actual=oper_val,
                    ))
                    report.duration_ms = (time.perf_counter() - start_time) * 1000
                    return False, envelope_meta, report

            elif "/cpu-utilization" in target:
                cpu_val = value_obj.get("cpu-utilization")
                if not isinstance(cpu_val, (int, float)) or cpu_val < 0 or cpu_val > 100:
                    report.failed_at_stage = ValidationStage.CONSTRAINTS
                    report.errors.append(ValidationErrorDetail(
                        stage=ValidationStage.CONSTRAINTS,
                        path=f"{edit_path}/value/cpu-utilization",
                        error_code="RANGE_VIOLATION_OVERFLOW" if (isinstance(cpu_val, (int, float)) and cpu_val > 100) else "TYPE_MISMATCH",
                        message=f"CPU utilization must be uint8 (0..100), got {cpu_val}",
                    ))
                    report.duration_ms = (time.perf_counter() - start_time) * 1000
                    return False, envelope_meta, report

            else:
                report.failed_at_stage = ValidationStage.CONSTRAINTS
                report.errors.append(ValidationErrorDetail(
                    stage=ValidationStage.CONSTRAINTS,
                    path=f"{edit_path}/target",
                    error_code="UNSUPPORTED_PATCH_TARGET",
                    message=f"Target '{target}' is not supported by the limited on-change patch handler",
                    expected="Path to /oper-status or /cpu-utilization",
                    actual=target,
                ))
                report.duration_ms = (time.perf_counter() - start_time) * 1000
                return False, envelope_meta, report

        report.stages_passed.append(ValidationStage.STRUCTURE)
        report.stages_passed.append(ValidationStage.CONSTRAINTS)
        report.stages_passed.append(ValidationStage.SEMANTICS)
        report.is_valid = True
        report.status = ValidationStatus.VALID
        report.duration_ms = (time.perf_counter() - start_time) * 1000

        envelope_meta["is_patch"] = True
        envelope_meta["patch_edits"] = edits
        return True, envelope_meta, report

    def _validate_ietf_interfaces(
        self, data: Any, module: YangCompiledModule, current_path: str, errors: List[ValidationErrorDetail]
    ):
        if not isinstance(data, dict):
            errors.append(ValidationErrorDetail(
                stage=ValidationStage.STRUCTURE,
                path=current_path,
                error_code="EXPECTED_CONTAINER",
                message=f"Node {current_path} must be a container object",
                expected="dict",
                actual=type(data).__name__,
            ))
            return

        if "interface" not in data:
            errors.append(ValidationErrorDetail(
                stage=ValidationStage.STRUCTURE,
                path=f"{current_path}/interface",
                error_code="MISSING_LIST",
                message="Missing mandatory list 'interface' under ietf-interfaces:interfaces",
                expected="list of interface entries",
            ))
            return

        interfaces = data["interface"]
        if not isinstance(interfaces, list):
            errors.append(ValidationErrorDetail(
                stage=ValidationStage.STRUCTURE,
                path=f"{current_path}/interface",
                error_code="EXPECTED_LIST",
                message="'interface' must be a JSON array (YANG list)",
                expected="list",
                actual=type(interfaces).__name__,
            ))
            return

        for idx, iface in enumerate(interfaces):
            iface_path = f"{current_path}/interface[{idx}]"
            if not isinstance(iface, dict):
                errors.append(ValidationErrorDetail(
                    stage=ValidationStage.STRUCTURE,
                    path=iface_path,
                    error_code="INVALID_LIST_ENTRY",
                    message="Interface entry must be a dictionary",
                ))
                continue

            # Check Key leaf 'name'
            if "name" not in iface or not iface["name"]:
                errors.append(ValidationErrorDetail(
                    stage=ValidationStage.STRUCTURE,
                    path=f"{iface_path}/name",
                    error_code="MISSING_MANDATORY_KEY",
                    message="Missing mandatory list key 'name' on interface entry",
                    expected="non-empty string name",
                ))
            else:
                name_val = iface["name"]
                if not isinstance(name_val, str):
                    errors.append(ValidationErrorDetail(
                        stage=ValidationStage.CONSTRAINTS,
                        path=f"{iface_path}/name",
                        error_code="TYPE_MISMATCH",
                        message=f"Interface name must be string, got {type(name_val).__name__}",
                        expected="string",
                        actual=name_val,
                    ))

            # Check Mandatory Leaf 'oper-status'
            if "oper-status" not in iface:
                errors.append(ValidationErrorDetail(
                    stage=ValidationStage.STRUCTURE,
                    path=f"{iface_path}/oper-status",
                    error_code="MISSING_MANDATORY_LEAF",
                    message="Missing mandatory leaf 'oper-status' on interface",
                    expected="oper-status enum",
                ))
            else:
                status_val = iface["oper-status"]
                valid_statuses = {"up", "down", "testing", "unknown", "dormant", "not-present", "lower-layer-down"}
                if status_val not in valid_statuses:
                    errors.append(ValidationErrorDetail(
                        stage=ValidationStage.CONSTRAINTS,
                        path=f"{iface_path}/oper-status",
                        error_code="ENUM_VIOLATION",
                        message=f"Invalid oper-status value '{status_val}'",
                        expected=sorted(list(valid_statuses)),
                        actual=status_val,
                    ))

            # Check Statistics if present
            if "statistics" in iface:
                stats = iface["statistics"]
                if not isinstance(stats, dict):
                    errors.append(ValidationErrorDetail(
                        stage=ValidationStage.STRUCTURE,
                        path=f"{iface_path}/statistics",
                        error_code="EXPECTED_CONTAINER",
                        message="Statistics must be a container object",
                    ))
                else:
                    for stat_leaf in ["in-octets", "out-octets", "in-errors", "out-errors", "in-discards"]:
                        if stat_leaf in stats:
                            self._validate_leaf_constraint(
                                stats[stat_leaf],
                                module.leafs.get(stat_leaf, LeafConstraint(name=stat_leaf, yang_type="counter32", range_min=0)),
                                f"{iface_path}/statistics/{stat_leaf}",
                                errors,
                            )

    def _validate_custom_telemetry(
        self, data: Any, module: YangCompiledModule, current_path: str, errors: List[ValidationErrorDetail]
    ):
        if not isinstance(data, dict):
            errors.append(ValidationErrorDetail(
                stage=ValidationStage.STRUCTURE,
                path=current_path,
                error_code="EXPECTED_CONTAINER",
                message=f"Node {current_path} must be a container object",
                expected="dict",
                actual=type(data).__name__,
            ))
            return

        for leaf_name in ["cpu-utilization", "memory-utilization", "throughput-bps", "rtt-latency-ms"]:
            if leaf_name in data:
                constraint = module.leafs.get(leaf_name)
                if constraint:
                    self._validate_leaf_constraint(
                        data[leaf_name], constraint, f"{current_path}/{leaf_name}", errors
                    )

    def _validate_leaf_constraint(
        self, val: Any, constraint: LeafConstraint, leaf_path: str, errors: List[ValidationErrorDetail]
    ):
        leaf_name = constraint.name

        if constraint.yang_type in ("uint8", "uint16", "uint32", "uint64", "counter32", "counter64"):
            int_val = None
            if isinstance(val, int) and not isinstance(val, bool):
                int_val = val
            elif isinstance(val, str):
                try:
                    int_val = int(val)
                except ValueError:
                    errors.append(ValidationErrorDetail(
                        stage=ValidationStage.CONSTRAINTS,
                        path=leaf_path,
                        error_code="TYPE_MISMATCH",
                        message=f"Leaf '{leaf_name}' expected integer value, got non-numeric string '{val}'",
                        expected="integer string",
                        actual=val,
                    ))
                    return
            else:
                errors.append(ValidationErrorDetail(
                    stage=ValidationStage.CONSTRAINTS,
                    path=leaf_path,
                    error_code="TYPE_MISMATCH",
                    message=f"Leaf '{leaf_name}' expected unsigned integer, got {type(val).__name__}",
                    expected="integer",
                    actual=val,
                ))
                return

            if int_val < 0:
                errors.append(ValidationErrorDetail(
                    stage=ValidationStage.CONSTRAINTS,
                    path=leaf_path,
                    error_code="UNSIGNED_CONSTRAINT_VIOLATION",
                    message=f"Leaf '{leaf_name}' is unsigned ({constraint.yang_type}) and cannot be negative",
                    expected=">= 0",
                    actual=val,
                ))
                return

            if constraint.range_min is not None and int_val < constraint.range_min:
                errors.append(ValidationErrorDetail(
                    stage=ValidationStage.CONSTRAINTS,
                    path=leaf_path,
                    error_code="RANGE_VIOLATION_UNDERFLOW",
                    message=f"Value {int_val} for '{leaf_name}' is below minimum allowed range ({constraint.range_min})",
                    expected=f">= {constraint.range_min}",
                    actual=int_val,
                ))
            if constraint.range_max is not None and int_val > constraint.range_max:
                errors.append(ValidationErrorDetail(
                    stage=ValidationStage.CONSTRAINTS,
                    path=leaf_path,
                    error_code="RANGE_VIOLATION_OVERFLOW",
                    message=f"Value {int_val} for '{leaf_name}' exceeds maximum allowed range ({constraint.range_max})",
                    expected=f"<= {constraint.range_max}",
                    actual=int_val,
                ))

        elif constraint.yang_type == "decimal64":
            dec_val = None
            if isinstance(val, (int, float)) and not isinstance(val, bool):
                dec_val = float(val)
            elif isinstance(val, str):
                try:
                    dec_val = float(val)
                except ValueError:
                    errors.append(ValidationErrorDetail(
                        stage=ValidationStage.CONSTRAINTS,
                        path=leaf_path,
                        error_code="TYPE_MISMATCH",
                        message=f"Leaf '{leaf_name}' expected decimal64, got non-numeric string '{val}'",
                    ))
                    return
            else:
                errors.append(ValidationErrorDetail(
                    stage=ValidationStage.CONSTRAINTS,
                    path=leaf_path,
                    error_code="TYPE_MISMATCH",
                    message=f"Leaf '{leaf_name}' expected decimal64, got {type(val).__name__}",
                ))
                return

            if constraint.range_min is not None and dec_val < constraint.range_min:
                errors.append(ValidationErrorDetail(
                    stage=ValidationStage.CONSTRAINTS,
                    path=leaf_path,
                    error_code="RANGE_VIOLATION_UNDERFLOW",
                    message=f"Value {dec_val} for '{leaf_name}' is below minimum allowed range ({constraint.range_min})",
                ))
            if constraint.range_max is not None and dec_val > constraint.range_max:
                errors.append(ValidationErrorDetail(
                    stage=ValidationStage.CONSTRAINTS,
                    path=leaf_path,
                    error_code="RANGE_VIOLATION_OVERFLOW",
                    message=f"Value {dec_val} for '{leaf_name}' exceeds maximum allowed range ({constraint.range_max})",
                ))

    def _validate_semantics(self, datastore_data: Dict[str, Any], errors: List[ValidationErrorDetail]):
        """
        Stage 6: NMDA operational datastore semantic validation & cross-field checks.
        """
        for root_key, val in datastore_data.items():
            if "interfaces" in root_key and isinstance(val, dict):
                interfaces = val.get("interface", [])
                if isinstance(interfaces, list):
                    for idx, iface in enumerate(interfaces):
                        if isinstance(iface, dict):
                            oper_status = iface.get("oper-status")
                            stats = iface.get("statistics", {})
                            in_oct = stats.get("in-octets", 0)
                            in_oct_val = int(in_oct) if str(in_oct).isdigit() else 0

                            # Case 1: Interface in test state cannot pass extreme active production traffic
                            if oper_status == "testing" and in_oct_val > 10**12:
                                errors.append(ValidationErrorDetail(
                                    stage=ValidationStage.SEMANTICS,
                                    path=f"/{root_key}/interface[{idx}]",
                                    error_code="SEMANTIC_INCONSISTENCY",
                                    message="Interface in test state cannot have extreme active traffic",
                                ))

                            # Case 2: Interface marked 'down' cannot report high error count growth without alert
                            in_err = stats.get("in-errors", 0)
                            in_err_val = int(in_err) if str(in_err).isdigit() else 0
                            if oper_status == "down" and in_err_val > 1000000:
                                errors.append(ValidationErrorDetail(
                                    stage=ValidationStage.SEMANTICS,
                                    path=f"/{root_key}/interface[{idx}]/statistics/in-errors",
                                    error_code="SEMANTIC_INCONSISTENCY",
                                    message="Interface marked oper-status down cannot have excessive error churn",
                                ))

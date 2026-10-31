"""
normalizer.py — Converts validated YANG telemetry payloads into Canonical TelemetryRecords.
Supports periodic snapshot push-update and on-change YANG Patch push-change-update.
"""
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class QualityFlag(str, Enum):
    GOOD = "GOOD"
    LATE = "LATE"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    INTERPOLATED = "INTERPOLATED"
    DUPLICATE = "DUPLICATE"
    ESTIMATED = "ESTIMATED"


class ValidationStatus(str, Enum):
    VALID = "VALID"
    INVALID = "INVALID"
    PARTIAL = "PARTIAL"
    SKIPPED = "SKIPPED"


class TelemetryRecord(BaseModel):
    """
    Canonical internal telemetry record — one per metric per device per timestamp.
    Normalized from vendor-specific and YANG models.
    """
    # Device Identity
    device_id: str = Field(description="Unique device identifier, e.g., 'core-router-01'")
    device_type: str = Field(description="Device category: router, switch, firewall")
    vendor: str = Field(description="Device vendor: cisco, juniper, arista, paloalto")
    model: Optional[str] = Field(None, description="Device model: 'ASR-9001'")

    # Schema Context
    schema_module: str = Field(description="YANG module name: 'ietf-interfaces', 'custom-telemetry'")
    schema_revision: str = Field(default="2018-02-20", description="YANG module revision")

    # Subscription Context
    subscription_id: int = Field(description="YANG-Push subscription ID")
    subscription_type: str = Field(default="periodic", description="'periodic' or 'on-change'")

    # Timestamps
    event_time: str = Field(description="ISO 8601 when event occurred on the device")
    ingestion_time: str = Field(description="ISO 8601 when receiver ingested message")
    processing_time: Optional[str] = Field(None, description="ISO 8601 when normalization completed")

    # Metric Data
    path: str = Field(description="YANG path e.g. /interfaces/interface[name=eth0]/statistics/in-octets")
    metric_name: str = Field(description="Normalized metric: 'cpu_utilization', 'memory_utilization', 'latency_ms', etc.")
    value: float = Field(description="Numeric measurement value")
    unit: Optional[str] = Field(None, description="Unit: 'percent', 'bytes', 'milliseconds', 'bps'")

    # Metadata & Quality
    source: str = Field(default="", description="Original YANG path")
    sequence: int = Field(default=0, description="Monotonic sequence number assigned at ingestion")
    validation_status: ValidationStatus = ValidationStatus.VALID
    validation_errors: List[str] = Field(default_factory=list)
    quality_flags: List[QualityFlag] = Field(default_factory=lambda: [QualityFlag.GOOD])


class Normalizer:
    """
    Extracts individual metrics from validated RFC 8641 notifications and maps
    them to standard TelemetryRecord instances.
    """

    @staticmethod
    def normalize(
        envelope_meta: Dict[str, Any],
        device_id: str,
        device_meta: Dict[str, Any],
        sequence: int,
        ingestion_time: str,
    ) -> List[TelemetryRecord]:
        now_iso = datetime.now(timezone.utc).isoformat()
        records: List[TelemetryRecord] = []
        event_time = envelope_meta.get("event_time", now_iso)
        sub_id = envelope_meta.get("subscription_id", 0)
        sub_type = envelope_meta.get("type", "periodic")

        vendor = device_meta.get("vendor", "generic")
        dev_type = device_meta.get("device_type", "router")
        model = device_meta.get("model", "unknown")

        # Handle On-Change YANG Patch
        if envelope_meta.get("is_patch"):
            patch_edits = envelope_meta.get("patch_edits", [])
            for edit in patch_edits:
                target = edit.get("target", "")
                val_dict = edit.get("value", {})
                if "oper-status" in target or "oper-status" in val_dict:
                    status_str = val_dict.get("oper-status", "unknown")
                    num_val = 1.0 if status_str == "up" else 0.0
                    records.append(TelemetryRecord(
                        device_id=device_id,
                        device_type=dev_type,
                        vendor=vendor,
                        model=model,
                        schema_module="ietf-interfaces",
                        schema_revision="2018-02-20",
                        subscription_id=sub_id,
                        subscription_type="on-change",
                        event_time=event_time,
                        ingestion_time=ingestion_time,
                        processing_time=now_iso,
                        path=target,
                        metric_name="oper_status",
                        value=num_val,
                        unit="state",
                        source=target,
                        sequence=sequence,
                        validation_status=ValidationStatus.VALID,
                    ))
                elif "cpu-utilization" in target or "cpu-utilization" in val_dict:
                    cpu_val = float(val_dict.get("cpu-utilization", 0))
                    records.append(TelemetryRecord(
                        device_id=device_id,
                        device_type=dev_type,
                        vendor=vendor,
                        model=model,
                        schema_module="custom-telemetry",
                        schema_revision="2024-01-01",
                        subscription_id=sub_id,
                        subscription_type="on-change",
                        event_time=event_time,
                        ingestion_time=ingestion_time,
                        processing_time=now_iso,
                        path=target,
                        metric_name="cpu_utilization",
                        value=cpu_val,
                        unit="percent",
                        source=target,
                        sequence=sequence,
                        validation_status=ValidationStatus.VALID,
                    ))
            return records

        # Handle Periodic Snapshot Datastore Contents
        datastore = envelope_meta.get("datastore_data", {})

        # 1. Parse ietf-interfaces:interfaces
        for key, val in datastore.items():
            if "interfaces" in key and isinstance(val, dict):
                interfaces = val.get("interface", [])
                for iface in interfaces:
                    if not isinstance(iface, dict):
                        continue
                    if_name = iface.get("name", "unknown")
                    stats = iface.get("statistics", {})

                    metrics_map = [
                        ("in-octets", "in_octets", stats.get("in-octets"), "bytes"),
                        ("out-octets", "out_octets", stats.get("out-octets"), "bytes"),
                        ("in-errors", "in_errors", stats.get("in-errors"), "count"),
                        ("out-errors", "out_errors", stats.get("out-errors"), "count"),
                        ("in-discards", "in_discards", stats.get("in-discards"), "count"),
                    ]

                    for y_field, norm_name, v, u in metrics_map:
                        if v is not None:
                            try:
                                num_v = float(v)
                                records.append(TelemetryRecord(
                                    device_id=device_id,
                                    device_type=dev_type,
                                    vendor=vendor,
                                    model=model,
                                    schema_module="ietf-interfaces",
                                    schema_revision="2018-02-20",
                                    subscription_id=sub_id,
                                    subscription_type=sub_type,
                                    event_time=event_time,
                                    ingestion_time=ingestion_time,
                                    processing_time=now_iso,
                                    path=f"/interfaces/interface[name={if_name}]/statistics/{y_field}",
                                    metric_name=norm_name,
                                    value=num_v,
                                    unit=u,
                                    source=f"{key}/interface[name={if_name}]/statistics/{y_field}",
                                    sequence=sequence,
                                    validation_status=ValidationStatus.VALID,
                                ))
                            except (ValueError, TypeError):
                                pass

            # 2. Parse custom-telemetry:system
            if "system" in key or "custom-telemetry" in key:
                sys_node = val.get("system", val) if isinstance(val, dict) else {}
                sys_metrics = [
                    ("cpu-utilization", "cpu_utilization", sys_node.get("cpu-utilization"), "percent"),
                    ("memory-utilization", "memory_utilization", sys_node.get("memory-utilization"), "percent"),
                    ("throughput-bps", "throughput_bps", sys_node.get("throughput-bps"), "bps"),
                    ("rtt-latency-ms", "rtt_latency_ms", sys_node.get("rtt-latency-ms"), "milliseconds"),
                ]

                for y_field, norm_name, v, u in sys_metrics:
                    if v is not None:
                        try:
                            num_v = float(v)
                            records.append(TelemetryRecord(
                                device_id=device_id,
                                device_type=dev_type,
                                vendor=vendor,
                                model=model,
                                schema_module="custom-telemetry",
                                schema_revision="2024-01-01",
                                subscription_id=sub_id,
                                subscription_type=sub_type,
                                event_time=event_time,
                                ingestion_time=ingestion_time,
                                processing_time=now_iso,
                                path=f"/system/{y_field}",
                                metric_name=norm_name,
                                value=num_v,
                                unit=u,
                                source=f"{key}/{y_field}",
                                sequence=sequence,
                                validation_status=ValidationStatus.VALID,
                            ))
                        except (ValueError, TypeError):
                            pass

        return records

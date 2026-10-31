"""
test_pipeline.py — Comprehensive Unit & Integration Test Suite for PS-034.
"""
import asyncio
import json
import os
import pytest
from datetime import datetime, timezone

from src.validation.schema_registry import SchemaRegistry
from src.validation.validation_engine import ValidationEngine
from src.validation.validation_result import ValidationStage, ValidationStatus
from src.pipeline.normalizer import Normalizer
from src.pipeline.deduplicator import Deduplicator, GapDetector
from src.pipeline.stream_queue import StreamQueue
from src.analytics.ewma_detector import EWMADetector
from src.analytics.robust_zscore import RobustZScoreDetector
from src.analytics.cusum_detector import CUSUMDetector
from src.analytics.isolation_forest import MultiVariateAnomalyDetector
from src.evaluation.reliability_evaluator import ReliabilityEvaluator
from src.simulator.simulator import TelemetrySimulator
from src.simulator.device_profiles import DEFAULT_DEVICE_FLEET
from src.storage.database import DatabaseManager

SCHEMAS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "yang_schemas")


@pytest.fixture
def engine():
    reg = SchemaRegistry(SCHEMAS_DIR)
    return ValidationEngine(reg)


def test_schema_loading(engine):
    assert "ietf-interfaces" in engine.registry.modules
    assert "custom-telemetry" in engine.registry.modules


def test_valid_yang_push_message(engine):
    """
    Full valid RFC 8641 message must pass all 6 stages.
    counter64/uint64 fields ('in-octets', 'out-octets', 'throughput-bps') are
    encoded as JSON strings per RFC 7951. decimal64 ('rtt-latency-ms') likewise.
    """
    valid_msg = {
        "ietf-notification:notification": {
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "ietf-yang-push:push-update": {
                "id": 101,
                "datastore-contents": {
                    "ietf-interfaces:interfaces": {
                        "interface": [{
                            "name": "GigabitEthernet0/0/0",
                            "oper-status": "up",
                            "statistics": {
                                "in-octets": "1234567",      # counter64 → string (RFC 7951)
                                "out-octets": "987654",      # counter64 → string (RFC 7951)
                                "in-errors": 0,
                                "in-discards": 0
                            }
                        }]
                    },
                    "custom-telemetry:system": {
                        "cpu-utilization": 45,
                        "memory-utilization": 58,
                        "throughput-bps": "800000000",   # uint64 → string (RFC 7951)
                        "rtt-latency-ms": "12.5"         # decimal64 → string (RFC 7951)
                    }
                }
            }
        }
    }
    is_valid, meta, report = engine.validate(valid_msg)
    assert is_valid is True, (
        f"Valid message rejected at {report.failed_at_stage}: {[e.message for e in report.errors]}"
    )
    assert report.status == ValidationStatus.VALID
    assert len(report.stages_passed) == 6


def test_syntax_failure(engine):
    corrupted_json = b'{"ietf-notification:notification": {"eventTime": "2024-01-01'
    is_valid, meta, report = engine.validate(corrupted_json)
    assert is_valid is False
    assert report.failed_at_stage == ValidationStage.SYNTAX
    assert report.errors[0].error_code == "JSON_SYNTAX_ERROR"


def test_envelope_failure(engine):
    missing_wrapper = {"some_other_key": {"data": 123}}
    is_valid, meta, report = engine.validate(missing_wrapper)
    assert is_valid is False
    assert report.failed_at_stage == ValidationStage.ENVELOPE
    assert report.errors[0].error_code == "MISSING_NOTIFICATION_WRAPPER"


def test_enum_violation(engine):
    """
    Invalid oper-status enum must be detected at CONSTRAINTS stage.
    Includes both modules so yangson formal validation runs and catches the violation.
    """
    bad_enum_msg = {
        "ietf-notification:notification": {
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "ietf-yang-push:push-update": {
                "id": 101,
                "datastore-contents": {
                    "ietf-interfaces:interfaces": {
                        "interface": [{
                            "name": "eth0",
                            "oper-status": "NON_EXISTENT_STATUS",  # Illegal enum
                        }]
                    },
                    "custom-telemetry:system": {
                        "cpu-utilization": 45,
                        "memory-utilization": 58,
                    }
                }
            }
        }
    }
    is_valid, meta, report = engine.validate(bad_enum_msg)
    assert is_valid is False
    # yangson raises YangTypeError for enum violations → CONSTRAINTS stage
    assert report.failed_at_stage == ValidationStage.CONSTRAINTS
    assert any(e.error_code == "ENUM_VIOLATION" for e in report.errors)


def test_range_violation(engine):
    out_of_bounds = {
        "ietf-notification:notification": {
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "ietf-yang-push:push-update": {
                "id": 101,
                "datastore-contents": {
                    "custom-telemetry:system": {
                        "cpu-utilization": 255,  # range is 0..100
                        "memory-utilization": 50
                    }
                }
            }
        }
    }
    is_valid, meta, report = engine.validate(out_of_bounds)
    assert is_valid is False
    assert report.failed_at_stage == ValidationStage.CONSTRAINTS
    assert any(e.error_code == "RANGE_VIOLATION_OVERFLOW" for e in report.errors)


def test_unsigned_negative_violation(engine):
    neg_counter = {
        "ietf-notification:notification": {
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "ietf-yang-push:push-update": {
                "id": 101,
                "datastore-contents": {
                    "ietf-interfaces:interfaces": {
                        "interface": [{
                            "name": "eth0",
                            "oper-status": "up",
                            "statistics": {
                                "in-octets": -100  # unsigned cannot be negative
                            }
                        }]
                    }
                }
            }
        }
    }
    is_valid, meta, report = engine.validate(neg_counter)
    assert is_valid is False
    assert any(e.error_code == "UNSIGNED_CONSTRAINT_VIOLATION" for e in report.errors)


def test_deduplicator():
    dedup = Deduplicator()
    fp = "sample_fingerprint_hash_1"
    assert dedup.check_and_record(fp) is False  # 1st time: unique
    assert dedup.check_and_record(fp) is True   # 2nd time: duplicate


def test_ewma_spike_detection():
    detector = EWMADetector(alpha=0.2, beta=0.1, k=3.0)
    # Warm up with baseline 40
    for _ in range(30):
        detector.update(40.0)
    # Extreme spike
    res = detector.update(95.0)
    assert res["is_anomaly"] is True
    assert res["anomaly_score"] > 0.5


def test_robust_zscore_outlier():
    rz = RobustZScoreDetector(window_size=30, threshold=3.5)
    for _ in range(25):
        rz.update(50.0)
    res = rz.update(120.0)
    assert res["is_anomaly"] is True
    assert res["robust_z_score"] > 3.5


def test_full_reliability_evaluation():
    import asyncio
    evaluator = ReliabilityEvaluator(SCHEMAS_DIR)
    results = asyncio.run(evaluator.run_all_evaluations())
    assert results["overall_passed"] is True
    for exp_name, exp in results["experiments"].items():
        assert exp["passed"] is True, f"Failed experiment: {exp_name}"



# =============================================================================
# Regression Tests — added to harden fixes from the session audit
# =============================================================================

# --- Yangson formal validation ---

def test_yangson_datamodel_initialized(engine):
    """SchemaRegistry must successfully load yangson.DataModel (formal_yang_validation=True)."""
    assert engine.registry.dm is not None, (
        "yangson.DataModel is None — yang-library.json or .yang files may be misconfigured"
    )
    assert engine.registry.formal_yang_validation is True


def test_yangson_accepts_simulator_message(engine):
    """A message produced by TelemetrySimulator must pass formal yangson validation."""
    sim = TelemetrySimulator()
    dev = DEFAULT_DEVICE_FLEET[0]
    metrics = sim._calculate_metrics(dev, None)
    msg = sim._build_rfc8641_message(dev, metrics)

    is_valid, meta, report = engine.validate(msg)
    assert is_valid is True, (
        f"Simulator message failed validation. "
        f"Stage: {report.failed_at_stage}, Errors: {[e.message for e in report.errors]}"
    )
    assert len(report.stages_passed) == 6


def test_simulator_counter64_string_encoding():
    """
    Simulator must encode counter64/uint64/decimal64 fields as JSON strings per RFC 7951.
    Specifically: in-octets, out-octets (counter64), throughput-bps (uint64),
    and rtt-latency-ms (decimal64) must be strings.
    """
    sim = TelemetrySimulator()
    dev = DEFAULT_DEVICE_FLEET[0]
    metrics = sim._calculate_metrics(dev, None)
    msg = sim._build_rfc8641_message(dev, metrics)

    iface = (
        msg["ietf-notification:notification"]
        ["ietf-yang-push:push-update"]
        ["datastore-contents"]
        ["ietf-interfaces:interfaces"]
        ["interface"][0]
    )
    stats = iface["statistics"]
    assert isinstance(stats["in-octets"], str), (
        f"in-octets must be a JSON string for RFC 7951 counter64, got {type(stats['in-octets'])}"
    )
    assert isinstance(stats["out-octets"], str), (
        f"out-octets must be a JSON string for RFC 7951 counter64, got {type(stats['out-octets'])}"
    )
    system = (
        msg["ietf-notification:notification"]
        ["ietf-yang-push:push-update"]
        ["datastore-contents"]
        ["custom-telemetry:system"]
    )
    assert isinstance(system["throughput-bps"], str), (
        f"throughput-bps must be a JSON string for RFC 7951 uint64, got {type(system['throughput-bps'])}"
    )
    assert isinstance(system["rtt-latency-ms"], str), (
        f"rtt-latency-ms must be a JSON string for RFC 7951 decimal64, got {type(system['rtt-latency-ms'])}"
    )


# --- YANG Patch on-change ---

def test_yang_patch_on_change_valid(engine):
    """A valid push-change-update with YANG Patch must pass validation."""
    patch_msg = {
        "ietf-notification:notification": {
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "ietf-yang-push:push-change-update": {
                "id": 201,
                "datastore-changes": {
                    "ietf-yang-patch:yang-patch": {
                        "patch-id": "patch-001",
                        "edit": [
                            {
                                "edit-id": "e1",
                                "operation": "replace",
                                "target": "/ietf-interfaces:interfaces/interface[name='GigabitEthernet0/0/0']/oper-status",
                                "value": {"oper-status": "down"},
                            }
                        ],
                    }
                },
            },
        }
    }
    is_valid, meta, report = engine.validate(patch_msg)
    assert is_valid is True, (
        f"Valid YANG Patch rejected: {[e.message for e in report.errors]}"
    )
    assert meta.get("is_patch") is True


def test_yang_patch_unsupported_target_rejected(engine):
    """A YANG Patch targeting an unsupported path must be rejected with UNSUPPORTED_PATCH_TARGET."""
    patch_msg = {
        "ietf-notification:notification": {
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "ietf-yang-push:push-change-update": {
                "id": 202,
                "datastore-changes": {
                    "ietf-yang-patch:yang-patch": {
                        "patch-id": "patch-002",
                        "edit": [
                            {
                                "edit-id": "e1",
                                "operation": "replace",
                                "target": "/ietf-routing:routing/ribs",  # Not a supported target
                                "value": {},
                            }
                        ],
                    }
                },
            },
        }
    }
    is_valid, meta, report = engine.validate(patch_msg)
    assert is_valid is False
    assert any(e.error_code == "UNSUPPORTED_PATCH_TARGET" for e in report.errors)


def test_yang_patch_invalid_enum_rejected(engine):
    """A YANG Patch with an invalid oper-status enum must be rejected."""
    patch_msg = {
        "ietf-notification:notification": {
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "ietf-yang-push:push-change-update": {
                "id": 203,
                "datastore-changes": {
                    "ietf-yang-patch:yang-patch": {
                        "patch-id": "patch-003",
                        "edit": [
                            {
                                "edit-id": "e1",
                                "operation": "replace",
                                "target": "/ietf-interfaces:interfaces/interface[name='eth0']/oper-status",
                                "value": {"oper-status": "FLYING"},  # Invalid enum
                            }
                        ],
                    }
                },
            },
        }
    }
    is_valid, meta, report = engine.validate(patch_msg)
    assert is_valid is False
    assert any(e.error_code == "ENUM_VIOLATION" for e in report.errors)


# --- Malformed string handling ---

def test_malformed_json_string_to_dlq(engine):
    """A raw malformed JSON string must fail at SYNTAX stage (not crash)."""
    bad_str = '{"ietf-notification:notification": {"eventTime": "2024-01-01'
    is_valid, meta, report = engine.validate(bad_str)
    assert is_valid is False
    assert report.failed_at_stage == ValidationStage.SYNTAX
    assert report.errors[0].error_code == "JSON_SYNTAX_ERROR"


# --- Deduplicator edge cases ---

def test_deduplicator_empty_datastore_fingerprint():
    """Deduplicator must handle empty datastore without crashing."""
    dedup = Deduplicator()
    fp = dedup.compute_fingerprint("dev-1", "2024-01-01T00:00:00+00:00", 101, {})
    assert fp is not None and isinstance(fp, str)
    # Should mark first as not-duplicate, second as duplicate
    assert dedup.check_and_record(fp) is False
    assert dedup.check_and_record(fp) is True


def test_deduplicator_ttl_window():
    """Two different fingerprints must both be marked as unique."""
    dedup = Deduplicator(window_size=100, ttl_seconds=60)
    fp1 = dedup.compute_fingerprint("dev-1", "2024-01-01T00:00:00", 101, {"a": 1})
    fp2 = dedup.compute_fingerprint("dev-1", "2024-01-01T00:00:01", 101, {"a": 2})
    assert dedup.check_and_record(fp1) is False
    assert dedup.check_and_record(fp2) is False  # different content, unique


# --- Raw payload JSON round-trip ---

def test_raw_payload_json_roundtrip():
    """The full RFC 8641 envelope from simulator must survive JSON serialization."""
    sim = TelemetrySimulator()
    dev = DEFAULT_DEVICE_FLEET[0]
    metrics = sim._calculate_metrics(dev, None)
    msg = sim._build_rfc8641_message(dev, metrics)

    serialized = json.dumps(msg)
    restored = json.loads(serialized)

    assert "ietf-notification:notification" in restored
    notif = restored["ietf-notification:notification"]
    assert "ietf-yang-push:push-update" in notif
    assert "eventTime" in notif


# --- StreamQueue in-memory fallback ---

def test_stream_queue_inmemory_produce_consume():
    """In-memory StreamQueue must deliver produced messages to consumer."""
    async def _run():
        sq = StreamQueue(stream_name="test:inmem", redis_url=None)
        # Force in-memory (don't call initialize() which tries Redis)
        sq.use_redis = False
        await sq.produce({"key": "value-1"})
        await sq.produce({"key": "value-2"})
        batch = await sq.consume_group("worker-test", count=5)
        for mid, _ in batch:
            await sq.ack(mid)
        return batch

    batch = asyncio.run(_run())
    assert len(batch) == 2
    assert batch[0][1]["key"] == "value-1"


# --- Reliability evaluator experiment 3 honesty check ---

def test_reliability_experiment3_not_claiming_network_delay():
    """Experiment 3 must declare it measures local CPU time, not network delay."""
    evaluator = ReliabilityEvaluator(SCHEMAS_DIR)
    result = asyncio.run(evaluator.eval_delivery_delay())
    name = result.get("name", "")
    note = result.get("note", "")
    # Must not use misleading 'delivery delay' framing without qualification
    assert "Local" in name or "local" in note or "CPU" in name or "CPU" in note or "Processing" in name, (
        f"Experiment 3 name/note does not clarify it measures CPU time: name='{name}', note='{note}'"
    )


# --- Reliability evaluator experiment 4 backend transparency ---

def test_reliability_experiment4_reports_backend():
    """Experiment 4 must report which backend was used (redis_streams vs in_memory_fallback)."""
    evaluator = ReliabilityEvaluator(SCHEMAS_DIR)
    result = asyncio.run(evaluator.eval_consumer_restart())
    assert "backend_used" in result, "Experiment 4 must report the queue backend used"
    assert result["backend_used"] in ("redis_streams", "in_memory_fallback"), (
        f"Unknown backend_used: {result['backend_used']}"
    )


# --- Phase 2-12 Audit & Improvement New Tests ---

def test_non_dict_notification_wrapper_dlq(engine):
    """Corrupted notification wrapper must fail safely without raising AttributeError."""
    corrupted_wrapper = {"ietf-notification:notification": "corrupted-string-not-dict"}
    is_valid, meta, report = engine.validate(corrupted_wrapper)
    assert is_valid is False
    assert report.failed_at_stage == ValidationStage.ENVELOPE
    assert any(e.error_code == "INVALID_NOTIFICATION_WRAPPER" for e in report.errors)


def test_invalid_iso8601_timestamp_envelope(engine):
    """Malformed eventTime must be rejected at ENVELOPE stage."""
    bad_time_msg = {
        "ietf-notification:notification": {
            "eventTime": "not-a-valid-iso-date",
            "ietf-yang-push:push-update": {
                "id": 101,
                "datastore-contents": {
                    "custom-telemetry:system": {
                        "cpu-utilization": 20,
                        "memory-utilization": 30,
                    }
                },
            },
        }
    }
    is_valid, meta, report = engine.validate(bad_time_msg)
    assert is_valid is False
    assert report.failed_at_stage == ValidationStage.ENVELOPE
    assert any(e.error_code == "INVALID_ISO8601_TIMESTAMP" for e in report.errors)


def test_single_module_interfaces_formal_validation(engine):
    """An update containing only ietf-interfaces must formally pass validation."""
    single_if_msg = {
        "ietf-notification:notification": {
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "ietf-yang-push:push-update": {
                "id": 102,
                "datastore-contents": {
                    "ietf-interfaces:interfaces": {
                        "interface": [{
                            "name": "GigabitEthernet0/0/0",
                            "oper-status": "up",
                            "statistics": {
                                "in-octets": "1000",
                                "out-octets": "2000",
                            },
                        }]
                    }
                },
            },
        }
    }
    is_valid, meta, report = engine.validate(single_if_msg)
    assert is_valid is True, f"Validation failed with: {[e.message for e in report.errors]}"
    assert ValidationStage.STRUCTURE in report.stages_passed
    assert ValidationStage.CONSTRAINTS in report.stages_passed


def test_single_module_custom_telemetry_formal_validation(engine):
    """An update containing only custom-telemetry must formally pass validation."""
    single_sys_msg = {
        "ietf-notification:notification": {
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "ietf-yang-push:push-update": {
                "id": 103,
                "datastore-contents": {
                    "custom-telemetry:system": {
                        "cpu-utilization": 45,
                        "memory-utilization": 60,
                        "throughput-bps": "500000000",
                        "rtt-latency-ms": "14.25",
                    }
                },
            },
        }
    }
    is_valid, meta, report = engine.validate(single_sys_msg)
    assert is_valid is True, f"Validation failed with: {[e.message for e in report.errors]}"
    assert ValidationStage.STRUCTURE in report.stages_passed
    assert ValidationStage.CONSTRAINTS in report.stages_passed


def test_rfc8525_yang_library_export(engine):
    """SchemaRegistry must export RFC 8525 compliant YANG Library metadata."""
    yl = engine.registry.get_rfc8525_library()
    assert "content-id" in yl
    assert "module-set" in yl
    assert isinstance(yl["module-set"], list)
    mod_names = [m["name"] for m in yl["module-set"][0]["module"]]
    assert "ietf-interfaces" in mod_names
    assert "custom-telemetry" in mod_names


def test_stream_queue_pending_inspection_and_recovery():
    """StreamQueue must allow inspecting pending messages and re-delivering them."""
    async def _test():
        sq = StreamQueue(stream_name="test:pending", redis_url=None)
        sq.use_redis = False
        await sq.produce({"task": 1})
        await sq.produce({"task": 2})

        # Consume 2 messages without acking (simulates crash before ack)
        batch = await sq.consume_group("worker-crashed", count=2)
        assert len(batch) == 2

        # Inspect pending list
        pending = await sq.inspect_pending()
        assert pending["pending_count"] == 2

        # Recover pending messages
        recovered = await sq.recover_pending_messages("worker-crashed", count=2)
        assert len(recovered) == 2
        assert recovered[0][1]["task"] == 1

        # Ack one and verify pending drops
        await sq.ack(recovered[0][0])
        pending_after = await sq.inspect_pending()
        assert pending_after["pending_count"] == 1

    asyncio.run(_test())


def test_database_dlq_stores_structured_error_details():
    """DatabaseManager must persist and query DLQ records with structured error details."""
    async def _test():
        test_db_path = "test_audit_dlq.db"
        if os.path.exists(test_db_path):
            os.remove(test_db_path)
        db = DatabaseManager(test_db_path)
        await db.initialize()

        test_details = [
            {"stage": "STAGE_5_CONSTRAINTS", "path": "/interfaces/interface[0]/oper-status", "error_code": "ENUM_VIOLATION"}
        ]
        await db.save_dlq_record({
            "id": "dlq-audit-123",
            "device_id": "core-router-01",
            "failed_stage": "STAGE_5_CONSTRAINTS",
            "error_summary": "Invalid oper-status",
            "raw_payload_preview": '{"test": 1}',
            "created_at": datetime.now(timezone.utc).isoformat(),
            "error_details": test_details,
        })

        recent = await db.get_recent_dlq(limit=10)
        assert len(recent) == 1
        assert recent[0]["id"] == "dlq-audit-123"
        assert len(recent[0]["error_details"]) == 1
        assert recent[0]["error_details"][0]["error_code"] == "ENUM_VIOLATION"

        await db.close()
        if os.path.exists(test_db_path):
            os.remove(test_db_path)

    asyncio.run(_test())


def test_semantic_inconsistency_down_interface(engine):
    """Stage 6 semantic validation must flag oper-status down interface with excessive errors."""
    msg = {
        "ietf-notification:notification": {
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "ietf-yang-push:push-update": {
                "id": 104,
                "datastore-contents": {
                    "ietf-interfaces:interfaces": {
                        "interface": [{
                            "name": "eth0",
                            "oper-status": "down",
                            "statistics": {
                                "in-octets": "0",
                                "out-octets": "0",
                                "in-errors": 2000000,  # Excessive error churn on down interface
                            },
                        }]
                    }
                },
            },
        }
    }
    is_valid, meta, report = engine.validate(msg)
    assert is_valid is False
    assert report.failed_at_stage == ValidationStage.SEMANTICS
    assert any(e.error_code == "SEMANTIC_INCONSISTENCY" for e in report.errors)


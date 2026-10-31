"""
app.py — FastAPI Application, YANG-Push Receiver, Pipeline Coordinator & Live Dashboard.
"""
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
import os
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel

from ..analytics.composite_detector import CompositeAnomalyDetector
from ..analytics.isolation_forest import MultiVariateAnomalyDetector
from ..evaluation.reliability_evaluator import ReliabilityEvaluator
from ..pipeline.dead_letter_queue import DeadLetterQueue
from ..pipeline.deduplicator import Deduplicator, GapDetector
from ..pipeline.normalizer import Normalizer
from ..pipeline.stream_queue import StreamQueue
from ..simulator.device_profiles import AnomalyType, DEFAULT_DEVICE_FLEET
from ..simulator.simulator import TelemetrySimulator
from ..storage.database import DatabaseManager
from ..validation.schema_registry import SchemaRegistry
from ..validation.validation_engine import ValidationEngine
from ..validation.validation_result import ValidationStage, ValidationStatus

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCHEMAS_DIR = os.path.join(BASE_DIR, "yang_schemas")

# Core Pipeline Components
registry = SchemaRegistry(SCHEMAS_DIR)
validation_engine = ValidationEngine(registry)
dlq = DeadLetterQueue(max_size=500)
deduplicator = Deduplicator(window_size=2000, ttl_seconds=60)
gap_detector = GapDetector()
stream_queue = StreamQueue(redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379"))
db = DatabaseManager()
simulator = TelemetrySimulator()
ml_detector = MultiVariateAnomalyDetector()
evaluator = ReliabilityEvaluator(SCHEMAS_DIR)

# Anomaly Detectors: (device_id, metric_name) -> CompositeAnomalyDetector
composite_detectors: Dict[str, CompositeAnomalyDetector] = {}

# Active WebSockets
connected_websockets: List[WebSocket] = []

# Statistics
pipeline_stats = {
    "total_received": 0,
    "valid_count": 0,
    "invalid_count": 0,
    "dlq_count": 0,
    "anomalies_detected": 0,
    "sequence_counter": 0,
    "recent_audits": [],
    "recent_anomalies": [],
    "latest_metrics_by_device": {},
}


async def broadcast_ws(payload: Dict[str, Any]):
    disconnected = []
    for ws in connected_websockets:
        try:
            await ws.send_json(payload)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        if ws in connected_websockets:
            connected_websockets.remove(ws)


async def process_raw_message(device_id: str, raw_payload: Any) -> Dict[str, Any]:
    """
    Ingests raw payload through the 6-stage validation pipeline, normalizer, and queue.
    """
    pipeline_stats["total_received"] += 1
    pipeline_stats["sequence_counter"] += 1
    seq = pipeline_stats["sequence_counter"]
    now_iso = datetime.now(timezone.utc).isoformat()

    # Preserve raw notification envelope end-to-end (before validation mutates state)
    raw_json_str = json.dumps(raw_payload) if isinstance(raw_payload, dict) else str(raw_payload)

    # Stage 1 - 6 Validation
    is_valid, envelope_meta, report = validation_engine.validate(raw_payload)

    # Record validation audit
    audit_item = {
        "device_id": device_id,
        "is_valid": is_valid,
        "status": report.status.value,
        "failed_at_stage": report.failed_at_stage.value if report.failed_at_stage else None,
        "error_summary": report.errors[0].message if report.errors else "Schema Validated (Pass)",
        "duration_ms": report.duration_ms,
        "timestamp": now_iso,
    }
    pipeline_stats["recent_audits"].insert(0, audit_item)
    if len(pipeline_stats["recent_audits"]) > 60:
        pipeline_stats["recent_audits"].pop()

    await db.record_validation_audit(
        device_id, is_valid, report.status.value,
        report.failed_at_stage.value if report.failed_at_stage else None,
        audit_item["error_summary"], report.duration_ms, now_iso
    )

    # Save raw RFC 8641 envelope to persistent storage (valid and invalid alike)
    _notif_type = "push-update"
    _sub_id_raw: Optional[int] = None
    _evt_time_raw = now_iso
    if isinstance(raw_payload, dict):
        _notif_inner = raw_payload.get("ietf-notification:notification", {})
        if _notif_inner.get("ietf-yang-push:push-change-update"):
            _notif_type = "push-change-update"
        if envelope_meta:
            _sub_id_raw = envelope_meta.get("subscription_id")
            _evt_time_raw = envelope_meta.get("event_time", now_iso)
    await db.save_raw_notification(
        device_id=device_id,
        notification_type=_notif_type,
        subscription_id=_sub_id_raw,
        event_time=_evt_time_raw,
        ingestion_time=now_iso,
        raw_payload=raw_json_str,
        is_valid=is_valid,
    )

    if not is_valid or envelope_meta is None:
        pipeline_stats["invalid_count"] += 1
        pipeline_stats["dlq_count"] += 1
        failed_stage = report.failed_at_stage.value if report.failed_at_stage else "UNKNOWN"
        error_details = [e.to_dict() for e in report.errors]

        q_msg = dlq.quarantine(
            device_id=device_id,
            failed_stage=failed_stage,
            error_details=error_details,
            raw_payload=raw_payload,
            subscription_id=report.subscription_id,
        )
        await db.save_dlq_record({
            "id": q_msg.id,
            "device_id": q_msg.device_id,
            "failed_stage": q_msg.failed_stage,
            "error_summary": q_msg.error_summary,
            "raw_payload_preview": q_msg.raw_payload_preview,
            "created_at": q_msg.created_at,
            "error_details": error_details,
        })
        return {"status": "quarantined", "report": report.to_dict()}

    pipeline_stats["valid_count"] += 1

    # Check Deduplication
    event_time = envelope_meta["event_time"]
    sub_id = envelope_meta["subscription_id"]
    datastore = envelope_meta["datastore_data"]
    fp = deduplicator.compute_fingerprint(device_id, event_time, sub_id, datastore)
    if deduplicator.check_and_record(fp):
        # Flagged as duplicate
        audit_item["error_summary"] = "Warning: Duplicate notification ignored"
        return {"status": "duplicate_ignored", "report": report.to_dict()}

    # Check Sequence Gaps & Delay
    sim_meta = raw_payload.get("_simulator", {}) if isinstance(raw_payload, dict) else {}
    rep_tick = sim_meta.get("tick")
    gap_detector.inspect_delivery(device_id, rep_tick, event_time, now_iso)

    # Normalize to canonical TelemetryRecord
    dev_obj = simulator.devices.get(device_id)
    dev_meta = {
        "vendor": dev_obj.vendor if dev_obj else "cisco",
        "device_type": dev_obj.device_type if dev_obj else "router",
        "model": dev_obj.model if dev_obj else "ASR-9001",
    }
    records = Normalizer.normalize(envelope_meta, device_id, dev_meta, seq, now_iso)

    # Push to Stream Queue
    for rec in records:
        await stream_queue.produce(rec.model_dump())

    # Save to storage batch
    await db.save_telemetry_batch(records)

    return {"status": "ingested", "records_count": len(records), "report": report.to_dict()}


async def background_simulation_loop():
    """Generates continuous RFC 8641 telemetry from the device fleet."""
    while True:
        try:
            tick_messages = simulator.generate_next_tick()
            for dev_id, msg in tick_messages:
                await process_raw_message(dev_id, msg)
        except Exception as e:
            print(f"[SimLoop Error] {e}")
        await asyncio.sleep(1.5)


async def background_stream_consumer_worker():
    """
    Consumer worker that reads normalized records from the Stream Queue,
    updates statistical baselines (EWMA, MAD, CUSUM), runs Isolation Forest,
    and alerts on anomalies.
    """
    while True:
        try:
            entries = await stream_queue.consume_group("analytics-worker-1", count=15, block_ms=1000)
            for msg_id, rec in entries:
                dev_id = rec.get("device_id")
                metric = rec.get("metric_name")
                val = rec.get("value", 0.0)
                ts = rec.get("event_time", "")

                # Store latest vital metric
                if dev_id not in pipeline_stats["latest_metrics_by_device"]:
                    pipeline_stats["latest_metrics_by_device"][dev_id] = {}
                pipeline_stats["latest_metrics_by_device"][dev_id][metric] = val

                # Anomaly evaluation
                det_key = f"{dev_id}:{metric}"
                if det_key not in composite_detectors:
                    composite_detectors[det_key] = CompositeAnomalyDetector(dev_id, metric)

                detector = composite_detectors[det_key]
                current_metrics = pipeline_stats["latest_metrics_by_device"][dev_id]
                res = detector.process(val, ts, ml_detector=ml_detector, current_metrics=current_metrics)

                if res["is_anomaly"]:
                    pipeline_stats["anomalies_detected"] += 1
                    pipeline_stats["recent_anomalies"].insert(0, res)
                    if len(pipeline_stats["recent_anomalies"]) > 50:
                        pipeline_stats["recent_anomalies"].pop()
                    await db.record_anomaly(res)

                await stream_queue.ack(msg_id)

            # Broadcast live state to WebSockets
            if connected_websockets:
                await broadcast_ws({
                    "type": "METRICS_UPDATE",
                    "stats": {
                        "total_received": pipeline_stats["total_received"],
                        "valid_count": pipeline_stats["valid_count"],
                        "invalid_count": pipeline_stats["invalid_count"],
                        "dlq_count": pipeline_stats["dlq_count"],
                        "anomalies_detected": pipeline_stats["anomalies_detected"],
                    },
                    "devices": {
                        d_id: {
                            "vendor": simulator.devices[d_id].vendor,
                            "type": simulator.devices[d_id].device_type,
                            "model": simulator.devices[d_id].model,
                            "metrics": pipeline_stats["latest_metrics_by_device"].get(d_id, {}),
                            "active_fault": getattr(simulator.active_faults.get(d_id, {}).get("type", "NONE"), "value", "NONE")
                        }
                        for d_id in simulator.devices
                    },
                    "recent_audit": pipeline_stats["recent_audits"][:8],
                    "recent_anomalies": pipeline_stats["recent_anomalies"][:5],
                })

        except Exception as e:
            print(f"[Consumer Error] {e}")
            await asyncio.sleep(0.5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.initialize()
    await stream_queue.initialize()
    ml_detector.bootstrap_default_baseline()

    # Launch background tasks
    sim_task = asyncio.create_task(background_simulation_loop())
    consumer_task = asyncio.create_task(background_stream_consumer_worker())

    yield

    sim_task.cancel()
    consumer_task.cancel()
    await stream_queue.close()
    await db.close()


app = FastAPI(title="PS-034 YANG-Push Telemetry Pipeline", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -------------------------------------------------------------
# REST Endpoints
# -------------------------------------------------------------

@app.post("/api/v1/telemetry/push")
async def receive_yang_push(request: Request):
    """
    Primary ingestion endpoint for YANG-Push notifications (RFC 8641).
    Accepts JSON-encoded push-update or push-change-update messages.
    Malformed JSON is forwarded as a raw string so the validator can
    classify it as a SYNTAX error and route it to the DLQ.
    Enforces a 1 MB payload size limit to prevent Denial of Service.
    """
    MAX_PAYLOAD_BYTES = 1_048_576  # 1 MB protection against DoS
    try:
        raw_bytes = await request.body()
        if len(raw_bytes) > MAX_PAYLOAD_BYTES:
            raise HTTPException(
                status_code=413,
                detail=f"Payload size ({len(raw_bytes)} bytes) exceeds 1MB limit ({MAX_PAYLOAD_BYTES} bytes)"
            )
        dev_id = "external-device"
        try:
            data = json.loads(raw_bytes.decode("utf-8"))
            # Extract device-id from header or _simulator metadata
            if isinstance(data, dict):
                dev_id = request.headers.get(
                    "X-Device-Id",
                    data.get("_simulator", {}).get("device_id", "external-device")
                )
            result = await process_raw_message(dev_id, data)
        except json.JSONDecodeError:
            # Let the validation engine classify and quarantine it
            raw_str = raw_bytes.decode("utf-8", errors="replace")
            dev_id = request.headers.get("X-Device-Id", "external-device")
            result = await process_raw_message(dev_id, raw_str)
        return JSONResponse(content=result)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/v1/devices")
async def get_devices():
    return {
        d_id: {
            "device_id": d.device_id,
            "vendor": d.vendor,
            "type": d.device_type,
            "model": d.model,
            "interfaces": d.interfaces,
            "is_online": d.is_online,
            "active_fault": getattr(simulator.active_faults.get(d_id, {}).get("type", "NONE"), "value", "NONE"),
            "current_metrics": pipeline_stats["latest_metrics_by_device"].get(d_id, {}),
        }
        for d_id, d in simulator.devices.items()
    }


@app.get("/api/v1/validation/audit")
async def get_validation_audits():
    return {
        "recent_audits": pipeline_stats["recent_audits"][:50],
        "total_audited": pipeline_stats["total_received"],
    }


@app.get("/api/v1/validation/stats")
async def get_validation_stats():
    total = max(1, pipeline_stats["total_received"])
    valid = pipeline_stats["valid_count"]
    invalid = pipeline_stats["invalid_count"]
    pass_rate = round((valid / total) * 100.0, 2)
    return {
        "total_received": pipeline_stats["total_received"],
        "valid": valid,
        "invalid": invalid,
        "pass_rate_pct": pass_rate,
        "dlq_stats": dlq.get_stats(),
    }


@app.get("/api/v1/dlq")
async def get_dlq():
    inmem = dlq.get_recent(limit=50)
    db_records = await db.get_recent_dlq(limit=50)
    return {
        "quarantined_messages": inmem if inmem else db_records,
        "persisted_dlq_count": len(db_records),
        "stats": dlq.get_stats(),
    }


@app.get("/api/v1/schemas/rfc8525")
async def get_rfc8525_schema():
    """Returns RFC 8525 compliant YANG Library metadata dictionary."""
    return registry.get_rfc8525_library()


@app.get("/api/v1/anomalies")
async def get_anomalies():
    return {
        "anomalies": pipeline_stats["recent_anomalies"][:50],
        "total_detected": pipeline_stats["anomalies_detected"],
    }


class FaultInjectionRequest(BaseModel):
    device_id: str
    anomaly_type: str
    duration_ticks: int = 15
    intensity: float = 1.0


@app.post("/api/v1/simulator/inject")
async def inject_fault(req: FaultInjectionRequest):
    try:
        atype = AnomalyType(req.anomaly_type)
        simulator.inject_fault(req.device_id, atype, req.duration_ticks, req.intensity)
        return {"status": "ok", "message": f"Injected {req.anomaly_type} into {req.device_id}"}
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid anomaly type: {req.anomaly_type}")


@app.post("/api/v1/simulator/clear")
async def clear_faults():
    simulator.clear_faults()
    return {"status": "ok", "message": "All active simulator faults cleared."}


@app.post("/api/v1/experiments/run-reliability")
async def run_reliability_tests():
    """Runs the 5-experiment reliability suite and returns results."""
    eval_res = await evaluator.run_all_evaluations()
    return eval_res


@app.get("/api/v1/schemas")
async def get_schemas():
    return registry.list_loaded_schemas()


@app.get("/api/v1/pipeline/health")
async def get_pipeline_health():
    """
    Returns the current health status of the pipeline, including whether
    formal yangson validation is active and whether Redis Streams are in use.
    """
    stream_stats = await stream_queue.get_stream_stats()
    return {
        "status": "healthy",
        "formal_yang_validation": registry.formal_yang_validation,
        "durable_queue": stream_queue.use_redis,
        "queue_backend": stream_stats.get("backend", "unknown"),
        "queue_stats": stream_stats,
        "schemas_loaded": list(registry.modules.keys()),
        "yangson_datamodel_initialized": registry.dm is not None,
        "note": (
            "formal_yang_validation=true means yangson.DataModel is active for RFC 7950 structural checks. "
            "durable_queue=true means Redis Streams (persistent consumer groups); "
            "false means in-memory fallback (no durability across restarts)."
        ),
    }


@app.get("/api/v1/telemetry/raw")
async def get_raw_telemetry(device_id: Optional[str] = None, limit: int = 50):
    """
    Returns raw RFC 8641 notification envelopes as stored at ingestion time.
    Includes both valid and invalid (DLQ-bound) notifications.
    """
    records = await db.get_raw_notifications(device_id=device_id, limit=limit)
    return {"raw_notifications": records, "count": len(records)}


@app.get("/api/v1/telemetry/history")
async def get_telemetry_history(
    device_id: Optional[str] = None,
    metric_name: Optional[str] = None,
    limit: int = 100,
):
    """Returns normalized time-series telemetry records from SQLite storage."""
    records = await db.get_recent_telemetry(
        device_id=device_id, metric_name=metric_name, limit=limit
    )
    return {"telemetry": records, "count": len(records)}


@app.get("/api/v1/anomalies/history")
async def get_anomaly_history(limit: int = 50):
    """Returns persisted anomaly incidents from SQLite storage."""
    records = await db.get_recent_anomalies(limit=limit)
    return {"anomalies": records, "count": len(records)}


@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_websockets.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in connected_websockets:
            connected_websockets.remove(websocket)


# -------------------------------------------------------------
# Embedded Live Dashboard UI
# -------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def serve_dashboard():
    static_file = os.path.join(os.path.dirname(__file__), "..", "static", "dashboard.html")
    if os.path.exists(static_file):
        with open(static_file, "r", encoding="utf-8") as f:
            return HTMLResponse(content=f.read())
    return HTMLResponse("<h2>Dashboard file not found.</h2>")

<<<<<<< HEAD
# PS-034: Schema-Validated YANG-Push Telemetry Pipeline

## Live Link:
https://ps034-telemetry-pipeline.onrender.com/

An end-to-end, production-ready implementation of an IETF-compliant network telemetry pipeline conforming to:
* **RFC 8641**: Subscription to YANG-Push Datastore Updates
* **RFC 8639**: Subscription to YANG Notifications
* **RFC 7950**: The YANG 1.1 Data Modeling Language
* **RFC 7951**: JSON Encoding of Data Modeled with YANG
* **RFC 8525**: YANG Library (Data Dictionary & Schema State)
* **RFC 8342**: Network Management Datastore Architecture (NMDA)
* **RFC 8343**: A YANG Data Model for Interface Management
* **RFC 8072**: YANG Patch (On-Change Datastore Updates)

---

## 🌟 Architecture Highlights

```
Simulated Device Fleet (Cisco, Arista, Juniper, Palo Alto)
                   │
                   ▼ (RFC 8641 push-update / push-change-update)
         Telemetry Ingestion (1 MB max payload check)
                   │
                   ▼
    ┌──────────────────────────────────────────────┐
    │     6-Stage YANG Schema Validation Engine    │
    │  [1: Syntax] ── [2: Envelope] ── [3: Schema] │
    │  [4: Struct] ── [5: Bounds]   ── [6: NMDA]   │
    └──────────────┬───────────────────────────────┘
                   │
         ┌─────────┴─────────┐
         │ (Valid)           │ (Invalid / Non-compliant)
         ▼                   ▼
    Normalizer          Dead Letter Queue (DLQ)
         │                   │ (Structured diagnostics & payload preview)
         ▼                   ▼
    Stream Queue        SQLite WAL / TimescaleDB
    (Redis Streams /    
     In-Memory Fallback)
         │
         ▼
  Analytics Layer
    ├── EWMA (Streaming adaptive bounds)
    ├── Robust Z-Score (Median Absolute Deviation)
    ├── CUSUM (Persistent shift detection)
    └── Isolation Forest (Multi-variate ML correlation)
         │
         ▼
  Real-Time Dashboard & WebSocket Feed
```

---

## 📁 Project Structure

```
yang-push-pipeline/
├── run.py                          # Single-command launcher (FastAPI + Simulator + Workers)
├── requirements.txt                # Python dependencies (yangson, pyang, fastapi, redis, etc.)
├── docker-compose.yml              # Multi-container orchestration (Redis + Pipeline)
├── Dockerfile                      # Production container image definition
├── evaluation_results.json         # Measured quantitative reliability metrics
├── yang_schemas/                   # Formal YANG modules & metadata
│   ├── ietf-interfaces@2018-02-20.yang   # Scoped RFC 8343 interface model
│   ├── ietf-yang-types@2013-07-15.yang   # RFC 6991 common data types
│   ├── custom-telemetry@2024-01-01.yang  # Custom system telemetry model
│   └── yang-library.json                 # Dual RFC 8525 & RFC 7895 metadata
├── src/
│   ├── simulator/                  # Fleet simulator & fault injector
│   │   ├── device_profiles.py      # Cisco, Arista, Juniper, Palo Alto baselines
│   │   └── simulator.py            # RFC 8641 message generator (with reproducible seed)
│   ├── validation/                 # 6-Stage formal schema validator
│   │   ├── schema_registry.py      # Dynamic YANG module parser & constraint compiler
│   │   ├── envelope_validator.py   # RFC 8641 / RFC 8639 envelope & ISO 8601 validator
│   │   ├── validation_engine.py    # 6-stage execution engine (yangson DataModel)
│   │   └── validation_result.py    # Structured audit & error reporting models
│   ├── pipeline/                   # Stream ingestion & queues
│   │   ├── normalizer.py           # Canonical TelemetryRecord mapping
│   │   ├── dead_letter_queue.py    # DLQ repository & error diagnostics
│   │   ├── stream_queue.py         # Redis Streams & In-Memory fallback queue
│   │   └── deduplicator.py         # Fingerprint cache & gap detector
│   ├── analytics/                  # Statistical & ML engines
│   │   ├── ewma_detector.py        # Adaptive EWMA bounds detector
│   │   ├── robust_zscore.py        # MAD-based robust Z-score outlier detector
│   │   ├── cusum_detector.py       # Two-sided CUSUM level-shift detector
│   │   ├── isolation_forest.py     # Multi-metric Isolation Forest
│   │   └── composite_detector.py   # Explainable ensemble scorer
│   ├── storage/                    # Storage layer (SQLite WAL / TimescaleDB)
│   │   └── database.py             # Telemetry, raw envelopes, and structured DLQ storage
│   ├── evaluation/                 # Reliability & experiment suite
│   │   └── reliability_evaluator.py# 5-Point quantitative reliability benchmark
│   ├── api/                        # FastAPI REST & WebSocket server
│   │   └── app.py                  # API routes, WS broadcast & background loops
│   └── static/                     # Real-time web dashboard
│       └── dashboard.html          # Embedded single-page dashboard UI
└── tests/
    └── test_pipeline.py            # 32 unit & integration test cases
```

---

## 🚀 Quick Start

### 1. Run the Pipeline & Dashboard (Local Standalone)
```bash
python run.py
```
Open **[http://localhost:8000](http://localhost:8000)** in your browser to view:
* Live fleet telemetry (Cisco, Arista, Juniper, Palo Alto)
* Real-time 6-Stage validation audit feed
* Dead Letter Queue (DLQ) quarantine inspector
* Statistical & ML anomaly alerts
* 1-Click Fault Injection panel
* Automated Reliability Evaluation Suite

### 2. Run with Docker Compose (with Redis Streams)
```bash
docker compose up --build
```

### 3. Run Test Suite
```bash
python -m pytest tests/test_pipeline.py -v
```

### 4. Trigger Fault Injection via API
```bash
# Inject Schema Violation (Illegal enum / type mismatch)
curl -X POST http://localhost:8000/api/v1/simulator/inject \
  -H "Content-Type: application/json" \
  -d '{"device_id": "core-router-01", "anomaly_type": "SCHEMA_VIOLATION"}'

# Inject CPU Spike
curl -X POST http://localhost:8000/api/v1/simulator/inject \
  -H "Content-Type: application/json" \
  -d '{"device_id": "dist-router-02", "anomaly_type": "CPU_SPIKE"}'
```

### 5. Inspect RFC 8525 YANG Library
```bash
curl http://localhost:8000/api/v1/schemas/rfc8525
```

### 6. Run Automated Reliability Evaluation
```bash
curl -X POST http://localhost:8000/api/v1/experiments/run-reliability
```
Evaluates:
1. **Message Loss Measurement**: monotonic sequence gap tracking.
2. **Duplicate Handling**: sliding-window fingerprint deduplication.
3. **Delivery Delay & Validation Latency**: p95 local validation processing latency.
4. **Consumer Restart Recovery**: worker crash recovery with zero data loss.
5. **YANG-Push Notification Structure Preservation**: schema model fidelity & roundtrip JSON.
=======
# AIORI-3-HACKATHON-PROJECTS
This repository will contain the code and resources for the AIORI-3 hackathon that will be developed during the AIORI-3

## YouTube Video link
https://youtu.be/3DwPlX0gcxg
>>>>>>> origin/main

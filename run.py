"""
run.py — Single-command runner for PS-034 Schema-Validated YANG-Push Telemetry Pipeline.
Starts the FastAPI application, background device fleet simulator, stream worker, and live dashboard.
"""
import sys
import os

# Add scratch project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import uvicorn

if __name__ == "__main__":
    print("=" * 70)
    print("🚀 Starting PS-034 Schema-Validated YANG-Push Telemetry Pipeline")
    print("=" * 70)
    print("• Standards Implemented: RFC 8641 (YANG-Push) & RFC 7950 (YANG 1.1)")
    print("• Multi-Stage Validation Engine: Stages 1-6 Active")
    print("• Analytics Layer: EWMA, MAD Robust Z-score, CUSUM, Isolation Forest")
    print("• Stream Queue: Redis Streams / Consumer Group Fallback Active")
    print("• Live Dashboard: http://localhost:8000")
    print("=" * 70)

    port = int(os.environ.get("PORT", "8000"))

    uvicorn.run(
        "src.api.app:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        log_level="info"
    )

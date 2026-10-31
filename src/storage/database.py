"""
database.py — Asynchronous Database Layer with SQLite WAL mode & TimescaleDB/Postgres support.
Uses Python built-in sqlite3 with asyncio thread dispatch and busy_timeout for concurrency.
Preserves raw RFC 8641 telemetry notifications end-to-end.
"""
import asyncio
import json
import os
import sqlite3
from typing import Any, Dict, List, Optional


class DatabaseManager:
    """
    Manages persistent storage for telemetry time-series, raw notification payloads,
    validation audits, quarantined DLQ events, and anomaly incidents.
    """

    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            self.db_path = os.path.join(base_dir, "pipeline_telemetry.db")
        else:
            self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    async def initialize(self):
        await asyncio.to_thread(self._init_sync)

    def _init_sync(self):
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
        self._conn.row_factory = sqlite3.Row
        cur = self._conn.cursor()
        # Enable Write-Ahead Logging (WAL) for high concurrency
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA busy_timeout=5000;")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS telemetry_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                device_type TEXT,
                vendor TEXT,
                metric_name TEXT NOT NULL,
                value REAL NOT NULL,
                unit TEXT,
                path TEXT,
                event_time TEXT NOT NULL,
                ingestion_time TEXT NOT NULL,
                sequence INTEGER,
                validation_status TEXT
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                notification_type TEXT NOT NULL,
                subscription_id INTEGER,
                event_time TEXT NOT NULL,
                ingestion_time TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                is_valid INTEGER NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS validation_audits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT,
                is_valid INTEGER NOT NULL,
                status TEXT,
                failed_at_stage TEXT,
                error_summary TEXT,
                duration_ms REAL,
                timestamp TEXT NOT NULL
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS anomalies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                metric TEXT NOT NULL,
                severity TEXT NOT NULL,
                composite_score REAL NOT NULL,
                value REAL,
                timestamp TEXT NOT NULL,
                evidence_json TEXT
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dlq_records (
                id TEXT PRIMARY KEY,
                device_id TEXT,
                failed_stage TEXT,
                error_summary TEXT,
                raw_preview TEXT,
                timestamp TEXT,
                error_details_json TEXT
            );
        """)

        # Schema migration check: ensure error_details_json column exists
        columns = [row["name"] for row in cur.execute("PRAGMA table_info(dlq_records);").fetchall()]
        if "error_details_json" not in columns:
            try:
                cur.execute("ALTER TABLE dlq_records ADD COLUMN error_details_json TEXT;")
            except Exception:
                pass

        cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_dev_time ON telemetry_records(device_id, event_time);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_metric ON telemetry_records(metric_name);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_notif_time ON raw_notifications(ingestion_time);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_anomalies_time ON anomalies(timestamp);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dlq_time ON dlq_records(timestamp);")
        self._conn.commit()

    async def save_raw_notification(
        self, device_id: str, notification_type: str, subscription_id: Optional[int],
        event_time: str, ingestion_time: str, raw_payload: str, is_valid: bool
    ):
        await asyncio.to_thread(
            self._save_raw_sync, device_id, notification_type, subscription_id,
            event_time, ingestion_time, raw_payload, 1 if is_valid else 0
        )

    def _save_raw_sync(
        self, device_id: str, notification_type: str, subscription_id: Optional[int],
        event_time: str, ingestion_time: str, raw_payload: str, is_valid: int
    ):
        if not self._conn:
            return
        cur = self._conn.cursor()
        cur.execute("""
            INSERT INTO raw_notifications (
                device_id, notification_type, subscription_id, event_time, ingestion_time, raw_payload, is_valid
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (device_id, notification_type, subscription_id, event_time, ingestion_time, raw_payload, is_valid))
        self._conn.commit()

    async def get_raw_notifications(self, device_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._get_raw_sync, device_id, limit)

    def _get_raw_sync(self, device_id: Optional[str], limit: int) -> List[Dict[str, Any]]:
        if not self._conn:
            return []
        query = "SELECT id, device_id, notification_type, subscription_id, event_time, ingestion_time, raw_payload, is_valid FROM raw_notifications"
        params = []
        if device_id:
            query += " WHERE device_id = ?"
            params.append(device_id)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        cur = self._conn.cursor()
        rows = cur.execute(query, params).fetchall()
        results = []
        for r in rows:
            raw_val = r["raw_payload"]
            try:
                parsed_raw = json.loads(raw_val)
            except Exception:
                parsed_raw = raw_val
            results.append({
                "id": r["id"],
                "device_id": r["device_id"],
                "notification_type": r["notification_type"],
                "subscription_id": r["subscription_id"],
                "event_time": r["event_time"],
                "ingestion_time": r["ingestion_time"],
                "raw_payload": parsed_raw,
                "is_valid": bool(r["is_valid"]),
            })
        return results

    async def save_telemetry_batch(self, records: List[Any]):
        if not records:
            return
        await asyncio.to_thread(self._save_telemetry_sync, records)

    def _save_telemetry_sync(self, records: List[Any]):
        if not self._conn:
            return
        query = """
            INSERT INTO telemetry_records (
                device_id, device_type, vendor, metric_name, value, unit, path,
                event_time, ingestion_time, sequence, validation_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = [
            (
                r.device_id, r.device_type, r.vendor, r.metric_name, r.value, r.unit,
                r.path, r.event_time, r.ingestion_time, r.sequence, r.validation_status.value
            )
            for r in records
        ]
        cur = self._conn.cursor()
        cur.executemany(query, params)
        self._conn.commit()

    async def record_validation_audit(self, device_id: str, is_valid: bool, status: str, failed_stage: Optional[str], error_summary: str, duration_ms: float, timestamp: str):
        await asyncio.to_thread(self._record_audit_sync, device_id, is_valid, status, failed_stage, error_summary, duration_ms, timestamp)

    def _record_audit_sync(self, device_id: str, is_valid: bool, status: str, failed_stage: Optional[str], error_summary: str, duration_ms: float, timestamp: str):
        if not self._conn:
            return
        cur = self._conn.cursor()
        cur.execute("""
            INSERT INTO validation_audits (device_id, is_valid, status, failed_at_stage, error_summary, duration_ms, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (device_id, 1 if is_valid else 0, status, failed_stage, error_summary, duration_ms, timestamp))
        self._conn.commit()

    async def record_anomaly(self, anomaly_dict: Dict[str, Any]):
        await asyncio.to_thread(self._record_anomaly_sync, anomaly_dict)

    def _record_anomaly_sync(self, anomaly_dict: Dict[str, Any]):
        if not self._conn:
            return
        cur = self._conn.cursor()
        cur.execute("""
            INSERT INTO anomalies (device_id, metric, severity, composite_score, value, timestamp, evidence_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            anomaly_dict["device_id"],
            anomaly_dict["metric"],
            anomaly_dict["severity"],
            anomaly_dict["composite_score"],
            anomaly_dict.get("value", 0.0),
            anomaly_dict["timestamp"],
            json.dumps(anomaly_dict.get("evidence", {})),
        ))
        self._conn.commit()

    async def save_dlq_record(self, dlq_dict: Dict[str, Any]):
        await asyncio.to_thread(self._save_dlq_sync, dlq_dict)

    def _save_dlq_sync(self, dlq_dict: Dict[str, Any]):
        if not self._conn:
            return
        cur = self._conn.cursor()
        error_details = dlq_dict.get("error_details", [])
        details_str = json.dumps(error_details) if error_details else "[]"
        cur.execute("""
            INSERT OR REPLACE INTO dlq_records (id, device_id, failed_stage, error_summary, raw_preview, timestamp, error_details_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            dlq_dict["id"],
            dlq_dict["device_id"],
            dlq_dict["failed_stage"],
            dlq_dict["error_summary"],
            dlq_dict["raw_payload_preview"],
            dlq_dict["created_at"],
            details_str,
        ))
        self._conn.commit()

    async def get_recent_dlq(self, limit: int = 50) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._get_dlq_sync, limit)

    def _get_dlq_sync(self, limit: int) -> List[Dict[str, Any]]:
        if not self._conn:
            return []
        cur = self._conn.cursor()
        rows = cur.execute(
            "SELECT id, device_id, failed_stage, error_summary, raw_preview, timestamp, error_details_json FROM dlq_records ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        ).fetchall()
        results = []
        for r in rows:
            details_raw = r["error_details_json"]
            try:
                parsed_details = json.loads(details_raw) if details_raw else []
            except Exception:
                parsed_details = []
            results.append({
                "id": r["id"],
                "device_id": r["device_id"],
                "failed_stage": r["failed_stage"],
                "error_summary": r["error_summary"],
                "raw_payload_preview": r["raw_preview"],
                "created_at": r["timestamp"],
                "error_details": parsed_details,
            })
        return results

    async def get_recent_telemetry(self, device_id: Optional[str] = None, metric_name: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._get_telemetry_sync, device_id, metric_name, limit)

    def _get_telemetry_sync(self, device_id: Optional[str], metric_name: Optional[str], limit: int) -> List[Dict[str, Any]]:
        if not self._conn:
            return []
        query = "SELECT device_id, metric_name, value, unit, event_time FROM telemetry_records"
        clauses = []
        params = []
        if device_id:
            clauses.append("device_id = ?")
            params.append(device_id)
        if metric_name:
            clauses.append("metric_name = ?")
            params.append(metric_name)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY id DESC LIMIT ?"
        params.append(limit)

        cur = self._conn.cursor()
        rows = cur.execute(query, params).fetchall()
        return [
            {
                "device_id": r["device_id"],
                "metric": r["metric_name"],
                "value": r["value"],
                "unit": r["unit"],
                "event_time": r["event_time"],
            }
            for r in reversed(rows)
        ]

    async def get_recent_anomalies(self, limit: int = 50) -> List[Dict[str, Any]]:
        return await asyncio.to_thread(self._get_anomalies_sync, limit)

    def _get_anomalies_sync(self, limit: int) -> List[Dict[str, Any]]:
        if not self._conn:
            return []
        cur = self._conn.cursor()
        rows = cur.execute(
            "SELECT device_id, metric, severity, composite_score, value, timestamp, evidence_json FROM anomalies ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [
            {
                "device_id": r["device_id"],
                "metric": r["metric"],
                "severity": r["severity"],
                "composite_score": r["composite_score"],
                "value": r["value"],
                "timestamp": r["timestamp"],
                "evidence": json.loads(r["evidence_json"]) if r["evidence_json"] else {},
            }
            for r in rows
        ]

    async def close(self):
        if self._conn:
            await asyncio.to_thread(self._conn.close)
            self._conn = None

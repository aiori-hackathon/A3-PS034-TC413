"""
deduplicator.py & gap_detector.py — Delivery reliability, deduplication, and message loss tracking.
"""
import hashlib
import json
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple


class Deduplicator:
    """
    Detects duplicate telemetry messages using a sliding window of payload fingerprints.
    """

    def __init__(self, window_size: int = 2000, ttl_seconds: float = 60.0):
        self.window_size = window_size
        self.ttl_seconds = ttl_seconds
        self._seen_hashes: Dict[str, float] = {}  # hash -> timestamp
        self._hash_order: deque[Tuple[str, float]] = deque()
        self.total_checked = 0
        self.total_duplicates = 0

    def compute_fingerprint(self, device_id: str, event_time: str, sub_id: int, datastore: Dict[str, Any]) -> str:
        key_str = f"{device_id}|{event_time}|{sub_id}|" + json.dumps(datastore, sort_keys=True)
        return hashlib.sha256(key_str.encode("utf-8")).hexdigest()

    def check_and_record(self, fingerprint: str) -> bool:
        """
        Returns True if message is a DUPLICATE, False if UNIQUE.
        """
        now = time.time()
        self.total_checked += 1
        self._cleanup(now)

        if fingerprint in self._seen_hashes:
            self.total_duplicates += 1
            return True

        self._seen_hashes[fingerprint] = now
        self._hash_order.append((fingerprint, now))

        if len(self._hash_order) > self.window_size:
            old_h, _ = self._hash_order.popleft()
            self._seen_hashes.pop(old_h, None)

        return False

    def _cleanup(self, now: float):
        while self._hash_order and (now - self._hash_order[0][1] > self.ttl_seconds):
            old_h, _ = self._hash_order.popleft()
            self._seen_hashes.pop(old_h, None)


class GapDetector:
    """
    Monitors message delivery sequences and timing to detect dropped or out-of-order packets.
    """

    def __init__(self):
        self.device_stats: Dict[str, Dict[str, Any]] = {}
        self.total_received = 0
        self.total_dropped_estimated = 0
        self.total_delays_ms = 0.0

    def inspect_delivery(self, device_id: str, reported_tick: Optional[int], event_time_iso: str, ingestion_time_iso: str) -> Dict[str, Any]:
        self.total_received += 1
        now = time.time()

        # Calculate delivery delay
        delay_ms = 0.0
        try:
            ev_dt = datetime.fromisoformat(event_time_iso.replace("Z", "+00:00"))
            ing_dt = datetime.fromisoformat(ingestion_time_iso.replace("Z", "+00:00"))
            delay_ms = max(0.0, (ing_dt - ev_dt).total_seconds() * 1000.0)
        except Exception:
            pass

        self.total_delays_ms += delay_ms

        if device_id not in self.device_stats:
            self.device_stats[device_id] = {
                "last_tick": reported_tick,
                "received_count": 1,
                "gap_count": 0,
                "lost_ticks_estimated": 0,
                "last_seen_time": now,
                "avg_delay_ms": delay_ms,
            }
            return {
                "is_gap": False,
                "gap_size": 0,
                "delivery_delay_ms": round(delay_ms, 2),
            }

        stats = self.device_stats[device_id]
        stats["received_count"] += 1
        stats["last_seen_time"] = now

        # Update running average delay
        stats["avg_delay_ms"] = (0.9 * stats["avg_delay_ms"]) + (0.1 * delay_ms)

        is_gap = False
        gap_size = 0
        if reported_tick is not None and stats["last_tick"] is not None:
            expected_tick = stats["last_tick"] + 1
            if reported_tick > expected_tick:
                is_gap = True
                gap_size = reported_tick - expected_tick
                stats["gap_count"] += 1
                stats["lost_ticks_estimated"] += gap_size
                self.total_dropped_estimated += gap_size

            stats["last_tick"] = reported_tick

        return {
            "is_gap": is_gap,
            "gap_size": gap_size,
            "delivery_delay_ms": round(delay_ms, 2),
        }

    def get_reliability_summary(self) -> Dict[str, Any]:
        avg_delay = (
            self.total_delays_ms / self.total_received
            if self.total_received > 0
            else 0.0
        )
        loss_rate = (
            (self.total_dropped_estimated / (self.total_received + self.total_dropped_estimated)) * 100.0
            if (self.total_received + self.total_dropped_estimated) > 0
            else 0.0
        )
        return {
            "total_messages_received": self.total_received,
            "total_dropped_estimated": self.total_dropped_estimated,
            "estimated_loss_rate_pct": round(loss_rate, 3),
            "average_delivery_delay_ms": round(avg_delay, 2),
            "device_stats": self.device_stats,
        }

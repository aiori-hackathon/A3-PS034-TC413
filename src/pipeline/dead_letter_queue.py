"""
dead_letter_queue.py — Manages quarantined payloads that fail YANG schema validation.
"""
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import uuid


@dataclass
class QuarantinedMessage:
    id: str
    device_id: str
    ingestion_time: str
    failed_stage: str
    error_summary: str
    error_details: List[Dict[str, Any]]
    raw_payload_preview: str
    subscription_id: Optional[int] = None
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class DeadLetterQueue:
    """
    In-memory and persistent quarantine repository for non-conformant telemetry.
    """

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._queue: List[QuarantinedMessage] = []
        self._counts_by_stage: Dict[str, int] = {}
        self._counts_by_error: Dict[str, int] = {}

    def quarantine(
        self,
        device_id: str,
        failed_stage: str,
        error_details: List[Dict[str, Any]],
        raw_payload: Any,
        subscription_id: Optional[int] = None,
    ) -> QuarantinedMessage:
        raw_str = str(raw_payload) if not isinstance(raw_payload, str) else raw_payload
        preview = raw_str[:300] + ("..." if len(raw_str) > 300 else "")

        summary = (
            error_details[0].get("message", "Validation failed")
            if error_details
            else f"Failed at {failed_stage}"
        )

        msg = QuarantinedMessage(
            id=str(uuid.uuid4())[:8],
            device_id=device_id,
            ingestion_time=datetime.now(timezone.utc).isoformat(),
            failed_stage=failed_stage,
            error_summary=summary,
            error_details=error_details,
            raw_payload_preview=preview,
            subscription_id=subscription_id,
        )

        self._queue.insert(0, msg)
        if len(self._queue) > self.max_size:
            self._queue.pop()

        self._counts_by_stage[failed_stage] = self._counts_by_stage.get(failed_stage, 0) + 1
        for err in error_details:
            code = err.get("error_code", "UNKNOWN")
            self._counts_by_error[code] = self._counts_by_error.get(code, 0) + 1

        return msg

    def get_recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        return [asdict(m) for m in self._queue[:limit]]

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_quarantined": len(self._queue),
            "by_stage": self._counts_by_stage,
            "by_error_code": self._counts_by_error,
        }

    def clear(self):
        self._queue.clear()
        self._counts_by_stage.clear()
        self._counts_by_error.clear()

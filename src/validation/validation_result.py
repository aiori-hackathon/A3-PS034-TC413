"""
validation_result.py — Structured validation reporting classes.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


class ValidationStage(str, Enum):
    SYNTAX = "STAGE_1_SYNTAX"
    ENVELOPE = "STAGE_2_ENVELOPE"
    RESOLUTION = "STAGE_3_SCHEMA_RESOLUTION"
    STRUCTURE = "STAGE_4_STRUCTURE"
    CONSTRAINTS = "STAGE_5_TYPES_AND_CONSTRAINTS"
    SEMANTICS = "STAGE_6_SEMANTICS"


class ValidationStatus(str, Enum):
    VALID = "VALID"
    INVALID = "INVALID"
    PARTIAL = "PARTIAL"


@dataclass
class ValidationErrorDetail:
    stage: ValidationStage
    path: str
    error_code: str
    message: str
    expected: Optional[Any] = None
    actual: Optional[Any] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stage": self.stage.value,
            "path": self.path,
            "error_code": self.error_code,
            "message": self.message,
            "expected": str(self.expected) if self.expected is not None else None,
            "actual": str(self.actual) if self.actual is not None else None,
        }


@dataclass
class ValidationReport:
    is_valid: bool
    status: ValidationStatus
    errors: List[ValidationErrorDetail] = field(default_factory=list)
    stages_passed: List[ValidationStage] = field(default_factory=list)
    failed_at_stage: Optional[ValidationStage] = None
    schema_module: Optional[str] = None
    subscription_id: Optional[int] = None
    event_time: Optional[str] = None
    duration_ms: float = 0.0
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "status": self.status.value,
            "failed_at_stage": self.failed_at_stage.value if self.failed_at_stage else None,
            "stages_passed": [s.value for s in self.stages_passed],
            "errors": [e.to_dict() for e in self.errors],
            "schema_module": self.schema_module,
            "subscription_id": self.subscription_id,
            "event_time": self.event_time,
            "duration_ms": round(self.duration_ms, 3),
            "timestamp": self.timestamp,
        }

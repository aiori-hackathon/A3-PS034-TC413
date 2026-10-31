"""
envelope_validator.py — RFC 8641 / RFC 8639 YANG-Push envelope validation.
Validates the outermost notification envelope structure, event timestamp format,
subscription IDs, and datastore contents wrapper.
"""
from datetime import datetime
from typing import Any, Dict, Optional, Tuple
from .validation_result import ValidationErrorDetail, ValidationStage


class EnvelopeValidator:
    """
    Validates that a telemetry message conforms to the YANG-Push envelope specification.
    Supports RFC 8641 push-update (periodic) and push-change-update (on-change).
    """

    @staticmethod
    def validate(data: Any) -> Tuple[bool, Optional[Dict[str, Any]], Optional[ValidationErrorDetail]]:
        if not isinstance(data, dict):
            return False, None, ValidationErrorDetail(
                stage=ValidationStage.ENVELOPE,
                path="/",
                error_code="ENVELOPE_NOT_OBJECT",
                message="Message must be a JSON object",
                expected="dict",
                actual=type(data).__name__,
            )

        notification = data.get("ietf-notification:notification")
        if notification is None:
            return False, None, ValidationErrorDetail(
                stage=ValidationStage.ENVELOPE,
                path="/ietf-notification:notification",
                error_code="MISSING_NOTIFICATION_WRAPPER",
                message="Missing top-level 'ietf-notification:notification' wrapper",
                expected="ietf-notification:notification object",
                actual="missing",
            )

        if not isinstance(notification, dict):
            return False, None, ValidationErrorDetail(
                stage=ValidationStage.ENVELOPE,
                path="/ietf-notification:notification",
                error_code="INVALID_NOTIFICATION_WRAPPER",
                message="Top-level 'ietf-notification:notification' wrapper must be a JSON object",
                expected="dict",
                actual=type(notification).__name__,
            )

        event_time = notification.get("eventTime")
        if not event_time or not isinstance(event_time, str):
            return False, None, ValidationErrorDetail(
                stage=ValidationStage.ENVELOPE,
                path="/ietf-notification:notification/eventTime",
                error_code="MISSING_EVENT_TIME",
                message="Missing or invalid 'eventTime' timestamp in notification envelope",
                expected="ISO 8601 string",
                actual=type(event_time).__name__ if event_time is not None else "missing",
            )

        # Validate ISO 8601 timestamp compliance
        try:
            # Normalize Z suffix to +00:00 for fromisoformat
            norm_time = event_time.replace("Z", "+00:00")
            datetime.fromisoformat(norm_time)
        except (ValueError, TypeError):
            return False, None, ValidationErrorDetail(
                stage=ValidationStage.ENVELOPE,
                path="/ietf-notification:notification/eventTime",
                error_code="INVALID_ISO8601_TIMESTAMP",
                message=f"Timestamp '{event_time}' is not a valid ISO 8601 timestamp (RFC 3339)",
                expected="ISO 8601 string (e.g. 2026-10-31T12:00:00Z)",
                actual=event_time,
            )

        push_update = notification.get("ietf-yang-push:push-update")
        push_change = notification.get("ietf-yang-push:push-change-update")

        if push_update is None and push_change is None:
            return False, None, ValidationErrorDetail(
                stage=ValidationStage.ENVELOPE,
                path="/ietf-notification:notification",
                error_code="MISSING_PUSH_PAYLOAD",
                message="Notification must contain either 'ietf-yang-push:push-update' or 'ietf-yang-push:push-change-update'",
                expected="push-update or push-change-update",
                actual="neither found",
            )

        if push_update is not None and not isinstance(push_update, dict):
            return False, None, ValidationErrorDetail(
                stage=ValidationStage.ENVELOPE,
                path="/ietf-notification:notification/ietf-yang-push:push-update",
                error_code="INVALID_PUSH_UPDATE_PAYLOAD",
                message="'ietf-yang-push:push-update' must be a JSON object",
                expected="dict",
                actual=type(push_update).__name__,
            )

        if push_change is not None and not isinstance(push_change, dict):
            return False, None, ValidationErrorDetail(
                stage=ValidationStage.ENVELOPE,
                path="/ietf-notification:notification/ietf-yang-push:push-change-update",
                error_code="INVALID_PUSH_CHANGE_PAYLOAD",
                message="'ietf-yang-push:push-change-update' must be a JSON object",
                expected="dict",
                actual=type(push_change).__name__,
            )

        content = push_update if push_update is not None else push_change
        push_type = "push-update" if push_update is not None else "push-change-update"

        sub_id = content.get("id")
        if sub_id is None or not isinstance(sub_id, int):
            return False, None, ValidationErrorDetail(
                stage=ValidationStage.ENVELOPE,
                path=f"/ietf-notification:notification/ietf-yang-push:{push_type}/id",
                error_code="INVALID_SUBSCRIPTION_ID",
                message="Missing or non-integer subscription 'id' in push notification",
                expected="integer subscription ID",
                actual=type(sub_id).__name__ if sub_id is not None else "missing",
            )

        # Datastore contents check
        if push_type == "push-update":
            contents = content.get("datastore-contents")
            if contents is None or not isinstance(contents, dict):
                return False, None, ValidationErrorDetail(
                    stage=ValidationStage.ENVELOPE,
                    path=f"/ietf-notification:notification/ietf-yang-push:{push_type}/datastore-contents",
                    error_code="MISSING_DATASTORE_CONTENTS",
                    message="Periodic push-update notification must contain a 'datastore-contents' object",
                    expected="dict",
                    actual=type(contents).__name__ if contents is not None else "missing",
                )
        else:
            changes = content.get("datastore-changes")
            if changes is None or not isinstance(changes, dict):
                return False, None, ValidationErrorDetail(
                    stage=ValidationStage.ENVELOPE,
                    path=f"/ietf-notification:notification/ietf-yang-push:{push_type}/datastore-changes",
                    error_code="MISSING_DATASTORE_CHANGES",
                    message="On-change notification must contain a 'datastore-changes' object",
                    expected="dict",
                    actual=type(changes).__name__ if changes is not None else "missing",
                )

        envelope_metadata = {
            "event_time": event_time,
            "subscription_id": sub_id,
            "type": push_type,
            "datastore_data": content.get("datastore-contents") if push_type == "push-update" else content.get("datastore-changes"),
        }
        return True, envelope_metadata, None

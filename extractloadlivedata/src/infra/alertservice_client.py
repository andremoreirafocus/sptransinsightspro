import json
from typing import Any, Dict
from urllib import request
from src.infra.structured_logging import get_structured_logger
from src.domain.events import ALLOWED_EVENTS, ALLOWED_EVENT_STATUSES, LogStatus

structured_logger = get_structured_logger(
    service="extractloadlivedata",
    component="alertservice_client",
    logger_name=__name__,
    allowed_events=ALLOWED_EVENTS,
    allowed_statuses=ALLOWED_EVENT_STATUSES,
)


def build_alert_payload(report: Dict[str, Any]) -> bytes:
    return json.dumps({"summary": report}).encode("utf-8")


def send_alert(webhook_url: str, report: Dict[str, Any]) -> None:
    if not webhook_url or webhook_url.strip().lower() in {"disabled", "none", "null"}:
        structured_logger.info(
            event="notification_dispatch_succeeded",
            status=LogStatus.SKIPPED,
            message="Alertservice webhook disabled or missing. Skipping alert send.",
        )
        return

    try:
        payload = build_alert_payload(report)
        req = request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=5) as resp:
            status = resp.getcode()
            if status and status >= 400:
                structured_logger.error(
                    event="notification_dispatch_failed",
                    status=LogStatus.FAILED,
                    message=f"Alertservice webhook returned status {status}",
                )
                return
            structured_logger.info(
                event="execution_summary_emitted",
                status=LogStatus.SUCCEEDED,
                message=f"Alertservice notification sent successfully (status={status}).",
            )
    except Exception as e:
        structured_logger.error(
            event="notification_dispatch_failed",
            status=LogStatus.FAILED,
            message=f"Failed to send alertservice notification: {e}",
        )

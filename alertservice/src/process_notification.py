import json
import logging
from typing import Any, Callable, Dict, Iterable

from .notifications_evaluator import evaluate_cumulative_warn

SummaryStore = Callable[[Dict[str, Any]], None]
QueryWindow = Callable[[str, str, int], Iterable[Dict[str, Any]]]
EmailSender = Callable[[str, str], None]
Formatter = Callable[[Dict[str, Any]], str]

logger = logging.getLogger(__name__)

VALID_STATUSES = {"FAIL", "WARN", "PASS"}


def process_notification(
    summary: Dict[str, Any],
    pipeline_config: Dict[str, Any],
    subject_prefix: str,
    store_summary: SummaryStore,
    query_window: QueryWindow,
    send_email: EmailSender,
    format_summary: Formatter,
) -> Dict[str, Any]:
    if not summary:
        raise ValueError("summary is required")
    if "pipeline" not in summary or "status" not in summary:
        raise ValueError("summary.pipeline and summary.status are required")
    pipeline = summary["pipeline"]
    status = summary["status"]
    if status not in VALID_STATUSES:
        logger.warning(
            "Unknown status '%s' for pipeline '%s'. Ignoring.", status, pipeline
        )
        return {"status": "ignored", "reason": "unknown_status"}
    if status == "PASS":
        store_summary(summary)
        return {"status": "stored", "reason": "pass_status"}
    pipeline_cfg = pipeline_config.get(pipeline)
    if pipeline_cfg is None:
        logger.error("Unknown pipeline config: %s. Ignoring request.", pipeline)
        return {"status": "ignored", "reason": "unknown_pipeline"}

    logger.info("Received notification: pipeline=%s status=%s", pipeline, status)
    if status == "FAIL":
        logger.info(
            "Received summary payload:\n%s",
            json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        )
    elif logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "Received summary payload:\n%s",
            json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        )
    store_summary(summary)
    notify_on_fail = pipeline_cfg["notify_on_fail"]
    notify_on_warn = pipeline_cfg["notify_on_warn"]
    subject = f"{subject_prefix} {pipeline}: {status}"
    body = format_summary(summary)
    if status == "FAIL" and notify_on_fail:
        send_email(subject, body)
        return {"status": "sent", "reason": "fail"}
    if status == "WARN" and notify_on_warn:
        warning_cfg = pipeline_cfg["warning_window"]
        thresholds = pipeline_cfg["warning_thresholds"]
        window_type = warning_cfg["type"]
        window_value = warning_cfg["value"]
        rows = query_window(pipeline, window_type, window_value)
        if evaluate_cumulative_warn(rows, thresholds):
            send_email(subject, body)
            return {"status": "sent", "reason": "cumulative_warn"}
    return {"status": "stored"}

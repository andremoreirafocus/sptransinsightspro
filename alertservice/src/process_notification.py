import json
import logging
from typing import Any, Dict

from .infra.config import get_email_subject_prefix, load_pipeline_config
from .notifications_evaluator import evaluate_cumulative_warn
from .infra.notifier import format_summary, send_email
from .infra.storage import store_summary

logger = logging.getLogger(__name__)


def process_notification(payload: Any) -> Dict[str, Any]:
    summary = getattr(payload, "summary", None) if payload is not None else None
    if not summary:
        raise ValueError("summary is required")
    if "pipeline" not in summary or "status" not in summary:
        raise ValueError("summary.pipeline and summary.status are required")
    pipeline_config = load_pipeline_config()
    email_subject_prefix = get_email_subject_prefix()
    pipeline = summary["pipeline"]
    status = summary["status"]
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
    else:
        logger.debug(
            "Received summary payload:\n%s",
            json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        )
    store_summary(summary)
    notify_on_fail = pipeline_cfg["notify_on_fail"]
    notify_on_warn = pipeline_cfg["notify_on_warn"]
    subject = f"{email_subject_prefix} {pipeline} - {status}"
    body = format_summary(summary)
    if status == "FAIL" and notify_on_fail:
        send_email(subject, body)
        return {"status": "sent", "reason": "fail"}
    if status == "WARN" and notify_on_warn:
        if evaluate_cumulative_warn(pipeline, pipeline_config):
            send_email(subject, body)
            return {"status": "sent", "reason": "cumulative_warn"}
    return {"status": "stored"}

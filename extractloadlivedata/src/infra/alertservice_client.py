import json
import logging
from typing import Any, Dict
from urllib import request

logger = logging.getLogger(__name__)


def build_alert_payload(report: Dict[str, Any]) -> bytes:
    return json.dumps({"summary": report}).encode("utf-8")


def send_alert(webhook_url: str, report: Dict[str, Any]) -> None:
    if not webhook_url or webhook_url.strip().lower() in {"disabled", "none", "null"}:
        logger.info("Alertservice webhook disabled or missing. Skipping alert send.")
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
                logger.error("Alertservice webhook returned status %s", status)
                return
            logger.info("Alertservice notification sent successfully (status=%s).", status)
    except Exception as e:
        logger.error("Failed to send alertservice notification: %s", e, exc_info=True)

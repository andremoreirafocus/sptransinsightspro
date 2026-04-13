import json
import logging
from typing import Any, Dict
from urllib import request

logger = logging.getLogger(__name__)


def send_webhook(summary: Dict[str, Any], webhook_url: str, timeout_seconds: int = 5) -> bool:
    if not webhook_url:
        raise ValueError("webhook_url is required")
    if webhook_url.strip().lower() in {"disabled", "none", "null"}:
        return False
    payload = json.dumps({"summary": summary}).encode("utf-8")
    req = request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout_seconds) as resp:
            status = resp.getcode()
            if status and status >= 400:
                raise RuntimeError(f"Webhook returned status {status}")
    except Exception as e:
        logger.error("Failed to send webhook notification: %s", e)
        raise
    return True

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, TextIO

from src.infra.structured_event_logger import (
    StructuredEventLogger,
    get_structured_logger,
)


class JsonLineFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self._service = service

    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()

        try:
            parsed = json.loads(message)
            if isinstance(parsed, dict):
                return message
        except Exception:
            pass

        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self._service,
            "component": record.name,
            "event": "python_log",
            "message": message,
        }
        return json.dumps(payload, ensure_ascii=False, default=str)


def get_process_structured_logger(
    *,
    service: str,
    component: str,
    logger_name: Optional[str] = None,
    base_metadata: Optional[Dict[str, Any]] = None,
    allowed_events: Optional[Iterable[str]] = None,
    allowed_statuses: Optional[Iterable[str]] = None,
    level: int = logging.INFO,
    stream: Optional[TextIO] = None,
) -> StructuredEventLogger:
    target_stream = stream if stream is not None else sys.stdout
    logging.basicConfig(
        level=level,
        handlers=[logging.StreamHandler(target_stream)],
    )
    formatter = JsonLineFormatter(service=service)
    for handler in logging.getLogger().handlers:
        handler.setFormatter(formatter)
    return get_structured_logger(
        service=service,
        component=component,
        logger_name=logger_name,
        base_metadata=base_metadata,
        allowed_events=allowed_events,
        allowed_statuses=allowed_statuses,
    )

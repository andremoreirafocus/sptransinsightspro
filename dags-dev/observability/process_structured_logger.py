import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional, TextIO

from observability.structured_event_logger import (
    StructuredEventLogger,
    get_structured_logger,
)


class JsonLineFormatter(logging.Formatter):
    def __init__(self, service: str, _json_loads=json.loads) -> None:
        super().__init__()
        self._service = service
        self._json_loads = _json_loads

    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()

        try:
            parsed = self._json_loads(message)
            if isinstance(parsed, dict):
                return message
        except json.JSONDecodeError:
            pass
        except Exception as e:
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": "ERROR",
                "service": self._service,
                "component": record.name,
                "event": "log_formatter_error",
                "message": f"JsonLineFormatter failed to process log record: {e}",
            }
            return json.dumps(payload, ensure_ascii=False, default=str)

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
    )

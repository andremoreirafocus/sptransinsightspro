import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from src.domain.events import LogEventType, LogLevel, LogStatusType

ALLOWED_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
LEVEL_TO_LOGGING = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_non_empty_string(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"'{field_name}' must be a non-empty string.")
    return value.strip()


def _normalize_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if metadata is None:
        return {}
    if not isinstance(metadata, dict):
        raise ValueError("'metadata' must be a dict when provided.")
    return metadata


def _normalize_status_value(status: LogStatusType, field_name: str) -> str:
    return _normalize_non_empty_string(status, field_name)


@dataclass(frozen=True)
class StructuredEventLogger:
    service: str
    component: str
    logger: logging.Logger
    base_metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "service", _normalize_non_empty_string(self.service, "service")
        )
        object.__setattr__(
            self, "component", _normalize_non_empty_string(self.component, "component")
        )
        object.__setattr__(
            self, "base_metadata", _normalize_metadata(self.base_metadata)
        )

    def emit(
        self,
        *,
        level: LogLevel,
        event: LogEventType,
        message: str,
        execution_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        status: Optional[LogStatusType] = None,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        normalized_level = _normalize_non_empty_string(level, "level").upper()
        if normalized_level not in ALLOWED_LEVELS:
            raise ValueError(
                f"Unsupported level '{normalized_level}'. Allowed levels: {sorted(ALLOWED_LEVELS)}"
            )

        payload: Dict[str, Any] = {
            "timestamp": _utc_timestamp(),
            "level": normalized_level,
            "service": self.service,
            "component": self.component,
            "event": _normalize_non_empty_string(event, "event"),
            "message": _normalize_non_empty_string(message, "message"),
        }

        if execution_id is not None:
            payload["execution_id"] = execution_id
        if correlation_id is not None:
            payload["correlation_id"] = correlation_id
        if status is not None:
            normalized_status = _normalize_status_value(status, "status").upper()
            payload["status"] = normalized_status
        if error_type is not None:
            payload["error_type"] = error_type
        if error_message is not None:
            payload["error_message"] = error_message

        merged_metadata = dict(self.base_metadata)
        merged_metadata.update(_normalize_metadata(metadata))
        if merged_metadata:
            payload["metadata"] = merged_metadata

        self.logger.log(
            LEVEL_TO_LOGGING[normalized_level],
            json.dumps(payload, ensure_ascii=False, default=str),
        )
        return payload

    def debug(self, *, event: LogEventType, message: str, **kwargs: Any) -> Dict[str, Any]:
        return self.emit(level="DEBUG", event=event, message=message, **kwargs)

    def info(self, *, event: LogEventType, message: str, **kwargs: Any) -> Dict[str, Any]:
        return self.emit(level="INFO", event=event, message=message, **kwargs)

    def warning(self, *, event: LogEventType, message: str, **kwargs: Any) -> Dict[str, Any]:
        return self.emit(level="WARNING", event=event, message=message, **kwargs)

    def error(self, *, event: LogEventType, message: str, **kwargs: Any) -> Dict[str, Any]:
        return self.emit(level="ERROR", event=event, message=message, **kwargs)

    def critical(self, *, event: LogEventType, message: str, **kwargs: Any) -> Dict[str, Any]:
        return self.emit(level="CRITICAL", event=event, message=message, **kwargs)


def get_structured_logger(
    *,
    service: str,
    component: str,
    logger_name: Optional[str] = None,
    base_metadata: Optional[Dict[str, Any]] = None,
) -> StructuredEventLogger:
    target_name = logger_name.strip() if logger_name and logger_name.strip() else service
    return StructuredEventLogger(
        service=service,
        component=component,
        logger=logging.getLogger(target_name),
        base_metadata=base_metadata or {},
    )

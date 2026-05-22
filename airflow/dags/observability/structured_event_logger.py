import json
import logging
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional, TypeAlias

EVENT_STATUS_STARTED = "STARTED"
EVENT_STATUS_SUCCEEDED = "SUCCEEDED"
EVENT_STATUS_FAILED = "FAILED"
EVENT_STATUS_RETRY = "RETRY"
EVENT_STATUS_SKIPPED = "SKIPPED"

LogLevel: TypeAlias = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
LogStatusType: TypeAlias = Literal["STARTED", "SUCCEEDED", "FAILED", "RETRY", "SKIPPED"]

ALLOWED_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
LEVEL_TO_LOGGING = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


_execution_context: ContextVar[Optional[Dict[str, str]]] = ContextVar(
    "_execution_context", default=None
)


def set_execution_context(execution_id: str, correlation_id: str) -> None:
    _execution_context.set({"execution_id": execution_id, "correlation_id": correlation_id})


def clear_execution_context() -> None:
    _execution_context.set(None)


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
        event: str,
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

        context = _execution_context.get()
        resolved_execution_id = execution_id if execution_id is not None else (context.get("execution_id") if context else None)
        resolved_correlation_id = correlation_id if correlation_id is not None else (context.get("correlation_id") if context else None)
        if resolved_execution_id is not None:
            payload["execution_id"] = resolved_execution_id
        if resolved_correlation_id is not None:
            payload["correlation_id"] = resolved_correlation_id
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

    def debug(self, *, event: str, message: str, **kwargs: Any) -> Dict[str, Any]:
        return self.emit(level="DEBUG", event=event, message=message, **kwargs)

    def info(self, *, event: str, message: str, **kwargs: Any) -> Dict[str, Any]:
        return self.emit(level="INFO", event=event, message=message, **kwargs)

    def warning(self, *, event: str, message: str, **kwargs: Any) -> Dict[str, Any]:
        return self.emit(level="WARNING", event=event, message=message, **kwargs)

    def error(self, *, event: str, message: str, **kwargs: Any) -> Dict[str, Any]:
        return self.emit(level="ERROR", event=event, message=message, **kwargs)

    def critical(self, *, event: str, message: str, **kwargs: Any) -> Dict[str, Any]:
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

from typing import Any, Dict

from observability.structured_event_logger import StructuredEventLogger
from updatelatestpositions.domain.events import EventType


class UpdateLatestPositionsLogger:
    def __init__(self, inner: StructuredEventLogger) -> None:
        self._inner = inner

    def debug(self, *, event: EventType, message: str, **kwargs: Any) -> Dict[str, Any]:
        return self._inner.debug(event=event, message=message, **kwargs)

    def info(self, *, event: EventType, message: str, **kwargs: Any) -> Dict[str, Any]:
        return self._inner.info(event=event, message=message, **kwargs)

    def warning(self, *, event: EventType, message: str, **kwargs: Any) -> Dict[str, Any]:
        return self._inner.warning(event=event, message=message, **kwargs)

    def error(self, *, event: EventType, message: str, **kwargs: Any) -> Dict[str, Any]:
        return self._inner.error(event=event, message=message, **kwargs)

    def critical(self, *, event: EventType, message: str, **kwargs: Any) -> Dict[str, Any]:
        return self._inner.critical(event=event, message=message, **kwargs)

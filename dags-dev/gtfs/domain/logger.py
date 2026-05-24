from gtfs.domain.events import EventType
from observability.structured_event_logger import StructuredEventLogger


class GtfsLogger:
    def __init__(self, logger: StructuredEventLogger) -> None:
        self._logger = logger

    def info(self, *, event: EventType, message: str, **kwargs) -> None:
        self._logger.info(event=event, message=message, **kwargs)

    def warning(self, *, event: EventType, message: str, **kwargs) -> None:
        self._logger.warning(event=event, message=message, **kwargs)

    def error(self, *, event: EventType, message: str, **kwargs) -> None:
        self._logger.error(event=event, message=message, **kwargs)

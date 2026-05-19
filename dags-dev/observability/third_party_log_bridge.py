import logging
from typing import Iterable

from observability.structured_event_logger import StructuredEventLogger


_HANDLER_MARKER = "_transformlivedata_third_party_bridge"


class _ThirdPartyToStructuredHandler(logging.Handler):
    def __init__(
        self,
        *,
        structured_logger: StructuredEventLogger,
        execution_id: str,
        correlation_id: str,
    ) -> None:
        super().__init__()
        self.structured_logger = structured_logger
        self.execution_id = execution_id
        self.correlation_id = correlation_id

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
            level_name = record.levelname.upper()
            metadata = {"origin_logger": record.name}
            if level_name == "DEBUG":
                self.structured_logger.debug(
                    event="third_party_log",
                    message=message,
                    execution_id=self.execution_id,
                    correlation_id=self.correlation_id,
                    status="SUCCEEDED",
                    metadata=metadata,
                )
            elif level_name == "INFO":
                self.structured_logger.info(
                    event="third_party_log",
                    message=message,
                    execution_id=self.execution_id,
                    correlation_id=self.correlation_id,
                    status="SUCCEEDED",
                    metadata=metadata,
                )
            elif level_name == "WARNING":
                self.structured_logger.warning(
                    event="third_party_log",
                    message=message,
                    execution_id=self.execution_id,
                    correlation_id=self.correlation_id,
                    status="SUCCEEDED",
                    metadata=metadata,
                )
            elif level_name == "ERROR":
                self.structured_logger.error(
                    event="third_party_log",
                    message=message,
                    execution_id=self.execution_id,
                    correlation_id=self.correlation_id,
                    status="FAILED",
                    metadata=metadata,
                )
            else:
                self.structured_logger.critical(
                    event="third_party_log",
                    message=message,
                    execution_id=self.execution_id,
                    correlation_id=self.correlation_id,
                    status="FAILED",
                    metadata=metadata,
                )
        except Exception:
            return


def configure_third_party_log_bridge(
    *,
    structured_logger: StructuredEventLogger,
    execution_id: str,
    correlation_id: str,
    namespaces: Iterable[str],
) -> None:
    formatter = logging.Formatter("%(message)s")
    for namespace in namespaces:
        namespace_logger = logging.getLogger(namespace)
        namespace_logger.handlers = []
        namespace_logger.propagate = False
        bridge_handler = _ThirdPartyToStructuredHandler(
            structured_logger=structured_logger,
            execution_id=execution_id,
            correlation_id=correlation_id,
        )
        setattr(bridge_handler, _HANDLER_MARKER, True)
        bridge_handler.setFormatter(formatter)
        namespace_logger.addHandler(bridge_handler)

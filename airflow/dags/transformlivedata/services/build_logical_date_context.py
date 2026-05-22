from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict
from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


def build_logical_date_context(logical_date_string: str) -> Dict[str, str]:
    try:
        dt_utc = datetime.fromisoformat(logical_date_string)
        dt = dt_utc.astimezone(ZoneInfo("America/Sao_Paulo"))
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        hour = dt.strftime("%H")
        minute = dt.strftime("%M")
        hour_minute = f"{hour}{minute}"
        partition_path = f"year={year}/month={month}/day={day}/"
        source_file = f"posicoes_onibus-{year}{month}{day}{hour_minute}.json"
        return {
            "partition_path": partition_path,
            "source_file": source_file,
        }
    except Exception as e:
        structured_logger.error(
            event="build_logical_date_context_failed",
            message="Failed to build logical date context",
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
            metadata={"logical_date_string": logical_date_string},
        )
        raise ValueError("Failed to build logical date context") from e

from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict


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
        raise RuntimeError(f"Failed to build logical date context: {e}") from e

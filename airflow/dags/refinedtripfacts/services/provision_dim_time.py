from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text

from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)

_SP = ZoneInfo("America/Sao_Paulo")


def provision_dim_time(
    logic_date: datetime,
    config: Dict[str, Any],
    engine_factory: Callable = create_engine,
) -> Dict[str, Any]:
    def get_config(config):
        general = config["general"]
        database = config["connections"]["database"]
        finished_trips_table = general["tables"]["finished_trips_table_name"]
        dim_time_table = general["tables"]["dim_time_table_name"]
        db_uri = (
            f"postgresql://{database['user']}:{database['password']}"
            f"@{database['host']}:{database['port']}/{database['database']}"
        )
        return finished_trips_table, dim_time_table, db_uri

    def _generate_dim_rows(min_date: date, max_date: date) -> List[Dict[str, Any]]:
        rows = []
        current = min_date
        while current <= max_date:
            for hour in range(24):
                local_dt = datetime(current.year, current.month, current.day, hour, 0, 0, tzinfo=_SP)
                weekday = local_dt.isoweekday()
                rows.append(
                    {
                        "time_key": int(local_dt.strftime("%Y%m%d%H")),
                        "date_actual": current,
                        "month": local_dt.month,
                        "day": local_dt.day,
                        "hour_of_day": hour,
                        "weekday": weekday,
                        "is_weekend": weekday in (6, 7),
                    }
                )
            current += timedelta(days=1)
        return rows

    finished_trips_table, dim_time_table, db_uri = get_config(config)
    engine = engine_factory(db_uri)
    try:
        with engine.begin() as conn:
            span_result = conn.execute(
                text(
                    f"SELECT MIN(trip_start_time) AS min_ts, MAX(trip_end_time) AS max_ts "
                    f"FROM {finished_trips_table} WHERE logic_date = :logic_date"
                ),
                {"logic_date": logic_date},
            )
            row = span_result.fetchone()
            min_ts, max_ts = row[0], row[1]
            min_date = min_ts.astimezone(_SP).date()
            max_date = max_ts.astimezone(_SP).date()
            expected_count = ((max_date - min_date).days + 1) * 24

            count_result = conn.execute(
                text(
                    f"SELECT COUNT(*) FROM {dim_time_table} "
                    f"WHERE date_actual >= :min_date AND date_actual <= :max_date"
                ),
                {"min_date": min_date, "max_date": max_date},
            )
            existing_count = count_result.fetchone()[0]

            if existing_count >= expected_count:
                return {"rows_ensured": 0}

            rows = _generate_dim_rows(min_date, max_date)
            insert_result = conn.execute(
                text(
                    f"INSERT INTO {dim_time_table} "
                    f"(time_key, date_actual, month, day, hour_of_day, weekday, is_weekend) "
                    f"VALUES (:time_key, :date_actual, :month, :day, :hour_of_day, :weekday, :is_weekend) "
                    f"ON CONFLICT (time_key) DO NOTHING"
                ),
                rows,
            )
            return {"rows_ensured": insert_result.rowcount}
    except Exception as e:
        structured_logger.error(
            event="dim_time_provisioning_failed",
            message=(
                f"Failed to provision dim_time for logic_date '{logic_date}' "
                f"from '{finished_trips_table}': {e}"
            ),
            metadata={
                "logic_date": str(logic_date),
                "finished_trips_table": finished_trips_table,
                "dim_time_table": dim_time_table,
            },
        )
        raise ValueError(
            f"Failed to provision dim_time for logic_date '{logic_date}' "
            f"from '{finished_trips_table}': {e}"
        )
    finally:
        engine.dispose()

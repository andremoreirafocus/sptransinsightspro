from datetime import datetime
from typing import Any, Callable, Dict

from sqlalchemy import create_engine, text

from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


def create_trip_facts(
    logic_date: datetime,
    config: Dict[str, Any],
    engine_factory: Callable = create_engine,
) -> Dict[str, Any]:
    def get_config(config):
        general = config["general"]
        database = config["connections"]["database"]
        finished_trips_table = general["tables"]["finished_trips_table_name"]
        trip_facts_table = general["tables"]["trip_facts_table_name"]
        db_uri = (
            f"postgresql://{database['user']}:{database['password']}"
            f"@{database['host']}:{database['port']}/{database['database']}"
        )
        return finished_trips_table, trip_facts_table, db_uri

    finished_trips_table, trip_facts_table, db_uri = get_config(config)
    engine = engine_factory(db_uri)
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    f"WITH src AS ("
                    f"    SELECT * FROM {finished_trips_table} WHERE logic_date = :logic_date"
                    f"), "
                    f"ins AS ("
                    f"    INSERT INTO {trip_facts_table} ("
                    f"        trip_id, vehicle_id, route_id, direction,"
                    f"        started_at, ended_at, duration_seconds, duration,"
                    f"        is_circular, distance_meters, avg_speed_kmh,"
                    f"        started_at_time_dim_key, ended_at_time_dim_key, logic_date"
                    f"    )"
                    f"    SELECT"
                    f"        trip_id,"
                    f"        vehicle_id,"
                    f"        left(trip_id, length(trip_id) - 2) AS route_id,"
                    f"        CASE right(trip_id, 1) WHEN '0' THEN 1 WHEN '1' THEN 2 END AS direction,"
                    f"        trip_start_time AS started_at,"
                    f"        trip_end_time AS ended_at,"
                    f"        duration_seconds,"
                    f"        make_interval(secs => duration_seconds) AS duration,"
                    f"        is_circular, distance_meters, avg_speed_kmh,"
                    f"        to_char(trip_start_time AT TIME ZONE 'America/Sao_Paulo', 'YYYYMMDDHH24')::int AS started_at_time_dim_key,"
                    f"        to_char(trip_end_time AT TIME ZONE 'America/Sao_Paulo', 'YYYYMMDDHH24')::int AS ended_at_time_dim_key,"
                    f"        logic_date"
                    f"    FROM src"
                    f"    ON CONFLICT (started_at, vehicle_id, trip_id) DO NOTHING"
                    f"    RETURNING 1"
                    f") "
                    f"SELECT"
                    f"    (SELECT count(*) FROM src) AS facts_derived,"
                    f"    (SELECT count(*) FROM ins) AS inserted_rows"
                ),
                {"logic_date": logic_date},
            )
            row = result.fetchone()
            facts_derived = int(row[0])
            inserted_rows = int(row[1])
            return {
                "facts_derived": facts_derived,
                "inserted_rows": inserted_rows,
                "skipped_rows": facts_derived - inserted_rows,
            }
    except Exception as e:
        structured_logger.error(
            event="trip_facts_creation_failed",
            message=(
                f"Failed to create trip facts for logic_date '{logic_date}' "
                f"into '{trip_facts_table}': {e}"
            ),
            metadata={
                "logic_date": str(logic_date),
                "trip_facts_table": trip_facts_table,
                "finished_trips_table": finished_trips_table,
            },
        )
        raise ValueError(
            f"Failed to create trip facts for logic_date '{logic_date}' "
            f"into '{trip_facts_table}': {e}"
        )
    finally:
        engine.dispose()

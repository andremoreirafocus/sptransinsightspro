from datetime import date
from typing import Any, Callable, Dict, List, Tuple

from sqlalchemy import create_engine, text

from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


def save_finished_trips_to_db(config: Dict[str, Any], trips_tuples: List[Tuple], logic_date: date, engine_factory: Callable = create_engine) -> Dict[str, Any]:
    def get_config(config):
        general = config["general"]
        tables = general["tables"]
        database = config["connections"]["database"]
        table_name = tables["finished_trips_table_name"]
        host = database["host"]
        port = database["port"]
        dbname = database["database"]
        dbuser = database["user"]
        password = database["password"]
        return (table_name, host, port, dbname, dbuser, password)

    (table_name, host, port, dbname, dbuser, password) = get_config(config)
    db_uri = f"postgresql://{dbuser}:{password}@{host}:{port}/{dbname}"
    engine = engine_factory(db_uri)
    staging_table = f"{table_name}_stg"
    structured_logger.info(
        event="trips_save_started",
        message="Starting trips save",
        metadata={"staging_table": staging_table},
    )
    try:
        with engine.begin() as conn:
            max_result = conn.execute(text(f"SELECT MAX(trip_end_time) FROM {table_name}"))
            latest_trip_end_time = max_result.fetchone()[0]

        if latest_trip_end_time is not None:
            new_trips_tuples = [t for t in trips_tuples if t[3] > latest_trip_end_time]
            structured_logger.info(
                event="trips_filtered",
                message="Trips filtered",
                metadata={
                    "filtered_count": len(trips_tuples) - len(new_trips_tuples),
                    "new_count": len(new_trips_tuples),
                },
            )
        else:
            new_trips_tuples = trips_tuples

        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {staging_table};"))
            conn.execute(
                text(f"""
                CREATE UNLOGGED TABLE {staging_table}
                AS SELECT * FROM {table_name} WITH NO DATA;
            """)
            )
            if new_trips_tuples:
                insert_stmt = text(f"""
                    INSERT INTO {staging_table} (
                        trip_id, vehicle_id, trip_start_time, trip_end_time,
                        duration_seconds, is_circular, distance_meters, avg_speed_kmh, logic_date
                    ) VALUES (:t_id, :v_id, :t_start, :t_end, :dur_s, :circ, :dist_m, :spd_kmh, :logic_date)
                """)
                params = [
                    {
                        "t_id": t[0],
                        "v_id": t[1],
                        "t_start": t[2],
                        "t_end": t[3],
                        "dur_s": t[4],
                        "circ": t[5],
                        "dist_m": t[6],
                        "spd_kmh": t[7],
                        "logic_date": logic_date,
                    }
                    for t in new_trips_tuples
                ]
                conn.execute(insert_stmt, params)
        with engine.begin() as conn:
            upsert_query = text(f"""
                INSERT INTO {table_name} (
                    trip_id, vehicle_id, trip_start_time, trip_end_time,
                    duration_seconds, is_circular, distance_meters, avg_speed_kmh, logic_date
                )
                SELECT
                    trip_id, vehicle_id, trip_start_time, trip_end_time,
                    duration_seconds, is_circular, distance_meters, avg_speed_kmh, logic_date
                FROM {staging_table}
                ON CONFLICT (trip_start_time, vehicle_id, trip_id)
                DO NOTHING;
            """)
            execution_result = conn.execute(upsert_query)
            added_rows = execution_result.rowcount
            previously_saved_rows = len(trips_tuples) - added_rows
            conn.execute(text(f"ANALYZE {table_name};"))
            structured_logger.info(
                event="trips_saved",
                message="Trips saved",
                metadata={"added_rows": added_rows, "previously_saved_rows": previously_saved_rows},
            )
        return {
            "added_rows": added_rows,
            "previously_saved_rows": previously_saved_rows,
        }
    except Exception as e:
        error_message = (
            "Persistence failed while saving finished trips to database"
        )
        structured_logger.error(event="trips_save_failed", message=error_message)
        raise ValueError(error_message) from e
    finally:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {staging_table};"))
        except Exception:
            pass
        engine.dispose()

from datetime import datetime
from typing import Any, Callable, Dict

from sqlalchemy import create_engine, text

from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


def measure_persisted_facts(
    logic_date: datetime,
    config: Dict[str, Any],
    engine_factory: Callable = create_engine,
) -> Dict[str, Any]:
    def get_config(config):
        general = config["general"]
        database = config["connections"]["database"]
        trip_facts_table = general["tables"]["trip_facts_table_name"]
        dim_time_table = general["tables"]["dim_time_table_name"]
        avg_speed_kmh_max = general["quality"]["avg_speed_kmh_max"]
        db_uri = (
            f"postgresql://{database['user']}:{database['password']}"
            f"@{database['host']}:{database['port']}/{database['database']}"
        )
        return trip_facts_table, dim_time_table, avg_speed_kmh_max, db_uri

    trip_facts_table, dim_time_table, avg_speed_kmh_max, db_uri = get_config(config)
    engine = engine_factory(db_uri)
    try:
        with engine.begin() as conn:
            metrics_row = conn.execute(
                text(
                    f"SELECT "
                    f"  count(*) AS persisted_facts, "
                    f"  count(*) FILTER (WHERE duration_seconds < 0) AS negative_duration, "
                    f"  count(*) FILTER (WHERE distance_meters < 0) AS negative_distance, "
                    f"  count(*) FILTER (WHERE started_at > ended_at) AS time_incoherent, "
                    f"  count(*) FILTER (WHERE avg_speed_kmh NOT BETWEEN 0 AND :avg_speed_kmh_max) AS implausible_speed "
                    f"FROM {trip_facts_table} "
                    f"WHERE logic_date = :logic_date"
                ),
                {"logic_date": logic_date, "avg_speed_kmh_max": avg_speed_kmh_max},
            ).fetchone()
            coverage_row = conn.execute(
                text(
                    f"SELECT count(*) AS uncovered_dim_keys "
                    f"FROM {trip_facts_table} tf "
                    f"WHERE tf.logic_date = :logic_date "
                    f"  AND ( "
                    f"    NOT EXISTS (SELECT 1 FROM {dim_time_table} d WHERE d.time_key = tf.started_at_time_dim_key) "
                    f"    OR NOT EXISTS (SELECT 1 FROM {dim_time_table} d WHERE d.time_key = tf.ended_at_time_dim_key) "
                    f"  )"
                ),
                {"logic_date": logic_date},
            ).fetchone()
        return {
            "persisted_facts": int(metrics_row[0]),
            "negative_duration": int(metrics_row[1]),
            "negative_distance": int(metrics_row[2]),
            "time_incoherent": int(metrics_row[3]),
            "implausible_speed": int(metrics_row[4]),
            "uncovered_dim_keys": int(coverage_row[0]),
        }
    except Exception as e:
        structured_logger.error(
            event="persisted_facts_measurement_failed",
            message=(
                f"Failed to measure persisted facts for logic_date '{logic_date}' "
                f"from '{trip_facts_table}': {e}"
            ),
            metadata={
                "logic_date": str(logic_date),
                "trip_facts_table": trip_facts_table,
                "dim_time_table": dim_time_table,
            },
        )
        raise ValueError(
            f"Failed to measure persisted facts for logic_date '{logic_date}' "
            f"from '{trip_facts_table}': {e}"
        )
    finally:
        engine.dispose()

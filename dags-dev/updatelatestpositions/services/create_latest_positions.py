from datetime import datetime, timezone
from typing import Callable, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from infra.duck_db_v3 import get_duckdb_connection
from infra.sql_db_v2 import update_db_table_with_dataframe
from updatelatestpositions.services.get_latest_path_for_query import (
    get_latest_path_for_query,
)
from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _evaluate_freshness(
    config: dict,
    df: pd.DataFrame,
    now_fn: Optional[Callable[[], datetime]] = None,
) -> None:
    quality = config["general"]["quality"]
    warn_threshold = quality["freshness_warn_staleness_minutes"]
    fail_threshold = quality["freshness_fail_staleness_minutes"]

    if df.empty:
        result = {
            "observed_lag_minutes": None,
            "warn_threshold_minutes": warn_threshold,
            "fail_threshold_minutes": fail_threshold,
        }
        structured_logger.warning(
            event="freshness_evaluation",
            message="Freshness evaluation: no records to evaluate",
            metadata=result,
        )
        return result

    now_utc = (now_fn or _now_utc)()
    now_sp_naive = now_utc.astimezone(ZoneInfo("America/Sao_Paulo")).replace(tzinfo=None)
    latest_ts = df["veiculo_ts"].max()
    if hasattr(latest_ts, "tzinfo") and latest_ts.tzinfo is not None:
        latest_ts = latest_ts.tz_convert(ZoneInfo("America/Sao_Paulo")).replace(tzinfo=None)
    lag_minutes = round((now_sp_naive - latest_ts).total_seconds() / 60, 2)

    result = {
        "observed_lag_minutes": lag_minutes,
        "warn_threshold_minutes": warn_threshold,
        "fail_threshold_minutes": fail_threshold,
    }
    structured_logger.info(
        event="freshness_evaluation",
        message="Freshness evaluation",
        metadata=result,
    )
    return result


def create_latest_positions_table(
    config,
    get_path_fn=get_latest_path_for_query,
    duckdb_client=None,
    save_fn=update_db_table_with_dataframe,
    evaluate_freshness_fn=_evaluate_freshness,
):
    def get_config(config):
        general = config["general"]
        tables = general["tables"]
        database = config["connections"]["database"]
        latest_positions_table_name = tables["latest_positions_table_name"]
        storage_connection = {
            **config["connections"]["object_storage"],
            "secure": False,
        }
        database_connection = {
            "host": database["host"],
            "port": database["port"],
            "database": database["database"],
            "user": database["user"],
            "password": database["password"],
        }
        return latest_positions_table_name, storage_connection, database_connection

    latest_positions_table_name, storage_connection, database_connection = get_config(
        config
    )
    latest_path_for_query = get_path_fn(config)
    if not latest_path_for_query:
        structured_logger.warning(
            event="positions_update_skipped",
            message="No recent data found in the last 2 hours. Update skipped.",
        )
        return

    try:
        structured_logger.info(
            event="positions_query_started",
            message="Querying positions from parquet",
            metadata={"path": latest_path_for_query, "table": latest_positions_table_name},
        )
        con = duckdb_client or get_duckdb_connection(storage_connection)
        refined_df = con.execute(f"""
            SELECT veiculo_ts, veiculo_id, veiculo_lat, veiculo_long, linha_lt, linha_sentido,
                   linha_lt || '-' || (CASE WHEN linha_sentido = 1 THEN '0'
                                            WHEN linha_sentido = 2 THEN '1' ELSE NULL END) AS trip_id
            FROM read_parquet('{latest_path_for_query}')
        """).df()
        total_records = refined_df.shape[0]
        structured_logger.info(
            event="positions_query_succeeded",
            message="Positions query completed",
            metadata={"total_records": total_records},
        )
        evaluate_freshness_fn(config, refined_df)
        structured_logger.info(
            event="positions_save_started",
            message=f"Saving {total_records} records to table '{latest_positions_table_name}'",
            metadata={"table": latest_positions_table_name, "total_records": total_records},
        )
        save_fn(database_connection, refined_df, latest_positions_table_name)
        structured_logger.info(
            event="positions_save_succeeded",
            message=f"Table '{latest_positions_table_name}' updated successfully",
            status="SUCCEEDED",
            metadata={"table": latest_positions_table_name, "total_records": total_records},
        )

    except Exception as e:
        structured_logger.error(
            event="positions_update_failed",
            message=f"Update failed for table '{latest_positions_table_name}': {e}",
            error_type=type(e).__name__,
            error_message=str(e),
            metadata={"table": latest_positions_table_name},
        )
        raise ValueError(f"Update failed for {latest_positions_table_name}: {e}") from e
    finally:
        if "con" in locals():
            con.close()

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from infra.duck_db_v3 import get_duckdb_connection
from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


def get_recent_positions(config: Dict[str, Any], duckdb_client: Optional[Any] = None) -> pd.DataFrame:
    def get_config(config):
        general = config["general"]
        analysis = general["analysis"]
        storage = general["storage"]
        tables = general["tables"]
        hours_interval = int(analysis["hours_window"])
        bucket_name = storage["trusted_bucket"]
        app_folder = storage["app_folder"]
        positions_table_name = tables["positions_table_name"]
        connection = {
            **config["connections"]["object_storage"],
            "secure": False,
        }
        return (
            hours_interval,
            bucket_name,
            app_folder,
            positions_table_name,
            connection,
        )

    (
        hours_interval,
        bucket_name,
        app_folder,
        positions_table_name,
        connection,
    ) = get_config(config)
    now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/Sao_Paulo"))
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    current_hour = int(now.strftime("%H"))
    min_hour = current_hour - hours_interval
    if min_hour < 0:
        min_hour = 0
    s3_path = f"s3://{bucket_name}/{app_folder}/{positions_table_name}/year={year}/month={month}/day={day}/**"
    structured_logger.info(
        event="positions_query_started",
        message="Starting positions query",
        metadata={"hours_interval": hours_interval, "current_hour": current_hour, "min_hour": min_hour, "s3_path": s3_path},
    )
    try:
        con = duckdb_client or get_duckdb_connection(connection)
        # Optimized: Select only needed columns for trip detection
        # Sorted by linha_lt, veiculo_id first for index-based grouping, then veiculo_ts for chronological order
        sql = f"""
            SELECT
                veiculo_ts, linha_lt, veiculo_id, linha_sentido, is_circular, extracao_ts,
                veiculo_lat, veiculo_long,
                distance_to_first_stop, distance_to_last_stop,
                trip_linear_distance
            FROM read_parquet('{s3_path}', hive_partitioning = true)
            WHERE hour::INTEGER >= {min_hour} AND hour::INTEGER <= {current_hour}
            ORDER BY linha_lt, veiculo_id, veiculo_ts ASC;
        """
        df_recent_positions = con.execute(sql).df()
        total_records = df_recent_positions.shape[0]
        structured_logger.info(
            event="positions_query_completed",
            message="Positions query completed",
            metadata={"record_count": total_records},
        )
    except Exception as e:
        error_message = (
            "Data retrieval failed for recent positions query in object storage/duckdb"
        )
        structured_logger.error(event="positions_query_failed", message=error_message)
        raise ValueError(error_message) from e
    finally:
        if "con" in locals():
            con.close()
    return df_recent_positions

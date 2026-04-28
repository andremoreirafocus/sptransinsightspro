from infra.duck_db_v3 import get_duckdb_connection
from datetime import datetime, timezone

from zoneinfo import ZoneInfo
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def get_recent_positions(config, duckdb_client=None):
    def get_config(config):
        try:
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
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise ValueError(f"Missing required configuration key: {e}")

    (
        hours_interval,
        bucket_name,
        app_folder,
        positions_table_name,
        connection,
    ) = get_config(config)
    logger.info(
        f"Bulk loading last {hours_interval} hours of positions for all vehicles..."
    )
    now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/Sao_Paulo"))
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    current_hour = int(now.strftime("%H"))
    logger.info(f"Current hour: {current_hour}")
    min_hour = current_hour - hours_interval
    logger.info(f"Minimum hour: {min_hour}")
    if min_hour < 0:
        min_hour = 0
    s3_path = f"s3://{bucket_name}/{app_folder}/{positions_table_name}/year={year}/month={month}/day={day}/**"
    try:
        logger.info("Connecting to DuckDB...")
        con = duckdb_client or get_duckdb_connection(connection)
        logger.info(
            f"Retrieveing position records for the last {hours_interval} hours in {s3_path}..."
        )
        # Optimized: Select only needed columns for trip detection
        # Sorted by linha_lt, veiculo_id first for index-based grouping, then veiculo_ts for chronological order
        sql = f"""
            SELECT 
                veiculo_ts, linha_lt, veiculo_id, linha_sentido, is_circular
            FROM read_parquet('{s3_path}', hive_partitioning = true)
            WHERE hour::INTEGER >= {min_hour} AND hour::INTEGER <= {current_hour}
            ORDER BY linha_lt, veiculo_id, veiculo_ts ASC;
        """
        logger.info("Executing SQL query...")
        logger.info(f"SQL query: {sql}")
        df_recent_positions = con.execute(sql).df()
        total_records = df_recent_positions.shape[0]
        logger.info(f"Retrieved {total_records} position records.")
        if con:
            con.close()
            logger.info("DuckDB connection closed.")
    except Exception as e:
        logger.error(f"Data retrieval failed: {e}")
        raise ValueError(f"Data retrieval failed: {e}")
    finally:
        if "con" in locals():
            con.close()
    return df_recent_positions

from infra.duck_db import get_duckdb_connection
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def get_recent_positions(config):
    def get_config(config):
        try:
            hours_interval = int(config["ANALYSIS_HOURS_WINDOW"])
            bucket_name = config["TRUSTED_BUCKET"]
            app_folder = config["APP_FOLDER"]
            positions_table_name = config["POSITIONS_TABLE_NAME"]
            return hours_interval, bucket_name, app_folder, positions_table_name
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    hours_interval, bucket_name, app_folder, positions_table_name = get_config(config)
    logger.info(
        f"Bulk loading last {hours_interval} hours of positions for all vehicles..."
    )
    # now = datetime.now(timezone.utc)
    now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/Sao_Paulo"))

    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    current_hour = int(now.strftime("%H"))
    min_hour = current_hour - hours_interval
    if min_hour < 0:
        min_hour = 0
    s3_path = f"s3://{bucket_name}/{app_folder}/{positions_table_name}/year={year}/month={month}/day={day}/**"
    try:
        logger.info("Connecting to DuckDB...")
        con = get_duckdb_connection(config)
        logger.info(
            f"Retrieveing position records for the last {hours_interval} hours..."
        )
        # Optimized: Select only needed columns for trip detection
        sql = f"""
            SELECT 
                veiculo_ts, linha_lt, veiculo_id, linha_sentido, is_circular
            FROM read_parquet('{s3_path}', hive_partitioning = true)
            WHERE hour::INTEGER >= {min_hour} AND hour::INTEGER <= {current_hour}
            ORDER BY veiculo_ts ASC;
        """
        # Original full select (kept for reference):
        # sql = f"""
        #     SELECT
        #         veiculo_ts, linha_lt, veiculo_id, linha_sentido,
        #         distance_to_first_stop, distance_to_last_stop,
        #         is_circular, lt_origem, lt_destino
        #     FROM read_parquet('{s3_path}', hive_partitioning = true)
        #     WHERE hour::INTEGER >= {min_hour} AND hour::INTEGER <= {current_hour}
        #     --WHERE veiculo_ts >= NOW() - INTERVAL '3 hours'
        #     ORDER BY veiculo_ts ASC;
        # """
        df_recent_positions = con.execute(sql).df()
        total_records = df_recent_positions.shape[0]
        logger.info(f"Retrieved {total_records} position records.")
        if con:
            con.close()
            logger.info("DuckDB connection closed.")
    except Exception as e:
        logger.error(f"Data retrieval failed: {e}")
        raise
    finally:
        if "con" in locals():
            con.close()
    return df_recent_positions

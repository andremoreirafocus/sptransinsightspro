from refinedfinishedtrips.services.extract_trips_per_line_per_vehicle_pandas import (
    extract_trips_per_line_per_vehicle_pandas,
)

# from infra.db import fetch_data_from_db_as_df
from refinedfinishedtrips.services.save_trips_to_db import save_trips_to_db
from datetime import datetime, timedelta, timezone
import duckdb
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def extract_trips_for_all_Lines_and_vehicles_pandas(config):
    def get_config(config):
        # Using .get() with fallback or checking existence to avoid KeyErrors
        try:
            bucket_name = config["TRUSTED_BUCKET"]
            app_folder = config["APP_FOLDER"]
            positions_table_name = config["POSITIONS_TABLE_NAME"]
            connection_data = {
                "minio_endpoint": config["MINIO_ENDPOINT"],
                "access_key": config["ACCESS_KEY"],
                "secret_key": config["SECRET_KEY"],
                "secure": False,
            }
            return bucket_name, app_folder, positions_table_name, connection_data
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    bucket_name, app_folder, positions_table_name, connection_data = get_config(config)
    hours_interval = 3
    logger.info(
        "Bulk loading last {hours_interval} hours of positions for all vehicles..."
    )
    logger.info("Connecting to DuckDB...")
    con = duckdb.connect(database=":memory:")
    con.execute(f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_endpoint='{connection_data["minio_endpoint"]}'; 
        SET s3_access_key_id='{connection_data["access_key"]}';
        SET s3_secret_access_key='{connection_data["secret_key"]}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    now = datetime.now(timezone.utc)
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    current_hour = int(now.strftime("%H"))
    min_hour = current_hour - hours_interval
    s3_path = f"s3://{bucket_name}/{app_folder}/{positions_table_name}/year={year}/month={month}/day={day}/**"
    sql = f"""
        SELECT 
            veiculo_ts, linha_lt, veiculo_id, linha_sentido, 
            distance_to_first_stop, distance_to_last_stop, 
            is_circular, lt_origem, lt_destino
        FROM read_parquet('{s3_path}', hive_partitioning = true)
        WHERE hour::INTEGER >= {min_hour} AND hour::INTEGER <= {current_hour}
        --WHERE veiculo_ts >= NOW() - INTERVAL '3 hours'
        ORDER BY veiculo_ts ASC;
    """
    df_all_positions = con.execute(sql).df()
    if df_all_positions.empty:
        logger.warning("No position data found for the last 3 hours.")
        return
    grouped = df_all_positions.groupby(["linha_lt", "veiculo_id"])
    total_groups = len(grouped)
    logger.info(
        f"Processing {total_groups} unique line/vehicle combinations in memory."
    )
    num_processed = 0
    all_finished_trips = []
    for (linha_lt, veiculo_id), df_group in grouped:
        finished_trips = extract_trips_per_line_per_vehicle_pandas(
            linha_lt, veiculo_id, df_group
        )
        if finished_trips:
            for finished_trip in finished_trips:
                all_finished_trips.append(finished_trip)
        num_processed += 1
        if num_processed % 500 == 0:
            logger.info(f"Progress: {num_processed}/{total_groups} processed.")
    logger.info(f"Total finished trips: {len(all_finished_trips)}")
    save_trips_to_db(config, all_finished_trips)
    if con:
        con.close()
        logger.info("DuckDB connection closed.")

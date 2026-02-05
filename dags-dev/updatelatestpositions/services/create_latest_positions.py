import duckdb
import pandas as pd
import logging
from datetime import datetime, timedelta, timezone
from minio import Minio
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def get_latest_path(config):
    def get_config(config):
        try:
            bucket = config["TRUSTED_BUCKET"]
            app_folder = config["APP_FOLDER"]
            positions_table_name = config["POSITIONS_TABLE_NAME"]
            connection_data = {
                "minio_endpoint": config["MINIO_ENDPOINT"],
                "access_key": config["ACCESS_KEY"],
                "secret_key": config["SECRET_KEY"],
                "secure": False,
            }
            return (
                bucket,
                app_folder,
                positions_table_name,
                connection_data,
            )
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    (
        bucket,
        app_folder,
        positions_table_name,
        connection_data,
    ) = get_config(config)
    client = Minio(
        endpoint=connection_data["minio_endpoint"],
        access_key=connection_data["access_key"],
        secret_key=connection_data["secret_key"],
        secure=False,
    )
    latest_file_path = None
    now = datetime.now(timezone.utc)
    for i in range(6):  # Look back window
        check_time = now - timedelta(hours=i)
        prefix = f"{app_folder}/{positions_table_name}/{check_time.strftime('year=%Y/month=%m/day=%d/hour=%H')}/"
        print(f"prefix: {prefix}")
        # Non-recursive list of just this specific prefix
        objects = client.list_objects(bucket, prefix=prefix, recursive=True)
        # Get the highest filename in this specific hour
        found_files = sorted(
            [obj.object_name for obj in objects if obj.object_name.endswith(".parquet")]
        )
        if found_files:
            latest_file_path = f"s3://{bucket}/{found_files[-1]}"
            break
    print(latest_file_path)
    return latest_file_path


def create_latest_positions_table(config):
    """
    Refined Layer (Cost-Optimized):
    - No recursive listing.
    - Explicitly checks the current and previous hour prefixes.
    - If the pipeline was delayed, it will find the most recent processed data.
    """

    def get_config(config):
        try:
            latest_positions_table_name = config["LATEST_POSITIONS_TABLE_NAME"]
            connection_data = {
                "minio_endpoint": config["MINIO_ENDPOINT"],
                "access_key": config["ACCESS_KEY"],
                "secret_key": config["SECRET_KEY"],
                "secure": False,
            }
            return (
                latest_positions_table_name,
                connection_data,
            )
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    latest_positions_table_name, connection_data = get_config(config)
    try:
        latest_file_path = get_latest_path(config)
        if not latest_file_path:
            logger.error(
                "No recent data found in the last 6 hours. Cost-saving scan aborted."
            )
            return
        logger.info(f"Discovery successful: {latest_file_path}")
        # 2. DUCKDB: Direct read of the single identified file
        con = duckdb.connect(":memory:")
        con.execute(f"""
            INSTALL httpfs; LOAD httpfs;
            SET s3_endpoint='{connection_data["minio_endpoint"]}'; 
            SET s3_access_key_id='{connection_data["access_key"]}';
            SET s3_secret_access_key='{connection_data["secret_key"]}';
            SET s3_use_ssl=false; SET s3_url_style='path';
        """)
        refined_df = con.execute(f"""
            SELECT veiculo_ts, veiculo_id, veiculo_lat, veiculo_long, linha_lt, linha_sentido,
                   linha_lt || '-' || (CASE WHEN linha_sentido = 1 THEN '0' 
                                            WHEN linha_sentido = 2 THEN '1' ELSE NULL END) AS trip_id
            FROM read_parquet('{latest_file_path}')
        """).df()
        total_records = refined_df.shape[0]
        logger.info(f"Saving {total_records} at table {latest_positions_table_name}...")
        # 3. POSTGRES: Load
        db_uri = f"postgresql://{config['DB_USER']}:{config['DB_PASSWORD']}@{config['DB_HOST']}:{config['DB_PORT']}/{config['DB_DATABASE']}"
        engine = create_engine(db_uri)
        refined_df.to_sql(
            name=latest_positions_table_name,
            con=engine,
            schema="refined",
            if_exists="replace",
            index=False,
        )
    except Exception as e:
        logger.error(f"Update failed: {e}")
        raise

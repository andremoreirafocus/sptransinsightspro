from infra.duck_db import get_duckdb_connection
from airflow.dags.infra.db import save_dataframe_to_db
import logging
from datetime import datetime, timedelta, timezone
from minio import Minio

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
    max_hours = 3
    for i in range(max_hours):  # Look back window
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
    def get_config(config):
        try:
            latest_positions_table_name = config["LATEST_POSITIONS_TABLE_NAME"]
            return (latest_positions_table_name,)
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise

    latest_positions_table_name = get_config(config)
    try:
        latest_file_path = get_latest_path(config)
        if not latest_file_path:
            logger.error(
                "No recent data found in the last 6 hours. Cost-saving scan aborted."
            )
            return
        logger.info(f"Discovery successful: {latest_file_path}")
        con = get_duckdb_connection(config)
        refined_df = con.execute(f"""
            SELECT veiculo_ts, veiculo_id, veiculo_lat, veiculo_long, linha_lt, linha_sentido,
                   linha_lt || '-' || (CASE WHEN linha_sentido = 1 THEN '0' 
                                            WHEN linha_sentido = 2 THEN '1' ELSE NULL END) AS trip_id
            FROM read_parquet('{latest_file_path}')
        """).df()
        total_records = refined_df.shape[0]
        logger.info(f"Saving {total_records} at table {latest_positions_table_name}...")
        save_dataframe_to_db(config, refined_df)
    except Exception as e:
        logger.error(f"Update failed: {e}")
        raise
    finally:
        if con:
            con.close()

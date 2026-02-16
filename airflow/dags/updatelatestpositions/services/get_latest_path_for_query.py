from infra.minio_functions import list_objects_in_minio_bucket
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import logging

logger = logging.getLogger(__name__)


def get_latest_path_for_query(config):
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
    latest_path_for_query = None
    now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/Sao_Paulo"))
    for i in range(2):  # Look back window
        check_time = now - timedelta(hours=i)
        prefix = f"{app_folder}/{positions_table_name}/{check_time.strftime('year=%Y/month=%m/day=%d/hour=%H')}/"
        print(f"prefix: {prefix}")
        objects = list_objects_in_minio_bucket(
            connection_data,
            bucket,
            prefix,
        )
        found_files = sorted(
            [obj.object_name for obj in objects if obj.object_name.endswith(".parquet")]
        )
        if found_files:
            latest_path_for_query = f"s3://{bucket}/{found_files[-1]}"
            break
    logger.info(f"Latest path for query in bucket {bucket}: {latest_path_for_query}")
    return latest_path_for_query

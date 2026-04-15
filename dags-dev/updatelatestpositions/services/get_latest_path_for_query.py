from infra.object_storage import list_objects_in_object_storage_bucket
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import logging

logger = logging.getLogger(__name__)


def get_latest_path_for_query(config, list_objects_fn=list_objects_in_object_storage_bucket):
    def get_config(config):
        try:
            general = config["general"]
            storage = general["storage"]
            tables = general["tables"]
            bucket = storage["trusted_bucket"]
            app_folder = storage["app_folder"]
            positions_table_name = tables["positions_table_name"]
            connection_data = {
                **config["connections"]["object_storage"],
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
            raise ValueError(f"Missing required configuration key: {e}")

    (
        bucket,
        app_folder,
        positions_table_name,
        connection_data,
    ) = get_config(config)
    latest_path_for_query = None
    now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/Sao_Paulo"))
    for i in range(2):
        check_time = now - timedelta(hours=i)
        prefix = f"{app_folder}/{positions_table_name}/{check_time.strftime('year=%Y/month=%m/day=%d/hour=%H')}/"
        logger.info(f"Looking at prefix: {bucket}/{prefix}...")
        objects = list_objects_fn(
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

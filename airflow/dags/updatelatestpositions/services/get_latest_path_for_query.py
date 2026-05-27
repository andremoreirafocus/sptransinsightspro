from infra.object_storage import list_objects_in_object_storage_bucket
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


def get_latest_path_for_query(
    config, list_objects_fn=list_objects_in_object_storage_bucket
):
    def get_config(config):
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

    (
        bucket,
        app_folder,
        positions_table_name,
        connection_data,
    ) = get_config(config)
    structured_logger.info(
        event="path_discovery_started",
        message="Starting path discovery",
    )
    latest_path_for_query = None
    now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/Sao_Paulo"))
    for i in range(2):
        check_time = now - timedelta(hours=i)
        prefix = f"{app_folder}/{positions_table_name}/{check_time.strftime('year=%Y/month=%m/day=%d/hour=%H')}/"
        structured_logger.info(
            event="prefix_scan_started",
            message=f"Scanning prefix {bucket}/{prefix}",
            metadata={"bucket": bucket, "prefix": prefix},
        )
        try:
            objects = list_objects_fn(
                connection_data,
                bucket,
                prefix,
            )
        except Exception as e:
            structured_logger.error(
                event="path_discovery_failed",
                message=f"Object storage scan failed for bucket '{bucket}' prefix '{prefix}': {e}",
                error_type=type(e).__name__,
                error_message=str(e),
                metadata={"bucket": bucket, "prefix": prefix},
            )
            raise ValueError(
                f"Object storage scan failed for bucket '{bucket}' prefix '{prefix}': {e}"
            ) from e
        found_files = sorted(
            [obj.object_name for obj in objects if obj.object_name.endswith(".parquet")]
        )
        if found_files:
            latest_path_for_query = f"s3://{bucket}/{found_files[-1]}"
            break
    if not latest_path_for_query:
        structured_logger.warning(
            event="path_discovery_empty",
            message=f"No parquet files found in the last 2 hours for bucket '{bucket}'",
            metadata={"bucket": bucket},
        )
    else:
        structured_logger.info(
            event="path_discovery_succeeded",
            message=f"Latest path found in bucket '{bucket}'",
            metadata={"bucket": bucket, "path": latest_path_for_query},
        )
    return latest_path_for_query

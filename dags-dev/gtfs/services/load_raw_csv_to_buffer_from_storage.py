import io
from typing import Any, Callable, Dict

from infra.object_storage import read_file_from_object_storage_to_bytesio
from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


def load_raw_csv_to_buffer_from_storage(
    config: Dict[str, Any], file_name: str, read_fn: Callable[..., Any] = read_file_from_object_storage_to_bytesio
) -> io.BytesIO:
    """
    Load position data from source bucket and app folder.
    :param source_bucket: Source bucket name
    :param app_folder: Application folder path
    :return: Loaded data
    """

    def get_config(config):
        general = config["general"]
        storage = general["storage"]
        source_bucket = storage["raw_bucket"]
        app_folder = storage["gtfs_folder"]
        connection_data = {
            **config["connections"]["object_storage"],
            "secure": False,
        }
        return source_bucket, app_folder, connection_data

    source_bucket, app_folder, connection_data = get_config(config)
    object_name = f"{app_folder}/{file_name}.txt"
    structured_logger.info(
        event="csv_load_started",
        message="Loading raw CSV from storage",
        metadata={"object_name": object_name, "bucket": source_bucket},
    )
    try:
        data = read_fn(connection_data, source_bucket, object_name)
    except Exception as e:
        error_message = (
            "Failed to load raw csv buffer from object storage: "
            f"bucket='{source_bucket}', object='{object_name}'"
        )
        structured_logger.error(
            event="csv_load_failed",
            message=error_message,
            error_type=type(e).__name__,
            error_message=str(e),
            metadata={"bucket": source_bucket, "object_name": object_name},
        )
        raise ValueError(error_message) from e
    structured_logger.info(
        event="csv_load_succeeded",
        message="CSV loaded from storage",
        metadata={"bytes": data.getbuffer().nbytes, "object_name": object_name},
    )
    return data

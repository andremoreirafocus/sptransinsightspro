import logging
from typing import Any, Callable, Dict, Optional

from infra.object_storage import write_generic_bytes_to_object_storage

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def save_buffer_to_storage(
    config: Dict[str, Any],
    file_name: str,
    buffer: Any,
    subfolder: Optional[str] = None,
    write_fn: Callable[..., Any] = write_generic_bytes_to_object_storage,
) -> None:
    def get_config(config):
        general = config["general"]
        storage = general["storage"]
        destination_bucket = storage["trusted_bucket"]
        app_folder = storage["gtfs_folder"]
        connection_data = {
            **config["connections"]["object_storage"],
            "secure": False,
        }
        return destination_bucket, app_folder, connection_data

    destination_bucket, app_folder, connection_data = get_config(config)
    logger.info(
        f"Saving data to file {file_name} to bucket: {destination_bucket}, folder: {app_folder}"
    )
    if subfolder is None:
        prefix = f"{app_folder}/{file_name.split('.')[0]}"
        destination_object_name = f"{prefix}/{file_name}"
    else:
        normalized_subfolder = str(subfolder).strip("/")
        destination_object_name = f"{app_folder}/{normalized_subfolder}/{file_name}"
    try:
        write_fn(
            connection_data,
            buffer=buffer,
            bucket_name=destination_bucket,
            object_name=destination_object_name,
        )
        logger.info("Save data successful!")
    except Exception as e:
        error_message = (
            "Error writing data to object storage while saving parquet buffer: "
            f"bucket='{destination_bucket}', object='{destination_object_name}', "
            f"error_type='{type(e).__name__}', error='{e}'"
        )
        logger.error(error_message)
        raise ValueError(error_message) from e

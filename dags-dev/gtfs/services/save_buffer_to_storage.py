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
        try:
            general = config["general"]
            storage = general["storage"]
            destination_bucket = storage["trusted_bucket"]
            app_folder = storage["gtfs_folder"]
            connection_data = {
                **config["connections"]["object_storage"],
                "secure": False,
            }
            return destination_bucket, app_folder, connection_data
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise ValueError(f"Missing required configuration key: {e}")

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
        logger.error(
            "Error writing data to MinIO bucket "
            f"'{destination_bucket}' with object name '{destination_object_name}'."
        )
        logger.error(f"Exception details: {e}")
        raise ValueError(
            "Failed to save parquet buffer to object storage: "
            f"bucket='{destination_bucket}', object='{destination_object_name}', error='{e}'"
        )

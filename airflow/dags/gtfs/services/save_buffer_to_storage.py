from typing import Any, Callable, Dict, Optional

from infra.object_storage import write_generic_bytes_to_object_storage
from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


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
    if subfolder is None:
        prefix = f"{app_folder}/{file_name.split('.')[0]}"
        destination_object_name = f"{prefix}/{file_name}"
    else:
        normalized_subfolder = str(subfolder).strip("/")
        destination_object_name = f"{app_folder}/{normalized_subfolder}/{file_name}"
    structured_logger.info(
        event="buffer_save_started",
        message="Saving buffer to storage",
        metadata={"file_name": file_name, "destination": destination_object_name},
    )
    try:
        bytes_written = len(buffer.getvalue()) if hasattr(buffer, "getvalue") else len(buffer)
        write_fn(
            connection_data,
            buffer=buffer,
            bucket_name=destination_bucket,
            object_name=destination_object_name,
        )
        structured_logger.info(
            event="buffer_save_succeeded",
            message="Buffer saved to storage",
            status="SUCCEEDED",
            metadata={"destination": destination_object_name, "bytes_written": bytes_written},
        )
    except Exception as e:
        error_message = (
            "Error writing data to object storage while saving parquet buffer: "
            f"bucket='{destination_bucket}', object='{destination_object_name}', "
            f"error_type='{type(e).__name__}', error='{e}'"
        )
        structured_logger.error(
            event="buffer_save_failed",
            message=error_message,
            error_type=type(e).__name__,
            error_message=str(e),
        )
        raise ValueError(error_message) from e

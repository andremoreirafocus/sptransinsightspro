from infra.object_storage import (
    read_file_from_object_storage_to_str,
    read_file_from_object_storage_to_bytesio,
)
from infra.compression import decompress_data
from observability.structured_event_logger import get_structured_logger
import json
from typing import Any, Callable, Dict, Tuple

structured_logger = get_structured_logger(
    service="transformlivedata",
    component="load_positions",
    logger_name=__name__,
)


def load_positions(
    config: Dict[str, Any],
    partition_path: str,
    source_file: str,
    read_bytes_fn: Callable[..., Any] = read_file_from_object_storage_to_bytesio,
    read_str_fn: Callable[..., Any] = read_file_from_object_storage_to_str,
    decompress_fn: Callable[..., Any] = decompress_data,
) -> Dict[str, Any]:
    def get_config(
        config: Dict[str, Any],
    ) -> Tuple[str, str, bool, str, Dict[str, Any]]:
        general = config["general"]
        connections = config["connections"]
        storage = general["storage"]
        compression = general["compression"]
        source_bucket = storage["raw_bucket"]
        app_folder = storage["app_folder"]
        raw_data_compression = bool(compression["raw_data_compression"])
        compression_extension = compression["raw_data_compression_extension"]
        connection_data = {
            **connections["object_storage"],
            "secure": False,
        }
        return (
            source_bucket,
            app_folder,
            raw_data_compression,
            compression_extension,
            connection_data,
        )

    (
        source_bucket,
        app_folder,
        raw_data_compression,
        compression_extension,
        connection_data,
    ) = get_config(config)
    prefix = f"{app_folder}/{partition_path}"
    object_name = f"{prefix}{source_file}"
    if raw_data_compression:
        object_name += compression_extension
    structured_logger.info(
        event="load_positions_storage_read_started",
        message="Loading positions payload from object storage",
        status="STARTED",
        metadata={
            "object_name": object_name,
            "source_bucket": source_bucket,
            "app_folder": app_folder,
            "compressed": raw_data_compression,
        },
    )
    if raw_data_compression:
        try:
            data = read_bytes_fn(connection_data, source_bucket, object_name)
            structured_logger.info(
                event="load_positions_decompression_started",
                message="Starting compressed payload decompression",
                status="STARTED",
                metadata={"object_name": object_name},
            )
        except Exception as e:
            structured_logger.error(
                event="load_positions_storage_read_failed",
                message="Failed to read object storage payload",
                status="FAILED",
                error_type=type(e).__name__,
                error_message=str(e),
                metadata={"object_name": object_name},
            )
            raise ValueError(
                f"Failed to read object storage payload '{object_name}': {e}"
            ) from e
        try:
            datastr = decompress_fn(data.getvalue())
            structured_logger.info(
                event="load_positions_decompression_succeeded",
                message="Compressed payload decompression succeeded",
                status="SUCCEEDED",
                metadata={"object_name": object_name},
            )
        except Exception as e:
            structured_logger.error(
                event="load_positions_decompression_failed",
                message="Failed to decompress object storage payload",
                status="FAILED",
                error_type=type(e).__name__,
                error_message=str(e),
                metadata={"object_name": object_name},
            )
            raise ValueError(
                f"Failed to decompress object storage payload '{object_name}': {e}"
            ) from e
    else:
        try:
            datastr = read_str_fn(connection_data, source_bucket, object_name)
        except Exception as e:
            structured_logger.error(
                event="load_positions_storage_read_failed",
                message="Failed to read object storage payload",
                status="FAILED",
                error_type=type(e).__name__,
                error_message=str(e),
                metadata={"object_name": object_name},
            )
            raise ValueError(
                f"Failed to read object storage payload '{object_name}': {e}"
            ) from e
    structured_logger.info(
        event="load_positions_storage_read_succeeded",
        message="Positions payload loaded from object storage",
        status="SUCCEEDED",
        metadata={"object_name": object_name, "bytes_loaded": len(datastr)},
    )
    try:
        data = json.loads(datastr)
    except Exception as e:
        structured_logger.error(
            event="load_positions_json_parse_failed",
            message="Invalid JSON payload",
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
            metadata={"object_name": object_name},
        )
        raise ValueError(f"Invalid JSON payload for '{object_name}': {e}") from e
    structured_logger.info(
        event="load_positions_json_parse_succeeded",
        message="JSON payload parsed successfully",
        status="SUCCEEDED",
        metadata={"object_name": object_name},
    )
    return data

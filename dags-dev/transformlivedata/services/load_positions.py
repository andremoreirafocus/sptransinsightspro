from infra.object_storage import (
    read_file_from_object_storage_to_str,
    read_file_from_object_storage_to_bytesio,
)
from infra.compression import decompress_data
import json
import logging
from typing import Any, Callable, Dict, Tuple

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


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
    logger.info(
        f"Loading position data {object_name} from bucket: {source_bucket}, folder: {app_folder}"
    )
    if raw_data_compression:
        try:
            data = read_bytes_fn(connection_data, source_bucket, object_name)
            logger.info("Data is compressed, decompressing...")
            datastr = decompress_fn(data.getvalue())
            logger.info("Data decompressed successfully.")
        except Exception as e:
            logger.error(
                "Failed to read/decompress object storage payload '%s': %s",
                object_name,
                e,
            )
            raise ValueError(
                f"Failed to read/decompress object storage payload '{object_name}': {e}"
            ) from e
    else:
        try:
            datastr = read_str_fn(connection_data, source_bucket, object_name)
        except Exception as e:
            logger.error(
                "Failed to read object storage payload '%s': %s",
                object_name,
                e,
            )
            raise ValueError(
                f"Failed to read object storage payload '{object_name}': {e}"
            ) from e
    logger.info(f"Loaded {len(datastr)} bytes from {object_name}")
    try:
        data = json.loads(datastr)
    except Exception as e:
        logger.error("Invalid JSON payload for '%s': %s", object_name, e)
        raise ValueError(f"Invalid JSON payload for '{object_name}': {e}") from e
    return data

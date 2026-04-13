from infra.object_storage import (
    read_file_from_object_storage_to_str,
    read_file_from_object_storage_to_bytesio,
)
from infra.compression import decompress_data
import json
import logging
from typing import Any, Dict, Tuple

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_positions(
    config: Dict[str, Any], partition_path: str, source_file: str
) -> Dict[str, Any]:
    def get_config(
        config: Dict[str, Any]
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
    try:
        prefix = f"{app_folder}/{partition_path}"
        object_name = f"{prefix}{source_file}"
    except Exception as e:
        logger.error("Error building object_name: %s", e)
        raise ValueError(f"Error building object_name: {e}")
    if raw_data_compression:
        object_name += compression_extension
    logger.info(
        f"Loading position data {object_name} from bucket: {source_bucket}, folder: {app_folder}"
    )
    if raw_data_compression:
        data = read_file_from_object_storage_to_bytesio(
            connection_data, source_bucket, object_name
        )
        logger.info("Data is compressed, decompressing...")
        datastr = decompress_data(data.getvalue())
        logger.info("Data decompressed successfully.")
    else:
        datastr = read_file_from_object_storage_to_str(
            connection_data, source_bucket, object_name
        )
    logger.info(f"Loaded {len(datastr)} bytes from {object_name}")
    data = json.loads(datastr)
    return data

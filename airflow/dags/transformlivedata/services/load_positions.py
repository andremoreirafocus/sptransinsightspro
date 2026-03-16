from infra.minio_functions import (
    read_file_from_minio_to_str,
    read_file_from_minio_to_BytesIO,
)
from infra.compression import decompress_data
import json
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_positions(config, partition_path, source_file):
    def get_config(config):
        storage = config["storage"]
        compression = config["compression"]
        source_bucket = storage["raw_bucket"]
        app_folder = storage["app_folder"]
        raw_data_compression = bool(compression["raw_data_compression"])
        compression_extension = compression["raw_data_compression_extension"]
        connection_data = {
            "minio_endpoint": storage["minio_endpoint"],
            "access_key": storage["access_key"],
            "secret_key": storage["secret_key"],
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
        raise
    if raw_data_compression:
        object_name += compression_extension
    # print(f"Config: {config}")
    # print(f"Connection data: {connection_data}")
    logger.info(
        f"Loading position data {object_name} from bucket: {source_bucket}, folder: {app_folder}"
    )
    if raw_data_compression:
        data = read_file_from_minio_to_BytesIO(
            connection_data, source_bucket, object_name
        )
        # print(datastr)
        logger.info("Data is compressed, decompressing...")
        datastr = decompress_data(data.getvalue())
        # print(f"Decompressed data {datastr}")
        logger.info("Data decompressed successfully.")
    else:
        datastr = read_file_from_minio_to_str(
            connection_data, source_bucket, object_name
        )
    logger.info(f"Loaded {len(datastr)} bytes from {object_name}")
    # logger.info(data)
    data = json.loads(datastr)
    return data

from infra.minio_functions import (
    read_file_from_minio_to_str,
    read_file_from_minio_to_BytesIO,
)
from infra.compression import decompress_data
import json
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_positions(config, year, month, day, hour, minute):
    def get_config(config):
        source_bucket = config["RAW_BUCKET"]
        app_folder = config["APP_FOLDER"]
        raw_data_compression = config["RAW_DATA_COMPRESSION"] == "true"
        compression_extension = config.get("RAW_DATA_COMPRESSION_EXTENSION", "")
        connection_data = {
            "minio_endpoint": config["MINIO_ENDPOINT"],
            "access_key": config["ACCESS_KEY"],
            "secret_key": config["SECRET_KEY"],
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
    hour_minute = f"{hour}{minute}"
    prefix = f"{app_folder}/year={year}/month={month}/day={day}/"
    base_file_name = "posicoes_onibus"
    object_name = f"{prefix}{base_file_name}-{year}{month}{day}{hour_minute}.json"
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
        print(f"Decompressed data {datastr}")
        logger.info("Data decompressed successfully.")
    else:
        datastr = read_file_from_minio_to_str(
            connection_data, source_bucket, object_name
        )
    logger.info(f"Loaded {len(datastr)} bytes from {object_name}")
    # logger.info(data)
    data = json.loads(datastr)
    return data

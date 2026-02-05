from transformdatatotrusted.infra.minio_functions import read_file_from_minio
import json
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_positions(config, year, month, day, hour, minute):
    def get_config(config):
        source_bucket = config["SOURCE_BUCKET"]
        app_folder = config["APP_FOLDER"]
        connection_data = {
            "minio_endpoint": config["MINIO_ENDPOINT"],
            "access_key": config["ACCESS_KEY"],
            "secret_key": config["SECRET_KEY"],
            "secure": False,
        }
        return source_bucket, app_folder, connection_data

    source_bucket, app_folder, connection_data = get_config(config)
    hour_minute = f"{hour}{minute}"
    prefix = f"{app_folder}/year={year}/month={month}/day={day}/"
    base_file_name = "posicoes_onibus"
    object_name = f"{prefix}{base_file_name}-{year}{month}{day}{hour_minute}.json"
    # print(f"Config: {config}")
    # print(f"Connection data: {connection_data}")
    logger.info(
        f"Loading position data from bucket: {source_bucket}, folder: {app_folder}"
    )
    datastr = read_file_from_minio(connection_data, source_bucket, object_name)
    logger.info(f"Loaded {len(datastr)} bytes from {object_name}")
    # logger.info(data)
    data = json.loads(datastr)
    return data

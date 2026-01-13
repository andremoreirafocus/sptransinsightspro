from src.infra.minio_functions import read_file_from_minio
import json
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_positions(config, source_bucket, app_folder, year, month, day, hour, minute):
    """
    Load position data from source bucket and app folder.
    :param source_bucket: Source bucket name
    :param app_folder: Application folder path
    :return: Loaded data
    """
    logger.info(
        f"Loading position data from bucket: {source_bucket}, folder: {app_folder}"
    )

    # Add your logic to load position data here
    # Example: read files from MinIO, parse them, and return as a list of records
    hour_minute = f"{hour}{minute}"
    prefix = f"{app_folder}/year={year}/month={month}/day={day}/"
    base_file_name = "posicoes_onibus"
    object_name = f"{prefix}{base_file_name}-{year}{month}{day}{hour_minute}.json"
    print(f"Config: {config}")
    connection_data = {
        "minio_endpoint": config.get("MINIO_ENDPOINT"),
        "access_key": config.get("ACCESS_KEY"),
        "secret_key": config.get("SECRET_KEY"),
        "secure": False,
    }
    print(f"Connection data: {connection_data}")
    datastr = read_file_from_minio(connection_data, source_bucket, object_name)
    logger.info(f"Loaded {len(datastr)} bytes from {object_name}")
    # logger.info(data)
    data = json.loads(datastr)
    return data

from src.infra.minio_functions import read_file_from_minio
from src.infra.get_minio_connection_data import get_minio_connection_data
import json
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_positions(source_bucket, app_folder, year, month, day, hour, minute):
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
    connection_data = get_minio_connection_data()
    object_name = f"{prefix}{base_file_name}-{year}{month}{day}{hour_minute}.json"
    datastr = read_file_from_minio(connection_data, source_bucket, object_name)
    logger.info(f"Loaded {len(datastr)} bytes from {object_name}")
    # logger.info(data)
    data = json.loads(datastr)
    return data

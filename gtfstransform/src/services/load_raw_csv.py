from src.infra.get_minio_connection_data import get_minio_connection_data
from src.infra.minio_functions import read_file_from_minio_to_BytesIO

import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_raw_csv(source_bucket, app_folder, file_name):
    """
    Load position data from source bucket and app folder.
    :param source_bucket: Source bucket name
    :param app_folder: Application folder path
    :return: Loaded data
    """
    logger.info(
        f"Loading {file_name} csv data from bucket: {source_bucket}, folder: {app_folder}"
    )
    # Add your logic to load position data here
    # Example: read files from MinIO, parse them, and return as a list of records
    prefix = f"{app_folder}/"
    connection_data = get_minio_connection_data()
    object_name = f"{prefix}{file_name}/{file_name}.txt"
    print(f"Reading object: {object_name} from bucket: {source_bucket} ...")
    data = read_file_from_minio_to_BytesIO(connection_data, source_bucket, object_name)
    logger.info(f"Loaded {data.getbuffer().nbytes} bytes from {object_name}")
    # logger.info(data)
    # print(data)
    return data

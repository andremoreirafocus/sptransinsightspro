from infra.minio_functions import read_file_from_minio_to_BytesIO

import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_raw_csv_to_buffer_from_storage(config, file_name):
    """
    Load position data from source bucket and app folder.
    :param source_bucket: Source bucket name
    :param app_folder: Application folder path
    :return: Loaded data
    """

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
    logger.info(
        f"Loading {file_name} csv data from bucket: {source_bucket}, folder: {app_folder}"
    )
    prefix = f"{app_folder}/"
    object_name = f"{prefix}{file_name}/{file_name}.txt"
    print(f"Reading object: {object_name} from bucket: {source_bucket} ...")
    data = read_file_from_minio_to_BytesIO(connection_data, source_bucket, object_name)
    logger.info(f"Loaded {data.getbuffer().nbytes} bytes from {object_name}")
    return data

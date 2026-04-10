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
        try:
            general = config["general"]
            storage = general["storage"]
            source_bucket = storage["raw_bucket"]
            app_folder = storage["gtfs_folder"]
            connection_data = {
                "minio_endpoint": storage["minio_endpoint"],
                "access_key": storage["access_key"],
                "secret_key": storage["secret_key"],
                "secure": False,
            }
            return source_bucket, app_folder, connection_data
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise ValueError(f"Missing required configuration key: {e}")

    source_bucket, app_folder, connection_data = get_config(config)
    logger.info(
        f"Loading {file_name} csv data from bucket: {source_bucket}, folder: {app_folder}"
    )
    prefix = f"{app_folder}/"
    object_name = f"{prefix}{file_name}/{file_name}.txt"
    logger.info(f"Reading object: {object_name} from bucket: {source_bucket} ...")
    data = read_file_from_minio_to_BytesIO(connection_data, source_bucket, object_name)
    logger.info(f"Loaded {data.getbuffer().nbytes} bytes from {object_name}")
    return data

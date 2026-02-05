from infra.minio_functions import write_generic_bytes_to_minio
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def save_buffer_to_storage(config, file_name, buffer):
    def get_config(config):
        destination_bucket = config["DESTINATION_BUCKET"]
        app_folder = config["APP_FOLDER"]
        connection_data = {
            "minio_endpoint": config["MINIO_ENDPOINT"],
            "access_key": config["ACCESS_KEY"],
            "secret_key": config["SECRET_KEY"],
            "secure": False,
        }
        return destination_bucket, app_folder, connection_data

    destination_bucket, app_folder, connection_data = get_config(config)
    logger.info(
        f"Saving data to file {file_name} to bucket: {destination_bucket}, folder: {app_folder}"
    )
    prefix = f"{app_folder}/{file_name.split('.')[0]}"
    destination_object_name = f"{prefix}/{file_name}"
    try:
        write_generic_bytes_to_minio(
            connection_data,
            buffer=buffer,
            bucket_name=destination_bucket,
            object_name=destination_object_name,
        )
    except Exception as e:
        logger.error(
            "Error writing data to MinIO bucket "
            f"'{destination_bucket}' with object name '{destination_object_name}'."
        )
        logger.error(f"Exception details: {e}")
    logger.info("Save data successful!")

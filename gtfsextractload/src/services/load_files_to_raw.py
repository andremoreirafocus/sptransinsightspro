from src.infra.minio_functions import write_generic_bytes_to_minio
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def load_files_to_raw(config, files_list):
    def get_config(config):
        folder = config.get("LOCAL_DOWNLOADS_FOLDER")
        bucket_name = config.get("RAW_BUCKET_NAME")
        app_folder = config.get("APP_FOLDER")
        connection_data = {
            "minio_endpoint": config["MINIO_ENDPOINT"],
            "access_key": config["ACCESS_KEY"],
            "secret_key": config["SECRET_KEY"],
            "secure": False,
        }
        return folder, bucket_name, app_folder, connection_data

    folder, bucket_name, app_folder, connection_data = get_config(config)
    for file_name in files_list:
        local_file_path = f"{folder}/{file_name}"
        logger.info(f"Reading file: {local_file_path} ...")
        with open(local_file_path, "rb") as f:
            data = f.read()
        file_name_no_ext = file_name.split(".")[0]
        prefix = f"{app_folder}/"
        destination_object_name = f"{prefix}{file_name_no_ext}/{file_name}"
        logger.info(
            f"Writing file: {local_file_path} to {bucket_name}/{destination_object_name}..."
        )
        write_generic_bytes_to_minio(
            connection_data,
            buffer=data,
            bucket_name=bucket_name,
            object_name=destination_object_name,
        )
        logger.info("Done.")

from infra.object_storage import write_generic_bytes_to_object_storage
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def save_files_to_raw_storage(
    config,
    files_list,
    read_file_fn=open,
    write_fn=write_generic_bytes_to_object_storage,
):
    def get_config(config):
        try:
            general = config["general"]
            extraction = general["extraction"]
            storage = general["storage"]
            folder = extraction["local_downloads_folder"]
            bucket_name = storage["raw_bucket"]
            app_folder = storage["gtfs_folder"]
            connection_data = {
                **config["connections"]["object_storage"],
                "secure": False,
            }
            return folder, bucket_name, app_folder, connection_data
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise ValueError(f"Missing required configuration key: {e}")

    folder, bucket_name, app_folder, connection_data = get_config(config)
    for file_name in files_list:
        local_file_path = f"{folder}/{file_name}"
        logger.info(f"Reading file: {local_file_path} ...")
        with read_file_fn(local_file_path, "rb") as f:
            data = f.read()
        file_name_no_ext = file_name.split(".")[0]
        prefix = f"{app_folder}/"
        destination_object_name = f"{prefix}{file_name_no_ext}/{file_name}"
        logger.info(
            f"Writing file: {local_file_path} to {bucket_name}/{destination_object_name}..."
        )
        write_fn(
            connection_data,
            buffer=data,
            bucket_name=bucket_name,
            object_name=destination_object_name,
        )
        logger.info("Done.")

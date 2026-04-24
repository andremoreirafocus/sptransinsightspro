from src.infra.compression import compress_data
import os

import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def save_data_to_json_file(
    data,
    downloads_folder,
    file_name,
    compression=False,
    open_fn=None,
    makedirs_fn=None,
):
    open_fn = open_fn or open
    makedirs_fn = makedirs_fn or os.makedirs
    try:
        if compression:
            logger.info("Compressing data..")
            mode = "wb"
            data, file_name_extension = compress_data(data)
            file_name += file_name_extension
            logger.info("Data compressed successfully.")
        else:
            mode = "w"
        makedirs_fn(downloads_folder, exist_ok=True)
        file_with_path = os.path.join(downloads_folder, file_name)
        logger.info(f"Writing buses_positions to file {file_with_path} ...")
        with open_fn(file_with_path, mode) as file:
            file.write(data)
        logger.info("File saved successfully!!!")
        return True
    except Exception as e:
        logger.error(f"Failed to save file: {e}")
        raise

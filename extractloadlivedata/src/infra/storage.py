import os
import json
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def save_data_to_json_file(json_data, downloads_folder, file_name):
    from src.infra.wsl_functions import is_disk_space_ok_wsl, is_wsl
    
    # Check disk space first, before any file operations
    if is_wsl():
        if not is_disk_space_ok_wsl():
            logger.error("Disk space critical: skipping execution")
            return False
    
    try:
        os.makedirs(downloads_folder, exist_ok=True)
        file_with_path = os.path.join(downloads_folder, file_name)
        logger.info(f"Writing buses_positions to file {file_with_path} ...")
        with open(file_with_path, "w") as file:
            file.write(json.dumps(json_data))
        logger.info("File saved successfully!!!")
        return True
    except Exception as e:
        logger.error(f"Failed to save file: {e}")
        return False

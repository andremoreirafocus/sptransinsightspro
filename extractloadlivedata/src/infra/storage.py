from src.infra.wsl_functions import is_disk_space_ok_wsl, is_wsl
import os
import zstandard as zstd
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def save_data_to_json_file(data, downloads_folder, file_name, compression=False):
    if is_wsl():
        if not is_disk_space_ok_wsl():
            logger.error("Disk space critical: skipping execution")
            return False

    try:
        if compression:
            logger.info("Compressing data with zstd...")
            cctx = zstd.ZstdCompressor(level=3)
            data = cctx.compress(data.encode("utf-8"))
            logger.info("Data compressed successfully.")
            mode = "wb"
            file_name += ".zst"
        else:
            mode = "w"
        os.makedirs(downloads_folder, exist_ok=True)
        file_with_path = os.path.join(downloads_folder, file_name)
        logger.info(f"Writing buses_positions to file {file_with_path} ...")
        with open(file_with_path, mode) as file:
            file.write(data)
        logger.info("File saved successfully!!!")
        return True
    except Exception as e:
        logger.error(f"Failed to save file: {e}")
        return False

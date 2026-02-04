import requests
import zipfile
import io
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def extract_gtfs_files(config):
    def get_config(config):
        url = config["GTFS_URL"]
        login = config["LOGIN"]
        password = config["PASSWORD"]
        downloads_folder = config["LOCAL_DOWNLOADS_FOLDER"]
        return url, login, password, downloads_folder

    url, login, password, downloads_folder = get_config(config)
    response = requests.get(url, auth=(login, password))
    if response.status_code == 404:
        logger.error("Check credentials or portal access")
        return
    response.raise_for_status()
    files_list = []
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        files_list = z.namelist()
        logger.info(files_list)  # List files: agency.txt, stops.txt, etc.
        z.extractall(downloads_folder)
    logger.info(f"GTFS files extracted to {downloads_folder}")
    return files_list

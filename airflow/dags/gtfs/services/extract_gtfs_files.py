import io
import logging
import zipfile
from pathlib import Path
from typing import Any, Callable, Dict, List

import requests

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def _is_safe_zip_member(base_dir: Path, member_name: str) -> bool:
    member_path = Path(member_name)
    if member_path.is_absolute():
        return False
    target_path = (base_dir / member_path).resolve()
    try:
        target_path.relative_to(base_dir)
    except ValueError:
        return False
    return True


def extract_gtfs_files(config: Dict[str, Any], http_get_fn: Callable[..., Any] = requests.get) -> List[str]:
    def get_config(config):
        try:
            general = config["general"]
            extraction = general["extraction"]
            connection = config["connections"]["http"]
            url = f"{connection['conn_type']}://{connection['host']}{connection['schema']}"
            login = connection["login"]
            password = connection["password"]
            downloads_folder = extraction["local_downloads_folder"]
            return url, login, password, downloads_folder
        except KeyError as e:
            logger.error(f"Missing required configuration key: {e}")
            raise ValueError(f"Missing required configuration key: {e}")

    url, login, password, downloads_folder = get_config(config)
    response = http_get_fn(url, auth=(login, password))
    if response.status_code == 404:
        error_msg = "GTFS endpoint returned 404: check credentials or portal access."
        logger.error(error_msg)
        raise ValueError(error_msg)
    response.raise_for_status()
    files_list = []
    base_dir = Path(downloads_folder).resolve()
    base_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        files_list = z.namelist()
        logger.info(files_list)  # List files: agency.txt, stops.txt, etc.
        for member in z.infolist():
            if not _is_safe_zip_member(base_dir, member.filename):
                raise ValueError(f"Unsafe zip entry detected: {member.filename}")
            z.extract(member, path=base_dir)
    logger.info(f"GTFS files extracted to {downloads_folder}")
    return files_list

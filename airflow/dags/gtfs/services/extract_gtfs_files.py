import io
import zipfile
from pathlib import Path
from typing import Any, Callable, Dict, List

import requests

from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


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
        general = config["general"]
        extraction = general["extraction"]
        connection = config["connections"]["http"]
        url = f"{connection['conn_type']}://{connection['host']}{connection['schema']}"
        login = connection["login"]
        password = connection["password"]
        downloads_folder = extraction["local_downloads_folder"]
        return url, login, password, downloads_folder

    url, login, password, downloads_folder = get_config(config)
    response = http_get_fn(url, auth=(login, password))
    if response.status_code == 404:
        error_msg = "GTFS endpoint returned 404: check credentials or portal access."
        structured_logger.error(
            event="gtfs_extraction_failed",
            message=error_msg,
            metadata={"status_code": 404},
        )
        raise ValueError(error_msg)
    response.raise_for_status()
    files_list = []
    base_dir = Path(downloads_folder).resolve()
    base_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        files_list = z.namelist()
        structured_logger.info(
            event="gtfs_extraction_started",
            message="GTFS zip downloaded",
            metadata={"files": files_list, "url": url},
        )
        for member in z.infolist():
            if not _is_safe_zip_member(base_dir, member.filename):
                structured_logger.error(
                    event="gtfs_extraction_failed",
                    message=f"Unsafe zip entry detected: {member.filename}",
                    metadata={"entry": member.filename},
                )
                raise ValueError(f"Unsafe zip entry detected: {member.filename}")
            z.extract(member, path=base_dir)
    structured_logger.info(
        event="gtfs_extraction_succeeded",
        message="GTFS files extracted",
        metadata={"downloads_folder": downloads_folder, "file_count": len(files_list)},
    )
    return files_list

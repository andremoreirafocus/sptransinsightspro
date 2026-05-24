from typing import Any, Callable, Dict, List, Optional

from infra.object_storage import write_generic_bytes_to_object_storage
from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


def save_files_to_raw_storage(
    config: Dict[str, Any],
    files_list: Optional[List[str]],
    failed: bool = False,
    read_file_fn: Callable[..., Any] = open,
    write_fn: Callable[..., Any] = write_generic_bytes_to_object_storage,
) -> None:
    def get_config(config):
        general = config["general"]
        extraction = general["extraction"]
        storage = general["storage"]
        folder = extraction["local_downloads_folder"]
        bucket_name = storage["raw_bucket"]
        app_folder = storage["gtfs_folder"]
        quarantined_subfolder = storage["quarantined_subfolder"]
        connection_data = {
            **config["connections"]["object_storage"],
            "secure": False,
        }
        return (
            folder,
            bucket_name,
            app_folder,
            quarantined_subfolder.strip("/"),
            connection_data,
        )

    (
        folder,
        bucket_name,
        app_folder,
        quarantined_subfolder,
        connection_data,
    ) = get_config(config)
    structured_logger.info(
        event="raw_files_upload_started",
        message="Uploading raw GTFS files to storage",
        metadata={"file_count": len(files_list or []), "target": "quarantine" if failed else "raw"},
    )
    for file_name in files_list or []:
        local_file_path = f"{folder}/{file_name}"
        with read_file_fn(local_file_path, "rb") as f:
            data = f.read()
        if failed:
            object_name = f"{app_folder}/{quarantined_subfolder}/{file_name}"
        else:
            object_name = f"{app_folder}/{file_name}"
        write_fn(
            connection_data,
            buffer=data,
            bucket_name=bucket_name,
            object_name=object_name,
        )
    structured_logger.info(
        event="raw_files_upload_succeeded",
        message="Raw GTFS files uploaded to storage",
        metadata={"file_count": len(files_list or []), "target": "quarantine" if failed else "raw"},
    )

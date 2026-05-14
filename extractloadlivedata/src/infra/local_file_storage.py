from src.infra.compression import compress_data
import os
from typing import Any, Callable, Optional
from src.infra.structured_logging import get_structured_logger
from src.domain.events import EVENT_STATUS_FAILED, EVENT_STATUS_STARTED, EVENT_STATUS_SUCCEEDED

structured_logger = get_structured_logger(
    service="extractloadlivedata",
    component="local_file_storage",
    logger_name=__name__,)


def save_data_to_json_file(
    data: Any,
    downloads_folder: str,
    file_name: str,
    compression: bool = False,
    open_fn: Optional[Callable[..., Any]] = None,
    makedirs_fn: Optional[Callable[..., Any]] = None,
) -> bool:
    open_fn = open_fn or open
    makedirs_fn = makedirs_fn or os.makedirs
    try:
        if compression:
            structured_logger.info(
                event="storage_persist_started",
                status=EVENT_STATUS_STARTED,
                message="Compressing data..",
            )
            mode = "wb"
            data, file_name_extension = compress_data(data)
            file_name += file_name_extension
            structured_logger.info(
                event="storage_persist_succeeded",
                status=EVENT_STATUS_SUCCEEDED,
                message="Data compressed successfully.",
            )
        else:
            mode = "w"
        makedirs_fn(downloads_folder, exist_ok=True)
        file_with_path = os.path.join(downloads_folder, file_name)
        structured_logger.info(
            event="storage_persist_started",
            status=EVENT_STATUS_STARTED,
            message=f"Writing buses_positions to file {file_with_path} ...",
        )
        with open_fn(file_with_path, mode) as file:
            file.write(data)
        structured_logger.info(
            event="storage_persist_succeeded",
            status=EVENT_STATUS_SUCCEEDED,
            message="File saved successfully!!!",
        )
        return True
    except Exception as e:
        structured_logger.error(
            event="storage_persist_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Failed to save file: {e}",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        raise

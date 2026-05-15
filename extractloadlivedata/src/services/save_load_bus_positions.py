from src.infra.local_file_storage import save_data_to_json_file
from src.infra.object_storage import (
    write_generic_bytes_to_object_storage,
)
from src.infra.compression import compress_data, decompress_data
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import json
import os
import glob
from typing import Any, Callable, Dict, List, Optional, Tuple
from src.infra.structured_logging import get_structured_logger
from src.domain.events import EVENT_STATUS_FAILED, EVENT_STATUS_RETRY, EVENT_STATUS_STARTED, EVENT_STATUS_SUCCEEDED
from src.services.exceptions import (
    LocalIngestBufferSaveError,
    SavePositionsToRawError,
)

structured_logger = get_structured_logger(
    service="extractloadlivedata",
    component="save_load_bus_positions",
    logger_name=__name__,)

ConfigDict = Dict[str, Any]
PayloadDict = Dict[str, Any]
StorageSaveResult = Dict[str, Any]


def get_file_name_from_data(data: PayloadDict) -> Tuple[str, str]:
    metadata = data.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError("Data metadata does not have a valid structure.")
    iso_timestamp_str = metadata.get("extracted_at")
    if not isinstance(iso_timestamp_str, str):
        raise ValueError("Missing or invalid metadata.extracted_at.")

    # Parse the timestamp
    dt = datetime.fromisoformat(iso_timestamp_str)

    # If naive (no timezone), assume UTC and make it aware
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        # Update metadata with timezone-aware ISO string for Airflow
        metadata["extracted_at"] = dt.isoformat()

    # Convert to São Paulo time for filename/partition
    dt_object = dt.astimezone(ZoneInfo("America/Sao_Paulo"))
    year = dt_object.year
    month = f"{dt_object.month:02d}"
    day = f"{dt_object.day:02d}"
    hour_minute = dt_object.strftime("%H%M")
    filename = f"posicoes_onibus-{year}{month}{day}{hour_minute}.json"
    partition = f"year={year}/month={month}/day={day}/"
    return filename, partition


def save_bus_positions_to_local_volume(
    config: ConfigDict, data: PayloadDict
) -> None:
    def get_config(config):
        ingest_buffer_folder = config["INGEST_BUFFER_PATH"]
        compression = config["DATA_COMPRESSION_ON_SAVE"] == "true"
        return ingest_buffer_folder, compression

    ingest_buffer_folder, compression = get_config(config)
    data_json = json.dumps(data)
    filename, _ = get_file_name_from_data(data)
    try:
        save_data_to_json_file(
            data_json,
            ingest_buffer_folder,
            filename,
            compression,
        )
    except Exception as e:
        structured_logger.error(
            event="storage_persist_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Failed to save buses positions to local volume for file '{filename}': {e}",
        )
        raise LocalIngestBufferSaveError(
            "failed to save buses positions to local volume"
        ) from e


def load_bus_positions_from_local_volume_file(
    folder: str,
    file: str,
    open_fn: Optional[Callable[..., Any]] = None,
) -> PayloadDict:
    try:
        file_path = f"{folder}/{file}"
        open_fn = open_fn or open
        if file.split(".")[-1] != "json":
            structured_logger.debug(
                event="pending_storage_file_started",
                status=EVENT_STATUS_STARTED,
                message=f"Pending file '{file}' is compressed.",
            )
            file_is_compressed = True
        else:
            file_is_compressed = False
        if file_is_compressed:
            with open_fn(file_path, "rb") as f:
                file_content = f.read()
                file_content = json.loads(decompress_data(file_content))
        else:
            with open_fn(file_path, "r") as f:
                file_content_json = f.read()
                file_content = json.loads(file_content_json)
    except Exception as e:
        structured_logger.error(
            event="pending_storage_file_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Error getting pending file '{file}': {e}",
        )
        raise ValueError(f"Error getting pending file '{file}': {e}")
    return file_content


def remove_local_file(
    config: ConfigDict,
    data: PayloadDict,
    glob_fn: Optional[Callable[..., Any]] = None,
    remove_fn: Optional[Callable[..., Any]] = None,
) -> None:
    def get_config(config):
        ingest_buffer_folder = config["INGEST_BUFFER_PATH"]
        return ingest_buffer_folder

    ingest_buffer_folder = get_config(config)
    glob_fn = glob_fn or glob.glob
    remove_fn = remove_fn or os.remove
    filename_without_path, _ = get_file_name_from_data(data)
    filename = f"{ingest_buffer_folder}/{filename_without_path}*"
    structured_logger.debug(
        event="pending_storage_file_started",
        status=EVENT_STATUS_STARTED,
        message=f"Attempting to remove local file(s) matching '{filename}' from '{ingest_buffer_folder}'",
    )
    matching_files = glob_fn(filename)
    structured_logger.debug(
        event="pending_storage_file_started",
        status=EVENT_STATUS_STARTED,
        message=f"Matching files found: {matching_files}",
    )
    if not matching_files:
        structured_logger.error(
            event="pending_storage_file_failed",
            status=EVENT_STATUS_FAILED,
            message=f"No matching local file found for '{filename}' to remove.",
        )
        return
    if len(matching_files) > 1:
        structured_logger.warning(
            event="pending_storage_file_started",
            status=EVENT_STATUS_STARTED,
            message=f"Multiple matching local files found for '{filename}'. Attempting to remove all matches.",
        )
    for file in matching_files:
        # file_path = f"{ingest_buffer_folder}/{file}"
        file_path = file
        try:
            remove_fn(file_path)
            structured_logger.info(
                event="pending_storage_file_succeeded",
                status=EVENT_STATUS_SUCCEEDED,
                message=f"Local file '{file_path}' removed successfully.",
            )
        except Exception as e:
            structured_logger.error(
                event="pending_storage_file_failed",
                status=EVENT_STATUS_FAILED,
                message=f"Error removing local file '{file_path}': {e}",
            )


def get_pending_storage_save_list(
    config: ConfigDict,
    listdir_fn: Optional[Callable[..., Any]] = None,
) -> List[str]:
    def get_config(config):
        ingest_buffer_folder = config["INGEST_BUFFER_PATH"]
        return ingest_buffer_folder

    ingest_buffer_folder = get_config(config)
    listdir_fn = listdir_fn or os.listdir
    pending_files = []
    for file in listdir_fn(ingest_buffer_folder):
        if file.startswith("posicoes_onibus"):
            pending_files.append(file)
    return pending_files


def save_bus_positions_to_storage_with_retries(
    config: ConfigDict,
    data: PayloadDict,
    sleep_fn: Optional[Callable[[int], None]] = None,
    save_fn: Optional[Callable[[ConfigDict, PayloadDict], None]] = None,
    with_metrics: bool = False,
) -> Any:
    def get_config(config):
        storage_max_retries = config["STORAGE_MAX_RETRIES"]
        return storage_max_retries

    storage_max_retries = get_config(config)
    sleep_fn = sleep_fn or time.sleep
    save_fn = save_fn or save_bus_positions_to_storage

    retries = 0
    back_off = 1
    save_successful = False
    while not save_successful:
        try:
            save_fn(config, data)
            save_successful = True
            if retries > 0:
                structured_logger.info(
                    event="storage_persist_succeeded",
                    status=EVENT_STATUS_SUCCEEDED,
                    message=f"Storage save successful after {retries} retries.",
                )
            if with_metrics:
                return {
                    "result": None,
                    "metrics": {
                        "retries": retries,
                    },
                }
            return True
        except Exception as e:
            retries += 1
            if retries >= storage_max_retries:
                structured_logger.error(
                    event="storage_persist_failed",
                    status=EVENT_STATUS_FAILED,
                    message=f"Max retries reached for Storage. Persistence failed. Error: {e}",
                )
                error = SavePositionsToRawError(
                    "max retries reached while saving positions to raw storage"
                )
                setattr(error, "retries", retries)
                raise error from e
            structured_logger.warning(
                event="storage_persist_failed",
                status=EVENT_STATUS_RETRY,
                message=f"Storage save failed! Retrying in {back_off} seconds... Error: {e}",
            )
            sleep_fn(back_off)
            back_off *= 2


def save_bus_positions_to_storage(config: ConfigDict, data: PayloadDict) -> None:
    def get_config(config):
        compression = config["DATA_COMPRESSION_ON_SAVE"] == "true"
        return compression

    compression = get_config(config)
    if not data_structure_is_valid(data):
        structured_logger.error(
            event="storage_persist_failed",
            status=EVENT_STATUS_FAILED,
            message="Data structure is invalid. Skipping processing.",
        )
        raise ValueError("Data structure is invalid.")
    hour_minute, total_qv, total_bus_lines = get_payload_summary(data)
    structured_logger.debug(
        event="storage_persist_started",
        status=EVENT_STATUS_STARTED,
        message=f"Received data for {total_qv} vehicles from {total_bus_lines} bus lines.",
    )
    data_json = json.dumps(data)
    save_data_to_raw_object_storage(
        config,
        data=data_json,
        compression=compression,
    )


def get_payload_summary(data: PayloadDict) -> Tuple[str, int, int]:
    payload = data.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("Data payload does not have a valid structure.")
    hr = payload.get("hr")
    if not isinstance(hr, str):
        raise ValueError("Missing or invalid payload.hr.")
    hour_minute = hr.replace(":", "")
    total_qv = 0
    lines = payload.get("l", [])
    if not isinstance(lines, list):
        raise ValueError("Missing or invalid payload.l.")
    for line in lines:
        total_qv += int(line.get("qv", 0))
    total_bus_lines = len(lines)
    return hour_minute, total_qv, total_bus_lines


def data_structure_is_valid(data: Any) -> bool:
    if not isinstance(data, dict):
        structured_logger.error(
            event="storage_persist_failed",
            status=EVENT_STATUS_FAILED,
            message="Data does not have a valid structure.",
        )
        return False
    required_fields = ["payload", "metadata"]
    for field in required_fields:
        if field not in data:
            structured_logger.error(
                event="storage_persist_failed",
                status=EVENT_STATUS_FAILED,
                message=f"Missing required field: {field}",
            )
            structured_logger.error(
                event="storage_persist_failed",
                status=EVENT_STATUS_FAILED,
                message=f"Data content: {data}",
            )
            return False
    if not isinstance(data.get("metadata"), dict):
        structured_logger.error(
            event="storage_persist_failed",
            status=EVENT_STATUS_FAILED,
            message="Data metadata does not have a valid structure.",
        )
        return False
    metadata = data.get("metadata")
    if not isinstance(metadata, dict):
        return False
    required_fields = ["source", "extracted_at", "total_vehicles"]
    for field in required_fields:
        if field not in metadata:
            structured_logger.error(
                event="metadata_validation_failed",
                status=EVENT_STATUS_FAILED,
                message=f"Missing required metadata field: {field}",
            )
            structured_logger.error(
                event="metadata_validation_failed",
                status=EVENT_STATUS_FAILED,
                message=f"Metadata content: {data.get('metadata')}",
            )
            return False
    if not isinstance(data.get("payload"), dict):
        structured_logger.error(
            event="storage_persist_failed",
            status=EVENT_STATUS_FAILED,
            message="Data payload does not have a valid structure.",
        )
        structured_logger.error(
            event="storage_persist_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Payload content: {data.get('payload')}",
        )
        structured_logger.error(
            event="storage_persist_failed",
            status=EVENT_STATUS_FAILED,
            message=f"Metadata content: {data.get('metadata')}",
        )
        return False
    required_fields = ["hr", "l"]
    payload = data.get("payload")
    if not isinstance(payload, dict):
        return False
    for field in required_fields:
        if field not in payload:
            structured_logger.error(
                event="storage_persist_failed",
                status=EVENT_STATUS_FAILED,
                message=f"Missing required payload field: {field}",
            )
            return False
    return True


def save_data_to_raw_object_storage(
    config: ConfigDict,
    data: str,
    compression: bool = False,
    client: Optional[Any] = None,
) -> None:
    def get_config(config):
        raw_bucket_name = config["SOURCE_BUCKET"]
        app_folder = config["APP_FOLDER"]
        connection_data = {
            "minio_endpoint": config["MINIO_ENDPOINT"],
            "access_key": config["ACCESS_KEY"],
            "secret_key": config["SECRET_KEY"],
            "secure": False,
        }
        return raw_bucket_name, app_folder, connection_data

    raw_bucket_name, app_folder, connection_data = get_config(config)
    if data:
        filename, partition = get_file_name_from_data(json.loads(data))
        prefix = f"{app_folder}/{partition}"
        destination_object_name = f"{prefix}{filename}"
        output_buffer: bytes
        structured_logger.debug(
            event="storage_persist_started",
            status=EVENT_STATUS_STARTED,
            message=f"Saving data to storage with object name: {destination_object_name}",
        )
        try:
            if compression:
                structured_logger.debug(
                    event="object_storage_compression_started",
                    status=EVENT_STATUS_STARTED,
                    message="Compressing data with zstd...",
                )
                output_buffer, file_name_extension = compress_data(data)
                destination_object_name += file_name_extension
                structured_logger.info(
                    event="object_storage_compression_succeeded",
                    status=EVENT_STATUS_SUCCEEDED,
                    message="Data compressed successfully.",
                )
            else:
                output_buffer = data.encode("utf-8")
            write_generic_bytes_to_object_storage(
                connection_data,
                buffer=output_buffer,
                bucket_name=raw_bucket_name,
                object_name=destination_object_name,
                client=client,
            )
            structured_logger.info(
                event="storage_persist_succeeded",
                status=EVENT_STATUS_SUCCEEDED,
                message=f"Data written to storage with object name: {destination_object_name}",
            )
        except Exception as e:
            structured_logger.error(
                event="storage_persist_failed",
                status=EVENT_STATUS_FAILED,
                message=(
                    "Error writing data to MinIO bucket "
                    f"'{raw_bucket_name}' with object name '{destination_object_name}'."
                ),
            )
            structured_logger.error(
                event="storage_persist_failed",
                status=EVENT_STATUS_FAILED,
                message=f"Exception details: {e}",
            )
            raise ValueError("Data structure is invalid.")
    else:
        structured_logger.error(
            event="storage_persist_failed",
            status=EVENT_STATUS_FAILED,
            message="No records found to write to the destination bucket.",
        )

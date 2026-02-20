from src.infra.storage import save_data_to_json_file
from src.infra.minio_functions import write_generic_bytes_to_minio
from src.infra.compression import compress_data
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import json
import os
import glob
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def get_file_name_from_data(data):
    iso_timestamp_str = data.get("metadata").get("extracted_at")
    dt_utc = datetime.fromisoformat(iso_timestamp_str)
    dt_object = dt_utc.astimezone(ZoneInfo("America/Sao_Paulo"))
    year = dt_object.year
    month = f"{dt_object.month:02d}"
    day = f"{dt_object.day:02d}"
    hour_minute, _, _ = get_payload_summary(data)
    filename = f"posicoes_onibus-{year}{month}{day}{hour_minute}.json"
    return filename


def save_bus_positions_to_local_volume(config, data):
    def get_config(config):
        ingest_buffer_folder = config["INGEST_BUFFER_PATH"]
        compression = config["DATA_COMPRESSION_ON_SAVE"] == "true"
        return ingest_buffer_folder, compression

    ingest_buffer_folder, compression = get_config(config)
    data_json = json.dumps(data)
    filename = get_file_name_from_data(data)
    save_data_to_json_file(
        data_json,
        ingest_buffer_folder,
        filename,
        compression,
    )


def remove_local_file(config, data):
    def get_config(config):
        ingest_buffer_folder = config["INGEST_BUFFER_PATH"]
        return ingest_buffer_folder

    ingest_buffer_folder = get_config(config)
    filename = f"{ingest_buffer_folder}/{get_file_name_from_data(data)}*"
    logging.info(
        f"Attempting to remove local file(s) matching '{filename}' from '{ingest_buffer_folder}'"
    )
    matching_files = glob.glob(filename)
    logger.debug(f"Matching files found: {matching_files}")
    if not matching_files:
        logger.error(f"No matching local file found for '{filename}' to remove.")
        return
    if len(matching_files) > 1:
        logger.warning(
            f"Multiple matching local files found for '{filename}'. Attempting to remove all matches."
        )
    for file in matching_files:
        # file_path = f"{ingest_buffer_folder}/{file}"
        file_path = file
        try:
            os.remove(file_path)
            logger.info(f"Local file '{file_path}' removed successfully.")
        except Exception as e:
            logger.error(f"Error removing local file '{file_path}': {e}")


def get_pending_storage_save_list(config):
    def get_config(config):
        ingest_buffer_folder = config["INGEST_BUFFER_PATH"]
        return ingest_buffer_folder

    ingest_buffer_folder = get_config(config)
    pending_files = []
    for file in os.listdir(ingest_buffer_folder):
        if file.startswith("posicoes_onibus"):
            pending_files.append(file)
    return pending_files


def save_bus_positions_to_storage_with_retries(config, data):
    def get_config(config):
        # Usamos uma chave específica para retries de storage,
        # ou reaproveitamos a da API conforme sua preferência
        storage_max_retries = int(config.get("STORAGE_MAX_RETRIES", 5))
        return storage_max_retries

    storage_max_retries = get_config(config)

    retries = 0
    back_off = 1
    save_successful = False
    while not save_successful:
        try:
            save_bus_positions_to_storage(config, data)
            save_successful = True
            if retries > 0:
                logger.info(f"Storage save successful after {retries} retries.")
            return True
        except Exception as e:
            retries += 1
            if retries >= storage_max_retries:
                logger.error(
                    f"Max retries reached for Storage. Persistence failed. Error: {e}"
                )
                # Aqui você decide se levanta a exceção ou apenas retorna False
                return False
            logger.warning(
                f"Storage save failed! Retrying in {back_off} seconds... Error: {e}"
            )
            time.sleep(back_off)
            back_off *= 2
    return False


def save_bus_positions_to_storage(config, data):
    def get_config(config):
        compression = config["DATA_COMPRESSION_ON_SAVE"] == "true"
        return compression

    compression = get_config(config)
    if not data_structure_is_valid(data):
        logger.error("Data structure is invalid. Skipping processing.")
        raise ValueError("Data structure is invalid.")
    hour_minute, total_qv, total_bus_lines = get_payload_summary(data)
    logger.info(
        f"Received data for {total_qv} vehicles from {total_bus_lines} bus lines."
    )
    hour_minute, _, _ = get_payload_summary(data)
    data_json = json.dumps(data)
    save_data_to_raw_object_storage(
        config,
        data=data_json,
        hour_minute=hour_minute,
        compression=compression,
    )


def get_payload_summary(data):
    hour_minute = data.get("payload").get("hr").replace(":", "")
    total_qv = 0
    payload = data.get("payload")
    for line in payload.get("l", []):
        # logger.info(f"Line: {line.get('qv')}")
        total_qv += int(line.get("qv", 0))
    total_bus_lines = len(data.get("l", []))
    return hour_minute, total_qv, total_bus_lines


def data_structure_is_valid(data):
    if not isinstance(data, dict):
        logger.error("Data does not have a valid structure.")
        return False
    required_fields = ["payload", "metadata"]
    for field in required_fields:
        if field not in data:
            logger.error(f"Missing required field: {field}")
            logger.error(f"Data content: {data}")
            return False
    if not isinstance(data.get("metadata"), dict):
        logger.error("Data metadata does not have a valid structure.")
        return False
    required_fields = ["source", "extracted_at", "total_vehicles"]
    for field in required_fields:
        if field not in data.get("metadata"):
            logger.error(f"Missing required metadata field: {field}")
            logger.error(f"Metadata content: {data.get('metadata')}")
            return False
    if not isinstance(data.get("payload"), dict):
        logger.error("Data payload does not have a valid structure.")
        logger.error(f"Payload content: {data.get('payload')}")
        logger.error(f"Metadata content: {data.get('metadata')}")
        return False
    required_fields = ["hr", "l"]
    for field in required_fields:
        if field not in data.get("payload"):
            logger.error(f"Missing required payload field: {field}")
            return False
    return True


def save_data_to_raw_object_storage(config, data, hour_minute, compression=False):
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
    iso_timestamp_str = json.loads(data).get("metadata").get("extracted_at")
    dt_utc = datetime.fromisoformat(iso_timestamp_str)
    dt_object = dt_utc.astimezone(ZoneInfo("America/Sao_Paulo"))
    year = dt_object.year
    month = f"{dt_object.month:02d}"
    day = f"{dt_object.day:02d}"
    if data:
        prefix = f"{app_folder}/year={year}/month={month}/day={day}/"
        base_file_name = "posicoes_onibus"
        destination_object_name = (
            f"{prefix}{base_file_name}-{year}{month}{day}{hour_minute}.json"
        )
        logger.info(
            f"Saving data to storage with object name: {destination_object_name}"
        )
        try:
            if compression:
                logger.info("Compressing data with zstd...")
                data, file_name_extension = compress_data(data)
                destination_object_name += file_name_extension
                logger.info("Data compressed successfully.")
            else:
                data = data.encode("utf-8")
            write_generic_bytes_to_minio(
                connection_data,
                buffer=data,
                bucket_name=raw_bucket_name,
                object_name=destination_object_name,
            )
        except Exception as e:
            logger.error(
                "Error writing data to MinIO bucket "
                f"'{raw_bucket_name}' with object name '{destination_object_name}'."
            )
            logger.error(f"Exception details: {e}")
            raise ValueError("Data structure is invalid.")
    else:
        logger.error("No records found to write to the destination bucket.")

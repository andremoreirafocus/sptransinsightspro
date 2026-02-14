from src.infra.storage import save_data_to_json_file
from src.infra.minio_functions import write_generic_bytes_to_minio
from datetime import datetime
from zoneinfo import ZoneInfo
import json
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def save_bus_positions_to_local_volume(config, data):
    def get_config(config):
        ingest_buffer_folder = config["INGEST_BUFFER_PATH"]
        return ingest_buffer_folder

    ingest_buffer_folder = get_config(config)
    hour_minute, _, _ = get_payload_summary(data)
    data_json = json.dumps(data)
    save_data_to_json_file(
        data_json, ingest_buffer_folder, f"buses_positions_{hour_minute}.json", True
    )


def save_bus_positions_to_storage(config, data):
    if not data_structure_is_valid(data):
        logger.error("Data structure is invalid. Skipping processing.")
        raise ValueError("Data structure is invalid.")
    hour_minute, total_qv, total_bus_lines = get_payload_summary(data)
    logger.info(
        f"Received data for {total_qv} vehicles from {total_bus_lines} bus lines."
    )
    hour_minute, _, _ = get_payload_summary(data)
    data_json = json.dumps(data)
    load_data_to_raw(
        config,
        data=data_json,
        hour_minute=hour_minute,
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


def load_data_to_raw(config, data, hour_minute):
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
        try:
            write_generic_bytes_to_minio(
                connection_data,
                buffer=data.encode("utf-8"),
                bucket_name=raw_bucket_name,
                object_name=destination_object_name,
            )
        except Exception as e:
            logger.error(
                "Error writing data to MinIO bucket "
                f"'{raw_bucket_name}' with object name '{destination_object_name}'."
            )
            logger.error(f"Exception details: {e}")
    else:
        logger.error("No records found to write to the destination bucket.")

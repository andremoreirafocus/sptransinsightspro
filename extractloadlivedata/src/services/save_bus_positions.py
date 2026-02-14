from src.services.load_data_to_raw import load_data_to_raw
from src.infra.storage import save_data_to_json_file
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
        data_json,
        ingest_buffer_folder,
        file_name=f"buses_positions_{hour_minute}.json",
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

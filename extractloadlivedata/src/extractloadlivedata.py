from src.services.extract_buses_positions import (
    extract_buses_positions_with_retries,
    get_buses_positions_with_metadata,
)
from src.services.save_bus_positions import (
    # save_bus_positions_to_storage,
    save_bus_positions_to_local_volume,
    save_bus_positions_to_storage_with_retries,
    remove_local_file,
    get_pending_storage_save_list,
)
from src.services.trigger_airflow import (
    create_pending_invokation,
    trigger_pending_invokations,
)
from src.infra.compression import decompress_data
from src.config import get_config
import json
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def extractloadlivedata():
    config = get_config()
    ingest_buffer_folder = config["INGEST_BUFFER_PATH"]
    buses_positions_payload = extract_buses_positions_with_retries(config)
    download_successful = buses_positions_payload is not None
    if download_successful:
        buses_positions, _ = get_buses_positions_with_metadata(buses_positions_payload)
        save_bus_positions_to_local_volume(config, buses_positions)
    pending_storage_save_list = get_pending_storage_save_list(config)
    if pending_storage_save_list:
        logger.warning(
            f"There are {len(pending_storage_save_list)} pending files to be saved to storage: {pending_storage_save_list}"
        )
        for pending_storage_save_file in pending_storage_save_list:
            logger.info(f"Attempting to save pending file '{pending_storage_save_file}' to storage.")
            pending_storage_save_file_path = f"{ingest_buffer_folder}/{pending_storage_save_file}"
            if pending_storage_save_file.split(".")[-1] != "json":
                logger.info(f"Pending file '{pending_storage_save_file}' is compressed.")
                file_is_compressed = True
            else:
                file_is_compressed = False
            save_on_storage_failure = False
            try:
                if file_is_compressed:
                    with open(pending_storage_save_file_path, "rb") as f:
                        pending_data = f.read()
                        pending_data = json.loads(decompress_data(pending_data))
                else:
                    with open(pending_storage_save_file_path, "r") as f:
                        pending_data_json = f.read()
                        pending_data = json.loads(pending_data_json)
                if save_bus_positions_to_storage_with_retries(config, pending_data):
                    remove_local_file(config, pending_data)
                    create_pending_invokation(pending_storage_save_file)
                else:
                    logger.error(
                        f"Failed to save pending file '{pending_storage_save_file}' to storage after retries."
                    )
                    save_on_storage_failure = True
                    break
            except Exception as e:
                logger.error(f"Error processing pending file '{pending_storage_save_file}': {e}")
        if save_on_storage_failure:
            logger.error(
                "One or more pending files failed to save to storage. Waiting for the next execution to retry."
            )
        trigger_pending_invokations()

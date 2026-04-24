from src.services.extract_buses_positions import (
    extract_buses_positions_with_retries,
    get_buses_positions_with_metadata,
)
from src.services.save_load_bus_positions import (
    save_bus_positions_to_local_volume,
    save_bus_positions_to_storage_with_retries,
    load_bus_positions_from_local_volume_file,
    remove_local_file,
    get_pending_storage_save_list,
)

from src.services.trigger_airflow import (
    create_pending_invokation,
    trigger_pending_airflow_dag_invokations,
)
from src.services.save_processing_requests import (
    create_pending_processing_request,
    trigger_pending_processing_requests,
)
from src.services.exceptions import (
    IngestNotificationError,
    LocalIngestBufferSaveError,
    PositionsDownloadError,
    SavePositionsToRawError,
)
from dataclasses import dataclass
from typing import Callable, Optional, Tuple, Any

from src.config import get_config
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Services:
    extract_buses_positions_with_retries: Callable[[dict], Any]
    get_buses_positions_with_metadata: Callable[[Any], Tuple[Any, Any]]
    save_bus_positions_to_local_volume: Callable[[dict, Any], None]
    save_bus_positions_to_storage_with_retries: Callable[[dict, Any], bool]
    load_bus_positions_from_local_volume_file: Callable[[str, str], Any]
    remove_local_file: Callable[[dict, Any], None]
    get_pending_storage_save_list: Callable[[dict], list]
    create_pending_invokation: Callable[[dict, str], None]
    trigger_pending_airflow_dag_invokations: Callable[[dict], None]
    create_pending_processing_request: Callable[[dict, str], None]
    trigger_pending_processing_requests: Callable[[dict], None]


def _build_services() -> Services:
    return Services(
        extract_buses_positions_with_retries=extract_buses_positions_with_retries,
        get_buses_positions_with_metadata=get_buses_positions_with_metadata,
        save_bus_positions_to_local_volume=save_bus_positions_to_local_volume,
        save_bus_positions_to_storage_with_retries=save_bus_positions_to_storage_with_retries,
        load_bus_positions_from_local_volume_file=load_bus_positions_from_local_volume_file,
        remove_local_file=remove_local_file,
        get_pending_storage_save_list=get_pending_storage_save_list,
        create_pending_invokation=create_pending_invokation,
        trigger_pending_airflow_dag_invokations=trigger_pending_airflow_dag_invokations,
        create_pending_processing_request=create_pending_processing_request,
        trigger_pending_processing_requests=trigger_pending_processing_requests,
    )


def _get_config_values(config: dict) -> Tuple[str, str]:
    ingest_buffer_folder = config["INGEST_BUFFER_PATH"]
    notification_engine = config.get("NOTIFICATION_ENGINE")
    if notification_engine is None:
        raise KeyError(
            "NOTIFICATION_ENGINE configuration is missing."
        )
    notification_engine = notification_engine.strip()
    return ingest_buffer_folder, notification_engine


def extractloadlivedata(
    config: Optional[dict] = None,
    services: Optional[Services] = None,
) -> None:
    services = services or _build_services()
    config = config or get_config()
    try:
        ingest_buffer_folder, notification_engine = _get_config_values(config)
    except Exception as e:
        logger.error(
            "Configuration error in extractloadlivedata execution: %s", e, exc_info=True
        )
        return
    logger.info(f"Notification engine set to: {notification_engine}")
    download_successful = False
    try:
        buses_positions_payload = services.extract_buses_positions_with_retries(config)
        download_successful = buses_positions_payload is not None
    except PositionsDownloadError as e:
        logger.error("Positions download failed: %s", e, exc_info=True)
    except Exception as e:
        logger.error("Unexpected positions download error: %s", e, exc_info=True)
    if download_successful:
        try:
            buses_positions, _ = services.get_buses_positions_with_metadata(
                buses_positions_payload
            )
            services.save_bus_positions_to_local_volume(config, buses_positions)
        except LocalIngestBufferSaveError as e:
            logger.error("Local ingest buffer save failed: %s", e, exc_info=True)
        except Exception as e:
            logger.error(
                "Unexpected error while saving current positions locally: %s",
                e,
                exc_info=True,
            )
    try:
        pending_storage_save_list = services.get_pending_storage_save_list(config)
    except Exception as e:
        logger.error("Failed to list pending storage save files: %s", e, exc_info=True)
        return
    if pending_storage_save_list:
        logger.warning(
            f"There are {len(pending_storage_save_list)} pending files to be saved to storage: {pending_storage_save_list}"
        )
        save_on_storage_failure = False
        for pending_storage_save_file in pending_storage_save_list:
            logger.info(
                f"Attempting to save pending file '{pending_storage_save_file}' to storage."
            )
            try:
                pending_storage_save_file_content = (
                    services.load_bus_positions_from_local_volume_file(
                        ingest_buffer_folder, pending_storage_save_file
                    )
                )
                services.save_bus_positions_to_storage_with_retries(
                    config, pending_storage_save_file_content
                )
                logger.info("Pending file saved to storage successfully.")
                services.remove_local_file(
                    config, pending_storage_save_file_content
                )
                if notification_engine == "airflow":
                    services.create_pending_invokation(
                        config, pending_storage_save_file
                    )
                else:
                    services.create_pending_processing_request(
                        config, pending_storage_save_file
                    )
            except SavePositionsToRawError as e:
                logger.error(
                    "Raw storage save failed for pending file '%s': %s",
                    pending_storage_save_file,
                    e,
                    exc_info=True,
                )
                save_on_storage_failure = True
                break
            except Exception as e:
                logger.error(
                    f"Error processing pending file '{pending_storage_save_file}': {e}",
                    exc_info=True,
                )
        if save_on_storage_failure:
            logger.error(
                "One or more pending files failed to save to storage. Waiting for the next execution to retry."
            )
        try:
            if notification_engine == "airflow":
                services.trigger_pending_airflow_dag_invokations(config)
            else:
                services.trigger_pending_processing_requests(config)
        except IngestNotificationError as e:
            logger.error("Ingest notification failed: %s", e, exc_info=True)
        except Exception as e:
            logger.error("Unexpected ingest notification error: %s", e, exc_info=True)

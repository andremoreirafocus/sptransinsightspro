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
from uuid import uuid4
import json

from src.config import get_config
from src.reporting import create_failure_quality_report, build_quality_summary
from src.infra.alertservice_client import send_alert
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)
SEVERE_NON_RECOVERABLE_FAILURE_MESSAGE_PREFIX = "[SEVERE] non recoverable "


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
    send_alert_fn: Callable[[str, dict], None] = send_alert,
) -> None:
    services = services or _build_services()
    config = config or get_config()
    execution_id = str(uuid4())
    webhook_url = config.get("NOTIFICATIONS_WEBHOOK_URL", "")
    items_total = 0
    items_failed = 0
    retries_seen = 0

    def _parse_notification_metrics(metrics: Any) -> Tuple[int, int, int]:
        if not isinstance(metrics, dict):
            raise TypeError("notification metrics must be a dict")
        success = metrics["success"]
        failed = metrics["failed"]
        retries = metrics["retries"]
        if not isinstance(success, int) or success < 0:
            raise TypeError("notification metrics['success'] must be a non-negative int")
        if not isinstance(failed, int) or failed < 0:
            raise TypeError("notification metrics['failed'] must be a non-negative int")
        if not isinstance(retries, int) or retries < 0:
            raise TypeError("notification metrics['retries'] must be a non-negative int")
        return success, failed, retries

    def _phase_message(phase: str) -> str:
        if phase == "positions_download":
            return f"{SEVERE_NON_RECOVERABLE_FAILURE_MESSAGE_PREFIX}api get failed"
        if phase == "local_ingest_buffer_save_positions":
            return f"{SEVERE_NON_RECOVERABLE_FAILURE_MESSAGE_PREFIX}save to local buffer failed"
        if phase == "save_positions_to_raw":
            return "save to raw storage failed"
        if phase == "ingest_notification":
            return "ingest notification failed"
        return "ingest execution failed"

    def _emit_failure_alert(failure_phase: str, message: Optional[str] = None) -> None:
        failure_message = message or _phase_message(failure_phase)
        summary = create_failure_quality_report(
            pipeline="extractloadlivedata",
            execution_id=execution_id,
            failure_phase=failure_phase,
            failure_message=failure_message,
            quality_report_path="null",
            acceptance_rate=1.0,
            items_failed=items_failed,
            items_total=items_total,
            retries=retries_seen,
        )
        logger.info(
            "alertservice_summary_payload\n%s",
            json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        )
        send_alert_fn(webhook_url, summary)

    def _emit_final_summary(status: str) -> None:
        summary = build_quality_summary(
            pipeline="extractloadlivedata",
            execution_id=execution_id,
            status=status,
            items_failed=items_failed,
            quality_report_path="null",
            acceptance_rate=1.0,
            items_total=items_total,
            retries=retries_seen,
        )
        logger.info(
            "alertservice_summary_payload\n%s",
            json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        )
        send_alert_fn(webhook_url, summary)

    try:
        ingest_buffer_folder, notification_engine = _get_config_values(config)
    except Exception as e:
        logger.error(
            "Configuration error in extractloadlivedata execution: %s", e, exc_info=True
        )
        items_failed += 1
        _emit_failure_alert("unknown")
        return
    logger.info(f"Notification engine set to: {notification_engine}")
    download_successful = False
    try:
        items_total += 1
        download_result = services.extract_buses_positions_with_retries(
            config, with_metrics=True
        )
        buses_positions_payload = download_result["result"]
        retries_seen += int(download_result.get("metrics", {}).get("retries", 0))
        download_successful = buses_positions_payload is not None
    except PositionsDownloadError as e:
        logger.error("Positions download failed: %s", e, exc_info=True)
        retries_seen += int(getattr(e, "retries", 0))
        items_failed += 1
        _emit_failure_alert("positions_download")
    except Exception as e:
        logger.error("Unexpected positions download error: %s", e, exc_info=True)
        items_failed += 1
        _emit_failure_alert("unknown")
    if download_successful:
        try:
            items_total += 1
            buses_positions, _ = services.get_buses_positions_with_metadata(
                buses_positions_payload
            )
            services.save_bus_positions_to_local_volume(config, buses_positions)
        except LocalIngestBufferSaveError as e:
            logger.error("Local ingest buffer save failed: %s", e, exc_info=True)
            items_failed += 1
            _emit_failure_alert("local_ingest_buffer_save_positions")
        except Exception as e:
            logger.error(
                "Unexpected error while saving current positions locally: %s",
                e,
                exc_info=True,
            )
            items_failed += 1
            _emit_failure_alert("unknown")
    try:
        pending_storage_save_list = services.get_pending_storage_save_list(config)
    except Exception as e:
        logger.error("Failed to list pending storage save files: %s", e, exc_info=True)
        items_failed += 1
        _emit_failure_alert("unknown")
        return
    if pending_storage_save_list:
        logger.warning(
            f"There are {len(pending_storage_save_list)} pending files to be saved to storage: {pending_storage_save_list}"
        )
        save_on_storage_failure = False
        for pending_storage_save_file in pending_storage_save_list:
            items_total += 1
            logger.info(
                f"Attempting to save pending file '{pending_storage_save_file}' to storage."
            )
            try:
                pending_storage_save_file_content = (
                    services.load_bus_positions_from_local_volume_file(
                        ingest_buffer_folder, pending_storage_save_file
                    )
                )
                save_result = services.save_bus_positions_to_storage_with_retries(
                    config, pending_storage_save_file_content, with_metrics=True
                )
                retries_seen += int(save_result.get("metrics", {}).get("retries", 0))
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
                retries_seen += int(getattr(e, "retries", 0))
                items_failed += 1
                _emit_failure_alert("save_positions_to_raw")
                save_on_storage_failure = True
                break
            except Exception as e:
                logger.error(
                    f"Error processing pending file '{pending_storage_save_file}': {e}",
                    exc_info=True,
                )
                items_failed += 1
                _emit_failure_alert("unknown")
        if save_on_storage_failure:
            logger.error(
                "One or more pending files failed to save to storage. Waiting for the next execution to retry."
            )
        try:
            if notification_engine == "airflow":
                notification_result = services.trigger_pending_airflow_dag_invokations(
                    config, with_metrics=True
                )
            else:
                notification_result = services.trigger_pending_processing_requests(
                    config, with_metrics=True
                )
            success_count, failed_count, retries_count = _parse_notification_metrics(
                notification_result["metrics"]
            )
            items_total += success_count + failed_count
            items_failed += failed_count
            retries_seen += retries_count
        except IngestNotificationError as e:
            logger.error("Ingest notification failed: %s", e, exc_info=True)
            metrics = getattr(e, "metrics", None)
            try:
                success_count, failed_count, retries_count = _parse_notification_metrics(
                    metrics
                )
                items_total += success_count + failed_count
                items_failed += failed_count
                retries_seen += retries_count
            except (KeyError, TypeError) as metrics_error:
                logger.error(
                    "Invalid ingest notification metrics contract: %s",
                    metrics_error,
                    exc_info=True,
                )
                items_failed += 1
            _emit_failure_alert("ingest_notification")
        except Exception as e:
            logger.error("Unexpected ingest notification error: %s", e, exc_info=True)
            items_failed += 1
            _emit_failure_alert("unknown")

    if items_failed > 0:
        return
    if retries_seen > 0:
        _emit_final_summary("WARN")
    else:
        _emit_final_summary("PASS")

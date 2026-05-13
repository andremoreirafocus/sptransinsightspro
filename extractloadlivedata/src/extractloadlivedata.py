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
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, cast
from uuid import uuid4

from src.config import get_config
from src.logging_taxonomy import ALLOWED_EVENTS, ALLOWED_STATUSES, LogStatus
from src.reporting import create_failure_quality_report, build_quality_summary
from src.infra.alertservice_client import send_alert
from src.infra.structured_logging import get_structured_logger

structured_logger = get_structured_logger(
    service="extractloadlivedata",
    component="orchestrator",
    logger_name=__name__,
    allowed_events=ALLOWED_EVENTS,
    allowed_statuses=ALLOWED_STATUSES,
)
SEVERE_NON_RECOVERABLE_FAILURE_MESSAGE_PREFIX = "[SEVERE] non recoverable "

ConfigDict = Dict[str, Any]
PayloadDict = Dict[str, Any]
AlertSender = Callable[[str, Dict[str, Any]], None]


@dataclass(frozen=True)
class Services:
    extract_buses_positions_with_retries: Callable[..., Any]
    get_buses_positions_with_metadata: Callable[[PayloadDict], Tuple[PayloadDict, str]]
    save_bus_positions_to_local_volume: Callable[[ConfigDict, PayloadDict], None]
    save_bus_positions_to_storage_with_retries: Callable[..., Any]
    load_bus_positions_from_local_volume_file: Callable[[str, str], PayloadDict]
    remove_local_file: Callable[[ConfigDict, PayloadDict], None]
    get_pending_storage_save_list: Callable[[ConfigDict], List[str]]
    create_pending_invokation: Callable[[ConfigDict, str], None]
    trigger_pending_airflow_dag_invokations: Callable[..., Any]
    create_pending_processing_request: Callable[[ConfigDict, str], None]
    trigger_pending_processing_requests: Callable[..., Any]


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


def _get_config_values(config: ConfigDict) -> Tuple[str, str]:
    ingest_buffer_folder = config["INGEST_BUFFER_PATH"]
    notification_engine = config.get("NOTIFICATION_ENGINE")
    if notification_engine is None:
        raise KeyError(
            "NOTIFICATION_ENGINE configuration is missing."
        )
    notification_engine = notification_engine.strip()
    return ingest_buffer_folder, notification_engine


def extractloadlivedata(
    config: Optional[ConfigDict] = None,
    services: Optional[Services] = None,
    send_alert_fn: AlertSender = send_alert,
) -> None:
    services = services or _build_services()
    config = cast(ConfigDict, config or get_config())
    execution_id = str(uuid4())
    structured_logger.info(
        event="execution_started",
        status=LogStatus.STARTED,
        execution_id=execution_id,
        message="extractloadlivedata execution started.",
    )
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
        structured_logger.info(
            event="execution_summary_emitted",
            status=LogStatus.SUCCEEDED,
            execution_id=execution_id,
            message="Failure quality summary emitted to alertservice.",
            metadata={
                "failure_phase": failure_phase,
                "summary_payload": summary,
                "items_total": items_total,
                "items_failed": items_failed,
                "retries_seen": retries_seen,
            },
        )
        send_alert_fn(webhook_url, summary)

    def _emit_final_summary(status: Literal["PASS", "WARN", "FAIL"]) -> None:
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
        structured_logger.info(
            event="execution_summary_emitted",
            status=LogStatus.SUCCEEDED,
            execution_id=execution_id,
            message="Final quality summary emitted to alertservice.",
            metadata={
                "summary_status": status,
                "summary_payload": summary,
                "items_total": items_total,
                "items_failed": items_failed,
                "retries_seen": retries_seen,
            },
        )
        send_alert_fn(webhook_url, summary)

    structured_logger.info(
        event="config_validation_started",
        status=LogStatus.STARTED,
        execution_id=execution_id,
        message="Runtime configuration validation started.",
    )
    try:
        ingest_buffer_folder, notification_engine = _get_config_values(config)
        structured_logger.info(
            event="config_validation_succeeded",
            status=LogStatus.SUCCEEDED,
            execution_id=execution_id,
            message="Runtime configuration validation succeeded.",
        )
    except Exception as e:
        structured_logger.error(
            event="config_validation_failed",
            status=LogStatus.FAILED,
            execution_id=execution_id,
            message="Runtime configuration validation failed.",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        items_failed += 1
        _emit_failure_alert("unknown")
        return
    structured_logger.info(
        event="notification_engine_selected",
        status=LogStatus.SUCCEEDED,
        execution_id=execution_id,
        message="Notification engine selected.",
        metadata={"notification_engine": notification_engine},
    )
    download_successful = False
    try:
        structured_logger.info(
            event="extract_positions_started",
            status=LogStatus.STARTED,
            execution_id=execution_id,
            message="Bus positions extraction started.",
        )
        items_total += 1
        download_result = services.extract_buses_positions_with_retries(
            config, with_metrics=True
        )
        buses_positions_payload = download_result["result"]
        retries_seen += int(download_result.get("metrics", {}).get("retries", 0))
        download_successful = buses_positions_payload is not None
        structured_logger.info(
            event="extract_positions_succeeded",
            status=LogStatus.SUCCEEDED,
            execution_id=execution_id,
            message="Bus positions extraction succeeded.",
            metadata={"download_successful": download_successful, "retries_seen": retries_seen},
        )
    except PositionsDownloadError as e:
        structured_logger.error(
            event="extract_positions_failed",
            status=LogStatus.FAILED,
            execution_id=execution_id,
            message="Bus positions extraction failed.",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        retries_seen += int(getattr(e, "retries", 0))
        items_failed += 1
        _emit_failure_alert("positions_download")
    except Exception as e:
        structured_logger.error(
            event="extract_positions_failed",
            status=LogStatus.FAILED,
            execution_id=execution_id,
            message="Unexpected bus positions extraction failure.",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        items_failed += 1
        _emit_failure_alert("unknown")
    if download_successful:
        try:
            items_total += 1
            buses_positions, _ = services.get_buses_positions_with_metadata(
                buses_positions_payload
            )
            structured_logger.debug(
                event="storage_persist_started",
                status=LogStatus.STARTED,
                execution_id=execution_id,
                message="Starting storage persistence for current extraction payload.",
            )
            services.save_bus_positions_to_local_volume(config, buses_positions)
            structured_logger.debug(
                event="storage_persist_succeeded",
                status=LogStatus.SUCCEEDED,
                execution_id=execution_id,
                message="Storage persistence completed for current extraction payload.",
            )
        except LocalIngestBufferSaveError as e:
            structured_logger.error(
                event="storage_persist_failed",
                status=LogStatus.FAILED,
                execution_id=execution_id,
                message="Local ingest buffer save failed.",
                error_type=type(e).__name__,
                error_message=str(e),
            )
            items_failed += 1
            _emit_failure_alert("local_ingest_buffer_save_positions")
        except Exception as e:
            structured_logger.error(
                event="storage_persist_failed",
                status=LogStatus.FAILED,
                execution_id=execution_id,
                message="Unexpected local buffer persistence failure.",
                error_type=type(e).__name__,
                error_message=str(e),
            )
            items_failed += 1
            _emit_failure_alert("unknown")
    try:
        pending_storage_save_list = services.get_pending_storage_save_list(config)
        structured_logger.info(
            event="pending_storage_scan_succeeded",
            status=LogStatus.SUCCEEDED,
            execution_id=execution_id,
            message="Pending storage scan completed.",
            metadata={"pending_files_count": len(pending_storage_save_list)},
        )
    except Exception as e:
        structured_logger.error(
            event="pending_storage_scan_failed",
            status=LogStatus.FAILED,
            execution_id=execution_id,
            message="Failed to list pending storage save files.",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        items_failed += 1
        _emit_failure_alert("unknown")
        return
    if pending_storage_save_list:
        structured_logger.warning(
            event="pending_storage_detected",
            status=LogStatus.STARTED,
            execution_id=execution_id,
            message="Pending files detected for storage save.",
            metadata={
                "pending_files_count": len(pending_storage_save_list),
                "pending_files": pending_storage_save_list,
            },
        )
        save_on_storage_failure = False
        for pending_storage_save_file in pending_storage_save_list:
            items_total += 1
            structured_logger.info(
                event="pending_storage_file_started",
                status=LogStatus.STARTED,
                execution_id=execution_id,
                message="Attempting storage persistence for pending file.",
                metadata={"pending_file": pending_storage_save_file},
            )
            try:
                pending_storage_save_file_content = (
                    services.load_bus_positions_from_local_volume_file(
                        ingest_buffer_folder, pending_storage_save_file
                    )
                )
                structured_logger.debug(
                    event="storage_persist_started",
                    status=LogStatus.STARTED,
                    execution_id=execution_id,
                    message="Starting storage persistence for pending file payload.",
                    metadata={"pending_file": pending_storage_save_file},
                )
                save_result = services.save_bus_positions_to_storage_with_retries(
                    config, pending_storage_save_file_content, with_metrics=True
                )
                retries_seen += int(save_result.get("metrics", {}).get("retries", 0))
                structured_logger.debug(
                    event="storage_persist_succeeded",
                    status=LogStatus.SUCCEEDED,
                    execution_id=execution_id,
                    message="Storage persistence completed for pending file payload.",
                    metadata={"pending_file": pending_storage_save_file},
                )
                structured_logger.info(
                    event="pending_storage_file_succeeded",
                    status=LogStatus.SUCCEEDED,
                    execution_id=execution_id,
                    message="Pending file saved to storage successfully.",
                    metadata={
                        "pending_file": pending_storage_save_file,
                        "retries_seen": retries_seen,
                    },
                )
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
                structured_logger.error(
                    event="pending_storage_file_failed",
                    status=LogStatus.FAILED,
                    execution_id=execution_id,
                    message="Raw storage save failed for pending file.",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    metadata={"pending_file": pending_storage_save_file},
                )
                retries_seen += int(getattr(e, "retries", 0))
                items_failed += 1
                _emit_failure_alert("save_positions_to_raw")
                save_on_storage_failure = True
                break
            except Exception as e:
                structured_logger.error(
                    event="pending_storage_file_failed",
                    status=LogStatus.FAILED,
                    execution_id=execution_id,
                    message="Unexpected pending file processing error.",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    metadata={"pending_file": pending_storage_save_file},
                )
                items_failed += 1
                _emit_failure_alert("unknown")
        if save_on_storage_failure:
            structured_logger.error(
                event="storage_persist_failed",
                status=LogStatus.FAILED,
                execution_id=execution_id,
                message="One or more pending files failed to save to storage.",
            )
        try:
            structured_logger.info(
                event="notification_dispatch_started",
                status=LogStatus.STARTED,
                execution_id=execution_id,
                message="Notification dispatch started.",
                metadata={"notification_engine": notification_engine},
            )
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
            structured_logger.info(
                event="notification_dispatch_succeeded",
                status=LogStatus.SUCCEEDED,
                execution_id=execution_id,
                message="Notification dispatch completed.",
                metadata={
                    "success_count": success_count,
                    "failed_count": failed_count,
                    "retries_count": retries_count,
                },
            )
        except IngestNotificationError as e:
            structured_logger.error(
                event="notification_dispatch_failed",
                status=LogStatus.FAILED,
                execution_id=execution_id,
                message="Ingest notification failed.",
                error_type=type(e).__name__,
                error_message=str(e),
            )
            metrics = getattr(e, "metrics", None)
            try:
                success_count, failed_count, retries_count = _parse_notification_metrics(
                    metrics
                )
                items_total += success_count + failed_count
                items_failed += failed_count
                retries_seen += retries_count
            except (KeyError, TypeError) as metrics_error:
                structured_logger.error(
                    event="notification_metrics_invalid",
                    status=LogStatus.FAILED,
                    execution_id=execution_id,
                    message="Invalid ingest notification metrics contract.",
                    error_type=type(metrics_error).__name__,
                    error_message=str(metrics_error),
                )
                items_failed += 1
            _emit_failure_alert("ingest_notification")
        except Exception as e:
            structured_logger.error(
                event="notification_dispatch_failed",
                status=LogStatus.FAILED,
                execution_id=execution_id,
                message="Unexpected ingest notification error.",
                error_type=type(e).__name__,
                error_message=str(e),
            )
            items_failed += 1
            _emit_failure_alert("unknown")

    if items_failed > 0:
        structured_logger.error(
            event="execution_failed_non_recoverable",
            status=LogStatus.FAILED,
            execution_id=execution_id,
            message="Execution finished with non-recoverable failures.",
            metadata={"items_total": items_total, "items_failed": items_failed, "retries_seen": retries_seen},
        )
        return
    if retries_seen > 0:
        _emit_final_summary("WARN")
    else:
        _emit_final_summary("PASS")
    structured_logger.info(
        event="execution_completed",
        status=LogStatus.SUCCEEDED,
        execution_id=execution_id,
        message="extractloadlivedata execution completed successfully.",
        metadata={"items_total": items_total, "items_failed": items_failed, "retries_seen": retries_seen},
    )

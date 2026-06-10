from src.services.trigger_airflow import (
    get_utc_logical_date_from_file,
)
from src.services.exceptions import (
    IngestNotificationError,
    LocalIngestBufferSaveError,
    PositionsDownloadError,
    SavePositionsToRawError,
)
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
import time

from src.domain.events import (
    EVENT_STATUS_FAILED,
    EVENT_STATUS_STARTED,
    EVENT_STATUS_SUCCEEDED,
    EVENT_STATUS_WITH_FAILURES,
)
from src.orchestration_dependencies import Services, ConfigDict
from src.quality.reporting import build_execution_report_metadata
from src.observability.process_structured_logger import get_structured_logger
from src.observability.structured_event_logger import (
    clear_execution_context,
    set_execution_context,
)

structured_logger = get_structured_logger(
    service="extractloadlivedata",
    component="orchestrator",
    logger_name=__name__,
)
SEVERE_NON_RECOVERABLE_FAILURE_MESSAGE_PREFIX = "[SEVERE] non recoverable "


def _get_config_values(config: ConfigDict) -> Tuple[str, str]:
    ingest_buffer_folder = config["INGEST_BUFFER_PATH"]
    notification_engine = config["NOTIFICATION_ENGINE"]
    if notification_engine is None:
        raise KeyError("NOTIFICATION_ENGINE configuration is missing.")
    notification_engine = notification_engine.strip()
    return ingest_buffer_folder, notification_engine


def extractloadlivedata(
    config: ConfigDict,
    services: Services,
) -> None:
    def _parse_notification_metrics(metrics: Any) -> Tuple[int, int, int]:
        if not isinstance(metrics, dict):
            raise TypeError("notification metrics must be a dict")
        success = metrics["success"]
        failed = metrics["failed"]
        retries = metrics["retries"]
        if not isinstance(success, int) or success < 0:
            raise TypeError(
                "notification metrics['success'] must be a non-negative int"
            )
        if not isinstance(failed, int) or failed < 0:
            raise TypeError("notification metrics['failed'] must be a non-negative int")
        if not isinstance(retries, int) or retries < 0:
            raise TypeError(
                "notification metrics['retries'] must be a non-negative int"
            )
        return success, failed, retries

    def _phase_message(phase: str) -> str:
        if phase == "positions_download":
            return f"{SEVERE_NON_RECOVERABLE_FAILURE_MESSAGE_PREFIX}api get failed"
        if phase == "local_ingest_buffer_save_positions":
            return f"{SEVERE_NON_RECOVERABLE_FAILURE_MESSAGE_PREFIX}save to local buffer failed"
        if phase == "save_positions_to_object_storage":
            return "save to object storage failed"
        if phase == "ingest_notification":
            return "ingest notification failed"
        return "ingest execution failed"

    def _handle_failure(failure_phase: str) -> None:
        failure_message = _phase_message(failure_phase)
        _severe = {"positions_download", "local_ingest_buffer_save_positions"}
        log = structured_logger.critical if failure_phase in _severe else structured_logger.error
        log(
            event="phase_failure_recorded",
            status=EVENT_STATUS_FAILED,
            message=failure_message,
            metadata={
                "failure_phase": failure_phase,
                "items_total": items_total,
                "items_failed": items_failed,
                "retries_seen": retries_seen,
            },
        )
    execution_id = datetime.now(timezone.utc).isoformat()
    set_execution_context(execution_id)
    execution_start = time.time()
    structured_logger.info(
        event="execution_started",
        status=EVENT_STATUS_STARTED,
        message="extractloadlivedata execution started.",
    )
    items_total = 0
    items_failed = 0
    retries_seen = 0
    pending_object_storage_save_files_count = 0
    pending_ingest_notifications_count = 0
    phase_metrics: Dict[str, Dict[str, int]] = {
        "extract": {"attempted": 0, "succeeded": 0, "failed": 0},
        "local_save": {"attempted": 0, "succeeded": 0, "failed": 0},
        "object_storage_save": {"attempted": 0, "succeeded": 0, "failed": 0},
        "notify": {"attempted": 0, "succeeded": 0, "failed": 0},
    }
    phase_durations: Dict[str, float] = {}
    failed_phases: list[str] = []
    logical_datetime: Optional[str] = None
    worked_correlation_ids: list[str] = []
    try:
        ingest_buffer_folder, notification_engine = _get_config_values(
            config
        )
        structured_logger.debug(
            event="config_validation_succeeded",
            status=EVENT_STATUS_SUCCEEDED,
            message="Runtime configuration validation succeeded.",
        )
    except Exception as e:
        structured_logger.error(
            event="config_validation_failed",
            status=EVENT_STATUS_FAILED,
            message="Runtime configuration validation failed.",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        structured_logger.error(
            event="execution_aborted",
            status=EVENT_STATUS_FAILED,
            message="Execution aborted: configuration validation failed.",
            metadata={
                "failed_phases": ["config_validation"],
                "error_type": type(e).__name__,
                "error_message": str(e),
            },
        )
        clear_execution_context()
        return
    structured_logger.info(
        event="notification_engine_selected",
        status=EVENT_STATUS_SUCCEEDED,
        message="Notification engine selected.",
        metadata={"notification_engine": notification_engine},
    )
    download_successful = False
    extract_start = time.time()
    try:
        structured_logger.info(
            event="extract_positions_started",
            status=EVENT_STATUS_STARTED,
            message="Bus positions extraction started.",
        )
        phase_metrics["extract"]["attempted"] = 1
        items_total += 1
        download_result = services.extract_buses_positions_with_retries(
            config, with_metrics=True
        )
        buses_positions_payload = download_result["result"]
        retries_seen = int(download_result["metrics"]["retries"])
        download_successful = buses_positions_payload is not None
        structured_logger.info(
            event="extract_positions_succeeded",
            status=EVENT_STATUS_SUCCEEDED,
            message="Bus positions extraction succeeded.",
            metadata={
                "download_successful": download_successful,
                "retries_seen": retries_seen,
            },
        )
    except PositionsDownloadError:
        items_failed += 1
        phase_metrics["extract"]["failed"] = 1
        failed_phases.append("positions_download")
        _handle_failure("positions_download")
    except Exception as e:
        structured_logger.error(
            event="extract_positions_failed",
            status=EVENT_STATUS_FAILED,
            message="Unexpected bus positions extraction failure.",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        items_failed += 1
        phase_metrics["extract"]["failed"] = 1
        failed_phases.append("positions_download")
        _handle_failure("positions_download")
    finally:
        phase_durations["extract"] = time.time() - extract_start
    if download_successful:
        try:
            items_total += 1
            buses_positions, _ = services.get_buses_positions_with_metadata(
                buses_positions_payload
            )
            logical_datetime = buses_positions["metadata"]["extracted_at"]
            if logical_datetime:
                worked_correlation_ids.append(logical_datetime)
            phase_metrics["extract"]["succeeded"] = 1
        except Exception as e:
            structured_logger.error(
                event="local_ingest_buffer_phase_failed",
                status=EVENT_STATUS_FAILED,
                correlation_id=logical_datetime,
                message="Unexpected local buffer persistence failure.",
                error_type=type(e).__name__,
                error_message=str(e),
            )
            items_failed += 1
            failed_phases.append("positions_download")
            _handle_failure("positions_download")
        if phase_metrics["extract"]["succeeded"] == 1:
            local_save_start = time.time()
            try:
                phase_metrics["local_save"]["attempted"] = 1
                structured_logger.debug(
                    event="local_storage_persist_started",
                    status=EVENT_STATUS_STARTED,
                    correlation_id=logical_datetime,
                    message="Starting storage persistence for current extraction payload.",
                )
                services.save_bus_positions_to_local_volume(config, buses_positions)
                phase_metrics["local_save"]["succeeded"] = 1
                structured_logger.info(
                    event="local_storage_persist_succeeded",
                    status=EVENT_STATUS_SUCCEEDED,
                    correlation_id=logical_datetime,
                    message="Storage persistence completed for current extraction payload.",
                )
            except LocalIngestBufferSaveError:
                items_failed += 1
                phase_metrics["local_save"]["failed"] = 1
                failed_phases.append("local_ingest_buffer_save_positions")
                _handle_failure("local_ingest_buffer_save_positions")
            except Exception as e:
                structured_logger.error(
                    event="local_ingest_buffer_phase_failed",
                    status=EVENT_STATUS_FAILED,
                    correlation_id=logical_datetime,
                    message="Unexpected local buffer persistence failure.",
                    error_type=type(e).__name__,
                    error_message=str(e),
                )
                items_failed += 1
                phase_metrics["local_save"]["failed"] = 1
                failed_phases.append("local_ingest_buffer_save_positions")
                _handle_failure("local_ingest_buffer_save_positions")
            finally:
                phase_durations["local_save"] = time.time() - local_save_start
    try:
        pending_storage_save_list = services.get_pending_storage_save_list(config)
        pending_object_storage_save_files_count = len(pending_storage_save_list)
        structured_logger.debug(
            event="pending_storage_scan_succeeded",
            status=EVENT_STATUS_SUCCEEDED,
            message="Pending storage scan completed.",
            metadata={"pending_files_count": len(pending_storage_save_list)},
        )
    except Exception as e:
        structured_logger.error(
            event="pending_storage_scan_failed",
            status=EVENT_STATUS_FAILED,
            message="Failed to list pending storage save files.",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        items_failed += 1
        failed_phases.append("save_positions_to_object_storage")
        _handle_failure("save_positions_to_object_storage")
        return
    if pending_storage_save_list:
        if len(pending_storage_save_list) > 1:
            structured_logger.warning(
                event="pending_storage_multiple_files_detected",
                status=EVENT_STATUS_STARTED,
                message="Multiple pending files detected for storage save. This may indicate a recurring failure pattern that should be investigated.",
                metadata={"pending_files_count": len(pending_storage_save_list)},
            )
        else:
            structured_logger.info(
                event="pending_storage_detected",
                status=EVENT_STATUS_STARTED,
                message="Pending files detected for storage save.",
                metadata={
                    "pending_files_count": len(pending_storage_save_list),
                    "pending_files": pending_storage_save_list,
                },
            )
        save_start = time.time()
        try:
            for pending_storage_save_file in pending_storage_save_list:
                items_total += 1
                phase_metrics["object_storage_save"]["attempted"] += 1
                file_logical_datetime = get_utc_logical_date_from_file(
                    pending_storage_save_file
                )
                if file_logical_datetime:
                    worked_correlation_ids.append(file_logical_datetime)
                structured_logger.debug(
                    event="pending_storage_file_started",
                    status=EVENT_STATUS_STARTED,
                    correlation_id=file_logical_datetime,
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
                        event="object_storage_persist_started",
                        status=EVENT_STATUS_STARTED,
                        correlation_id=file_logical_datetime,
                        message="Starting object storage persistence for pending file payload.",
                        metadata={"pending_file": pending_storage_save_file},
                    )
                    save_result = services.save_bus_positions_to_storage_with_retries(
                        config, pending_storage_save_file_content, with_metrics=True
                    )
                    retries_seen = int(save_result["metrics"]["retries"])
                    structured_logger.debug(
                        event="object_storage_persist_succeeded",
                        status=EVENT_STATUS_SUCCEEDED,
                        correlation_id=file_logical_datetime,
                        message="Object storage persistence completed for pending file payload.",
                        metadata={"pending_file": pending_storage_save_file},
                    )
                    structured_logger.info(
                        event="pending_storage_file_succeeded",
                        status=EVENT_STATUS_SUCCEEDED,
                        correlation_id=file_logical_datetime,
                        message="Pending file saved to storage successfully.",
                        metadata={
                            "pending_file": pending_storage_save_file,
                            "retries_seen": retries_seen,
                        },
                    )
                    phase_metrics["object_storage_save"]["succeeded"] += 1
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
                    retries_seen += int(getattr(e, "retries", 0))
                    items_failed += 1
                    phase_metrics["object_storage_save"]["failed"] += 1
                    failed_phases.append("save_positions_to_object_storage")
                    _handle_failure("save_positions_to_object_storage")
                    break
                except Exception as e:
                    structured_logger.error(
                        event="pending_storage_file_failed",
                        status=EVENT_STATUS_FAILED,
                        correlation_id=file_logical_datetime,
                        message="Unexpected pending file processing error.",
                        error_type=type(e).__name__,
                        error_message=str(e),
                        metadata={"pending_file": pending_storage_save_file},
                    )
                    items_failed += 1
                    phase_metrics["object_storage_save"]["failed"] += 1
                    failed_phases.append("save_positions_to_object_storage")
                    _handle_failure("save_positions_to_object_storage")
        finally:
            phase_durations["object_storage_save"] = time.time() - save_start
        try:
            if notification_engine == "airflow":
                pending_notifications_list = services.get_pending_invokations(config)
            else:
                pending_notifications_list = services.get_pending_processing_requests(config)
            pending_ingest_notifications_count = len(pending_notifications_list)
        except Exception as e:
            structured_logger.error(
                event="pending_notifications_scan_failed",
                status=EVENT_STATUS_FAILED,
                message="Failed to scan pending notifications list.",
                error_type=type(e).__name__,
                error_message=str(e),
            )
        notify_start = time.time()
        try:
            structured_logger.info(
                event="notification_dispatch_started",
                status=EVENT_STATUS_STARTED,
                correlation_id=logical_datetime,
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
            phase_metrics["notify"]["attempted"] = success_count + failed_count
            phase_metrics["notify"]["succeeded"] = success_count
            phase_metrics["notify"]["failed"] = failed_count
            structured_logger.info(
                event="notification_dispatch_succeeded",
                status=EVENT_STATUS_SUCCEEDED,
                correlation_id=logical_datetime,
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
                status=EVENT_STATUS_FAILED,
                correlation_id=logical_datetime,
                message="Ingest notification failed.",
                error_type=type(e).__name__,
                error_message=str(e),
            )
            metrics = getattr(e, "metrics", None)
            try:
                success_count, failed_count, retries_count = (
                    _parse_notification_metrics(metrics)
                )
                items_total += success_count + failed_count
                items_failed += failed_count
                retries_seen += retries_count
                phase_metrics["notify"]["attempted"] = success_count + failed_count
                phase_metrics["notify"]["succeeded"] = success_count
                phase_metrics["notify"]["failed"] = failed_count
            except (KeyError, TypeError) as metrics_error:
                structured_logger.error(
                    event="notification_metrics_invalid",
                    status=EVENT_STATUS_FAILED,
                    message="Invalid ingest notification metrics contract.",
                    error_type=type(metrics_error).__name__,
                    error_message=str(metrics_error),
                )
                items_failed += 1
            failed_phases.append("ingest_notification")
            _handle_failure("ingest_notification")
        except Exception as e:
            structured_logger.error(
                event="notification_dispatch_failed",
                status=EVENT_STATUS_FAILED,
                message="Unexpected ingest notification error.",
                error_type=type(e).__name__,
                error_message=str(e),
            )
            items_failed += 1
            failed_phases.append("ingest_notification")
            _handle_failure("ingest_notification")
        finally:
            phase_durations["notify"] = time.time() - notify_start
    execution_end = time.time()
    execution_seconds = execution_end - execution_start
    items_total = sum(phase_metrics[op]["attempted"] for op in phase_metrics)
    items_failed = sum(phase_metrics[op]["failed"] for op in phase_metrics)
    execution_report_metadata = build_execution_report_metadata(
        execution_seconds=execution_seconds,
        items_total=items_total,
        items_failed=items_failed,
        retries_seen=retries_seen,
        worked_correlation_ids=worked_correlation_ids,
        phase_metrics=phase_metrics,
        phase_durations=phase_durations,
        logical_datetime=logical_datetime,
        pending_object_storage_save_files_count=pending_object_storage_save_files_count,
        pending_ingest_notifications_count=pending_ingest_notifications_count,
    )
    structured_logger.info(
        event="execution_metrics_final",
        status=EVENT_STATUS_SUCCEEDED,
        message="Execution metrics finalized.",
        metadata=execution_report_metadata,
    )

    if bool(failed_phases):
        structured_logger.info(
            event="execution_completed",
            status=EVENT_STATUS_WITH_FAILURES,
            message="extractloadlivedata execution completed with failures.",
            metadata={**execution_report_metadata, "failed_phases": failed_phases},
        )
    else:
        structured_logger.info(
            event="execution_completed",
            status=EVENT_STATUS_SUCCEEDED,
            message="extractloadlivedata execution completed successfully.",
            metadata=execution_report_metadata,
        )
    clear_execution_context()

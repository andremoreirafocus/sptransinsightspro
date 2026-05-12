"""Application-level logging taxonomy for extractloadlivedata."""

from enum import Enum

class LogStatus(str, Enum):
    STARTED = "STARTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    RETRY = "RETRY"
    SKIPPED = "SKIPPED"

ALLOWED_STATUSES = {
    LogStatus.STARTED.value,
    LogStatus.SUCCEEDED.value,
    LogStatus.FAILED.value,
    LogStatus.RETRY.value,
    LogStatus.SKIPPED.value,
}

ALLOWED_EVENTS = {
    "scheduler_tick_started",
    "scheduler_tick_completed",
    "scheduler_config_loaded",
    "scheduler_started",
    "scheduler_stopped",
    "scheduler_shutdown_completed",
    "cli_dev_mode_requested",
    "cli_invalid_parameter",
    "config_validation_started",
    "config_validation_succeeded",
    "config_validation_failed",
    "execution_started",
    "execution_completed",
    "notification_engine_selected",
    "extract_positions_started",
    "extract_positions_failed",
    "extract_positions_succeeded",
    "pending_storage_scan_failed",
    "pending_storage_scan_succeeded",
    "pending_storage_detected",
    "pending_storage_file_started",
    "pending_storage_file_succeeded",
    "pending_storage_file_failed",
    "storage_persist_started",
    "storage_persist_failed",
    "storage_persist_succeeded",
    "notification_dispatch_started",
    "notification_dispatch_failed",
    "notification_dispatch_succeeded",
    "notification_metrics_invalid",
    "execution_summary_emitted",
    "execution_failed_non_recoverable",
}

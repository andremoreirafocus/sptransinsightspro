"""Application-level logging taxonomy for extractloadlivedata."""

from typing import Final, Literal, TypeAlias

LOG_LEVEL_DEBUG: Final = "DEBUG"
LOG_LEVEL_INFO: Final = "INFO"
LOG_LEVEL_WARNING: Final = "WARNING"
LOG_LEVEL_ERROR: Final = "ERROR"
LOG_LEVEL_CRITICAL: Final = "CRITICAL"

EVENT_STATUS_STARTED: Final = "STARTED"
EVENT_STATUS_SUCCEEDED: Final = "SUCCEEDED"
EVENT_STATUS_FAILED: Final = "FAILED"
EVENT_STATUS_RETRY: Final = "RETRY"
EVENT_STATUS_SKIPPED: Final = "SKIPPED"

LogLevel: TypeAlias = Literal[
    "DEBUG",
    "INFO",
    "WARNING",
    "ERROR",
    "CRITICAL",
]
LogStatusType: TypeAlias = Literal[
    "STARTED",
    "SUCCEEDED",
    "FAILED",
    "RETRY",
    "SKIPPED",
]
LogEventType: TypeAlias = Literal[
    "api_authentication_successful",
    "api_authentication_failed",
    "api_get_started",
    "api_get_successful",
    "api_get_failed",
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
    "extract_positions_succeeded_after_retries",
    "summarize_extracted_positions_succeeded",
    "pending_storage_scan_failed",
    "pending_storage_scan_succeeded",
    "pending_storage_detected",
    "pending_storage_file_started",
    "pending_storage_file_succeeded",
    "pending_storage_file_failed",
    "remove_pending_storage_file_succeeded",
    "remove_pending_storage_file_failed",
    "pending_storage_multiple_files_detected",
    "local_storage_persist_started",
    "local_storage_compression_succeeded",
    "local_storage_persist_succeeded",
    "local_storage_persist_failed",
    "object_storage_persist_started",
    "object_storage_persist_succeeded",
    "object_storage_persist_failed",
    "object_storage_list_failed",
    "object_storage_compression_started",
    "object_storage_compression_succeeded",
    "storage_persist_started",
    "storage_persist_failed",
    "storage_persist_succeeded",
    "metadata_validation_failed",
    "notification_dispatch_started",
    "notification_dispatch_failed",
    "notification_dispatch_succeeded",
    "notification_metrics_invalid",
    "execution_metrics_final",
    "execution_summary_emitted",
    "execution_failed_non_recoverable",
    "get_utc_logical_date_succeeded",
    "get_utc_logical_date_failed",
    "db_storage_persist_started",
    "db_storage_persist_succeeded",
    "db_storage_persist_failed",
    
]

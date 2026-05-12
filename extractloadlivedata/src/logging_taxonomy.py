"""Application-level logging taxonomy for extractloadlivedata."""

ALLOWED_STATUSES = {
    "STARTED",
    "SUCCEEDED",
    "FAILED",
    "RETRY",
    "SKIPPED",
}

ALLOWED_EVENTS = {
    "scheduler_tick_started",
    "scheduler_tick_completed",
    "config_validation_started",
    "config_validation_failed",
    "extract_positions_started",
    "extract_positions_retry",
    "extract_positions_failed",
    "extract_positions_succeeded",
    "storage_persist_started",
    "storage_persist_failed",
    "storage_persist_succeeded",
    "notification_dispatch_started",
    "notification_dispatch_failed",
    "notification_dispatch_succeeded",
    "execution_summary_emitted",
    "execution_failed_non_recoverable",
}


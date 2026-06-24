"""Application-level logging taxonomy for extractloadlivedata.

Events are split into two categories:
- ``_OrchestratorEvent``: emitted only by ``extractloadlivedata.py`` (execution flow,
  phase coordination, cross-cutting concerns).
- ``_ServiceEvent``: emitted only by service files under ``src/services/``
  (domain/infrastructure operations).

``EventType`` is the public alias — the union of both — and is what callers use.
"""

from typing import Final, Literal, TypeAlias, Union

EVENT_STATUS_STARTED: Final = "STARTED"
EVENT_STATUS_SUCCEEDED: Final = "SUCCEEDED"
EVENT_STATUS_FAILED: Final = "FAILED"
EVENT_STATUS_RETRY: Final = "RETRY"
EVENT_STATUS_SKIPPED: Final = "SKIPPED"
EVENT_STATUS_WITH_FAILURES: Final = "WITH_FAILURES"

_OrchestratorEvent: TypeAlias = Literal[
    # Execution lifecycle
    "execution_started",
    "execution_completed",
    "execution_metrics_final",
    "execution_aborted",
    "phase_failure_recorded",
    # Configuration
    "config_validation_started",
    "config_validation_succeeded",
    "config_validation_failed",
    "notification_engine_selected",
    # Extract phase (orchestrator-level annotations)
    "extract_positions_started",
    "extract_positions_succeeded",
    # Pending storage scan + loop coordination
    "pending_storage_scan_failed",
    "pending_storage_scan_succeeded",
    "pending_storage_detected",
    "pending_storage_multiple_files_detected",
    "pending_storage_file_started",
    "pending_storage_file_succeeded",
    "pending_storage_file_failed",
    # Local buffer phase
    "local_storage_persist_succeeded",
    "local_ingest_buffer_phase_failed",
    # Object storage phase
    "object_storage_persist_started",
    "object_storage_persist_succeeded",
    # Notification dispatch (orchestrator-owned coordination)
    "notification_dispatch_started",
    "notification_dispatch_succeeded",
    "notification_dispatch_failed",
    "notification_metrics_invalid",
]

_ServiceEvent: TypeAlias = Literal[
    # SPTrans API
    "api_authentication_successful",
    "api_authentication_failed",
    "api_get_started",
    "api_get_successful",
    "api_get_failed",
    # Scheduler / CLI infrastructure
    "scheduler_tick_started",
    "scheduler_tick_completed",
    "scheduler_config_loaded",
    "scheduler_started",
    "scheduler_stopped",
    "scheduler_shutdown_completed",
    "cli_dev_mode_requested",
    "cli_invalid_parameter",
    # Extract service outcomes
    "extract_positions_failed",
    "extract_positions_succeeded_after_retries",
    "summarize_extracted_positions_succeeded",
    # Local storage service
    "local_storage_persist_started",
    "local_storage_compression_succeeded",
    "local_storage_persist_failed",
    # Object storage service
    "object_storage_compression_started",
    "object_storage_compression_succeeded",
    "object_storage_persist_succeeded_after_retries",
    "object_storage_persist_failed",
    "object_storage_list_failed",
    # Validation
    "metadata_validation_failed",
    # Pending file removal
    "remove_pending_storage_file_succeeded",
    "remove_pending_storage_file_failed",
    # DB / processing request service
    "db_processing_request_started",
    "db_processing_request_failed",
    "db_storage_persist_started",
    "db_storage_persist_succeeded",
    "db_storage_persist_failed",
    # Airflow invocation service
    "airflow_invocation_cache_started",
    "airflow_invocation_cache_succeeded",
    "airflow_invocation_scan_succeeded",
    "airflow_invocation_detected",
    # UTC date extraction
    "get_utc_logical_date_succeeded",
    "get_utc_logical_date_failed",
]

EventType: TypeAlias = Union[_OrchestratorEvent, _ServiceEvent]

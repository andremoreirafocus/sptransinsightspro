from typing import Literal, Union

from typing_extensions import TypeAlias

_OrchestratorEvent: TypeAlias = Literal[
    "execution_aborted",
    "execution_finished",
    "execution_phase_metrics",
    "execution_started",
    "webhook_notification_disabled",
    "webhook_notification_failed",
    "webhook_notification_sent",
]
_ServiceEvent: TypeAlias = Literal[
    "buffer_save_failed",
    "buffer_save_started",
    "buffer_save_succeeded",
    "csv_load_failed",
    "csv_load_started",
    "csv_load_succeeded",
    "file_relocation_completed",
    "file_relocation_item_failed",
    "file_relocation_started",
    "gtfs_extraction_failed",
    "gtfs_extraction_started",
    "gtfs_extraction_succeeded",
    "raw_file_validation_error",
    "raw_files_upload_started",
    "raw_files_upload_succeeded",
    "raw_validation_completed",
    "raw_validation_started",
    "table_csv_load_failed",
    "table_csv_parse_failed",
    "table_staging_failed",
    "table_transform_started",
    "table_transform_succeeded",
    "table_validation_failed",
    "table_validation_skipped",
    "table_validation_started",
    "trip_details_creation_failed",
    "trip_details_creation_started",
    "trip_details_creation_succeeded",
]
EventType: TypeAlias = Union[_OrchestratorEvent, _ServiceEvent]

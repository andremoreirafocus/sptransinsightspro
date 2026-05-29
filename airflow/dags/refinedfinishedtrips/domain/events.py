"""Application-level logging taxonomy for refinedfinishedtrips."""

from typing import Literal, TypeAlias, Union

_OrchestratorEvent: TypeAlias = Literal[
    "config_load_started",
    "config_load_succeeded",
    "dataset_trigger_received",
    "execution_aborted",
    "execution_finished",
    "execution_phase_metrics",
    "execution_started",
    "failure_report_error",
    "failure_report_skipped",
    "persistence_started",
    "persistence_succeeded",
    "persistence_warned",
    "positions_load_started",
    "positions_load_succeeded",
    "positions_quality_failed",
    "positions_quality_started",
    "positions_quality_succeeded",
    "positions_quality_warned",
    "quality_report_failed",
    "quality_report_metrics",
    "quality_report_started",
    "quality_report_succeeded",
    "trip_extraction_started",
    "trip_extraction_succeeded",
    "trip_extraction_warned",
]

_ServiceEvent: TypeAlias = Literal[
    "failure_quality_report_saved",
    "final_quality_report_saved",
    "freshness_evaluation",
    "low_trip_count_evaluation",
    "positions_quality_validated",
    "positions_query_completed",
    "positions_query_failed",
    "positions_query_started",
    "recent_gaps_evaluation",
    "trip_extraction_completed",
    "trip_extraction_progress",
    "trips_filtered",
    "trips_quality_validated",
    "trips_save_failed",
    "trips_save_started",
    "trips_saved",
    "vehicle_trip_extraction_failed",
    "zero_trips_evaluation",
]

EventType: TypeAlias = Union[_OrchestratorEvent, _ServiceEvent]

"""Application-level logging taxonomy for refinedfinishedtrips."""

from typing import Literal, TypeAlias

_OrchestratorEvent: TypeAlias = Literal[
    "config_load_failed",
    "config_load_started",
    "config_load_succeeded",
    "execution_failed",
    "execution_finished",
    "execution_phase_metrics",
    "execution_started",
    "failure_report_error",
    "failure_report_skipped",
    "persistence_failed",
    "persistence_started",
    "persistence_succeeded",
    "persistence_warned",
    "positions_load_failed",
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
    "trip_extraction_failed",
    "trip_extraction_started",
    "trip_extraction_succeeded",
    "trip_extraction_warned",
]

EventType: TypeAlias = _OrchestratorEvent

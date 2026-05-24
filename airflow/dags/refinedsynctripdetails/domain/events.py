"""Application-level logging taxonomy for refinedsynctripdetails."""

from typing import Literal, TypeAlias, Union

_OrchestratorEvent: TypeAlias = Literal[
    "config_load_started",
    "config_load_succeeded",
    "execution_aborted",
    "execution_finished",
    "execution_phase_metrics",
    "execution_started",
]

_ServiceEvent: TypeAlias = Literal[
    "trip_details_load_failed",
    "trip_details_load_started",
    "trip_details_load_succeeded",
    "trip_details_save_failed",
    "trip_details_save_started",
    "trip_details_save_succeeded",
    "trip_details_transform_failed",
    "trip_details_transform_skipped",
    "trip_details_transform_started",
    "trip_details_transform_succeeded",
]

EventType: TypeAlias = Union[_OrchestratorEvent, _ServiceEvent]

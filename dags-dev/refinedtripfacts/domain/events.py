"""Application-level logging taxonomy for refinedtripfacts."""

from typing import Literal, TypeAlias, Union

_OrchestratorEvent: TypeAlias = Literal[
    "config_load_started",
    "config_load_succeeded",
    "input_trips_measurement_started",
    "input_trips_measurement_succeeded",
    "dim_time_provisioning_started",
    "dim_time_provisioning_succeeded",
    "trip_facts_creation_started",
    "trip_facts_creation_succeeded",
    "trip_facts_verification_started",
    "trip_facts_verification_succeeded",
    "data_quality_validation_started",
    "data_quality_validation_succeeded",
    "quality_report_started",
    "quality_report_succeeded",
    "dataset_trigger_received",
    "execution_started",
    "execution_finished",
    "execution_aborted",
    "execution_phase_metrics",
    "quality_report_metrics",
    "failure_report_error",
    "failure_report_skipped",
]

_ServiceEvent: TypeAlias = Literal[
    "input_trips_measurement_failed",
    "dim_time_provisioning_failed",
    "trip_facts_creation_failed",
    "persisted_facts_measurement_failed",
    "quality_report_failed",
    "final_quality_report_saved",
    "failure_quality_report_saved",
]

EventType: TypeAlias = Union[_OrchestratorEvent, _ServiceEvent]

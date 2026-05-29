"""Application-level logging taxonomy for updatelatestpositions."""

from typing import Literal, TypeAlias, Union

_OrchestratorEvent: TypeAlias = Literal[
    "config_load_started",
    "config_load_succeeded",
    "dataset_trigger_received",
    "execution_aborted",
    "execution_finished",
    "execution_phase_metrics",
    "execution_started",
]

_ServiceEvent: TypeAlias = Literal[
    "freshness_evaluation",
    "path_discovery_empty",
    "path_discovery_failed",
    "path_discovery_started",
    "path_discovery_succeeded",
    "positions_query_started",
    "positions_query_succeeded",
    "positions_save_started",
    "positions_save_succeeded",
    "positions_update_failed",
    "positions_update_skipped",
    "prefix_scan_started",
]

EventType: TypeAlias = Union[_OrchestratorEvent, _ServiceEvent]

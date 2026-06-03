from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from quality.execution_phase_metrics import ExecutionPhaseMetricsTracker
from refinedfinishedtrips.domain.logger import RefinedFinishedTripsLogger
from refinedfinishedtrips.orchestration_dependencies import (
    RefinedFinishedTripsOrchestrationDependencies,
)


@dataclass
class PipelineTaskRunState:
    execution_id: str
    correlation_id: str
    run_ts: datetime
    pipeline_config: dict[str, Any] | None = None
    positions_result: dict[str, Any] | None = None
    trips_result: dict[str, Any] | None = None
    persistence_result: dict[str, Any] | None = None
    column_lineage: dict[str, Any] | None = None
    extraction_metrics: dict[str, Any] = field(default_factory=dict)


def handle_phase_metrics_event(
    state: PipelineTaskRunState,
    tracker: ExecutionPhaseMetricsTracker,
    structured_logger: RefinedFinishedTripsLogger,
    overall_status: str,
) -> None:
    emit = structured_logger.info if overall_status == "success" else structured_logger.error
    emit(
        event="execution_phase_metrics",
        message="Execution phase metrics",
        status="SUCCEEDED" if overall_status == "success" else "FAILED",
        metadata=tracker.to_log_payload(overall_status),
    )


def handle_finishedtrips_ready_dataset_emission(logic_date_str: str) -> None:
    # Actual Airflow Dataset outlet emission is performed by the DAG wrapper via outlet_events.
    # This handler is the reference implementation for non-Airflow contexts.
    pass


def handle_failure_event(
    state: PipelineTaskRunState,
    deps: RefinedFinishedTripsOrchestrationDependencies,
    structured_logger: RefinedFinishedTripsLogger,
    phase: str,
    message: str,
) -> None:
    if state.pipeline_config is None:
        structured_logger.error(
            event="failure_report_skipped",
            message="Failed to write quality report on failure: pipeline_config is not available",
            status="FAILED",
        )
        return
    try:
        result = deps.create_failure_quality_report(
            config=state.pipeline_config,
            execution_id=state.execution_id,
            run_ts=state.run_ts,
            failure_phase=phase,
            failure_message=message,
            positions_result=state.positions_result,
            trips_result=state.trips_result,
            persistence_result=state.persistence_result,
            column_lineage=state.column_lineage,
        )
        structured_logger.info(
            event="quality_report_metrics",
            message="Quality report metrics",
            status="FAILED",
            metadata=result["summary"],
        )
    except Exception as e:
        structured_logger.error(
            event="failure_report_error",
            message="Failed to write quality report on failure",
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )

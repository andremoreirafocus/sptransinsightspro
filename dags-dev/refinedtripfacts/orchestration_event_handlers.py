from dataclasses import dataclass
from datetime import datetime
from typing import Any

from quality.execution_phase_metrics import ExecutionPhaseMetricsTracker
from refinedtripfacts.domain.logger import RefinedTripFactsLogger
from refinedtripfacts.orchestration_dependencies import RefinedTripFactsOrchestrationDependencies


@dataclass
class PipelineTaskRunState:
    execution_id: str
    correlation_id: str
    run_ts: datetime
    pipeline_config: dict[str, Any] | None = None
    measurement_result: dict[str, Any] | None = None
    dim_time_result: dict[str, Any] | None = None
    creation_result: dict[str, Any] | None = None
    persisted_metrics: dict[str, Any] | None = None
    quality_result: dict[str, Any] | None = None
    column_lineage: dict[str, Any] | None = None


def handle_phase_metrics_event(
    state: PipelineTaskRunState,
    tracker: ExecutionPhaseMetricsTracker,
    structured_logger: RefinedTripFactsLogger,
    overall_status: str,
) -> None:
    emit = structured_logger.info if overall_status == "success" else structured_logger.error
    emit(
        event="execution_phase_metrics",
        message="Execution phase metrics",
        status="SUCCEEDED" if overall_status == "success" else "FAILED",
        metadata=tracker.to_log_payload(overall_status),
    )


def handle_failure_event(
    state: PipelineTaskRunState,
    deps: RefinedTripFactsOrchestrationDependencies,
    structured_logger: RefinedTripFactsLogger,
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
            quality_result=state.quality_result,
            measurement_result=state.measurement_result,
            dim_time_result=state.dim_time_result,
            creation_result=state.creation_result,
            persisted_metrics=state.persisted_metrics,
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

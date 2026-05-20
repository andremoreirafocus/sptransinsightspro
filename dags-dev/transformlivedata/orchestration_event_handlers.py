from dataclasses import dataclass
from typing import Any

from quality.execution_phase_metrics import ExecutionPhaseMetricsTracker
from transformlivedata.domain.logger import TransformLivedataLogger
from transformlivedata.orchestration_dependencies import (
    TransformLiveDataOrchestrationDependencies,
)


@dataclass
class PipelineTaskRunState:
    execution_id: str
    correlation_id: str
    logical_date_utc: str
    pipeline_config: dict[str, Any] | None = None
    source_file: str = ""
    transform_result: dict[str, Any] | None = None
    expectations_result: dict[str, Any] | None = None
    quarantine_save_status: str = "SKIPPED"
    quarantine_save_error: str | None = None


def handle_phase_metrics_event(
    state: PipelineTaskRunState,
    tracker: ExecutionPhaseMetricsTracker,
    structured_logger: TransformLivedataLogger,
    overall_status: str,
) -> None:
    emit = structured_logger.info if overall_status == "success" else structured_logger.error
    emit(
        event="execution_phase_metrics",
        message="Execution phase metrics",
        execution_id=state.execution_id,
        correlation_id=state.correlation_id,
        status="SUCCEEDED" if overall_status == "success" else "FAILED",
        metadata=tracker.to_log_payload(overall_status),
    )


def handle_failure_event(
    state: PipelineTaskRunState,
    deps: TransformLiveDataOrchestrationDependencies,
    structured_logger: TransformLivedataLogger,
    phase: str,
    message: str,
) -> None:
    if state.pipeline_config is None:
        structured_logger.error(
            event="failure_report_skipped",
            message="Failed to write quality report on failure: pipeline_config is not available",
            execution_id=state.execution_id,
            correlation_id=state.correlation_id,
            status="FAILED",
        )
        return
    try:
        report_log_metadata = deps.create_failure_quality_report(
            config=state.pipeline_config,
            execution_id=state.execution_id,
            logical_date_utc=state.logical_date_utc,
            source_file=state.source_file,
            failure_phase=phase,
            failure_message=message,
            batch_ts=(
                state.transform_result.get("batch_ts")
                if state.transform_result is not None
                else state.logical_date_utc
            ),
            transform_result=state.transform_result,
            expectations_result=state.expectations_result,
            quarantine_save_status=state.quarantine_save_status,
            quarantine_save_error=state.quarantine_save_error,
        )
        structured_logger.info(
            event="quality_report_metrics",
            message="Quality report metrics",
            execution_id=state.execution_id,
            correlation_id=state.correlation_id,
            status="FAILED",
            metadata=report_log_metadata,
        )
    except Exception as e:
        structured_logger.error(
            event="failure_report_error",
            message="Failed to write quality report on failure",
            execution_id=state.execution_id,
            correlation_id=state.correlation_id,
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )

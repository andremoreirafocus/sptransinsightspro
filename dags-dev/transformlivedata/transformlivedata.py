from transformlivedata.config.transformlivedata_config_schema import GeneralConfig
from transformlivedata.config.observability import THIRD_PARTY_LOGGER_NAMESPACES
from transformlivedata.orchestration_dependencies import (
    TransformLiveDataOrchestrationDependencies,
)
from transformlivedata.orchestration_event_handlers import (
    PipelineTaskRunState,
    handle_failure_event,
    handle_phase_metrics_event,
)
from observability.third_party_log_bridge import configure_third_party_log_bridge
from observability.structured_event_logger import get_structured_logger, set_execution_context
from quality.execution_phase_metrics import ExecutionPhaseMetricsTracker
from transformlivedata.domain.logger import TransformLivedataLogger
import pandas as pd
import uuid


def load_transform_save_positions(
    pipeline_name: str,
    logical_date_string: str,
    deps: TransformLiveDataOrchestrationDependencies,
) -> None:
    def create_execution_failed_log_record(message: str) -> None:
        structured_logger.error(
            event="execution_failed",
            message=message,
            status="FAILED",
        )

    state = PipelineTaskRunState(
        execution_id=str(uuid.uuid4()),
        correlation_id=logical_date_string,
        logical_date_utc=logical_date_string,
    )
    set_execution_context(state.execution_id, state.correlation_id)
    _inner_logger = get_structured_logger(
        service=pipeline_name,
        component="orchestrator",
        logger_name=__name__,
    )
    configure_third_party_log_bridge(
        structured_logger=_inner_logger,
        execution_id=state.execution_id,
        correlation_id=state.correlation_id,
        namespaces=THIRD_PARTY_LOGGER_NAMESPACES,
    )
    structured_logger: TransformLivedataLogger = TransformLivedataLogger(_inner_logger)
    phase_order = [
        "config_load",
        "load_positions",
        "raw_schema_validation",
        "transform",
        "expectations_validation",
        "save_trusted",
        "save_quarantine",
        "mark_processed",
        "quality_report",
    ]
    tracker = ExecutionPhaseMetricsTracker(
        pipeline=pipeline_name,
        execution_id=state.execution_id,
        logical_date_utc=logical_date_string,
        phase_order=phase_order,
    )
    structured_logger.info(
        event="config_load_started",
        message="Starting configuration load",
        status="STARTED",
    )
    tracker.begin("config_load")
    try:
        logical_date_context = deps.build_logical_date_context(logical_date_string)
        state.source_file = logical_date_context["source_file"]
        state.pipeline_config = deps.get_config(
            pipeline_name,
            None,
            GeneralConfig,
            None,
            "minio_conn",
            "airflow_postgres_conn",
        )
        tracker.finish("config_load", "success")
        structured_logger.info(
            event="config_load_succeeded",
            message="Configuration load succeeded",
            status="SUCCEEDED",
        )
    except Exception as e:
        tracker.finish("config_load", "failed")
        error_msg = "Configuration load and validation failed"
        structured_logger.error(
            event="config_load_failed",
            message=error_msg,
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_failed_log_record(error_msg)
        raise ValueError("Pipeline configuration validation failed") from e
    structured_logger.info(
        event="execution_started",
        message="Starting execution",
        status="STARTED",
    )
    structured_logger.info(
        event="load_positions_started",
        message="Starting positions load",
        status="STARTED",
    )
    tracker.begin("load_positions")
    try:
        raw_positions = deps.load_positions(
            state.pipeline_config,
            logical_date_context["partition_path"],
            logical_date_context["source_file"],
        )
        structured_logger.info(
            event="load_positions_succeeded",
            message="Positions load succeeded",
            status="SUCCEEDED",
        )
    except Exception as e:
        tracker.finish("load_positions", "failed")
        error_msg = "Load positions failed."
        structured_logger.error(
            event="load_positions_failed",
            message=error_msg,
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        handle_failure_event(state, deps, structured_logger, "load_positions", error_msg)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_failed_log_record(error_msg)
        raise
    if not raw_positions:
        tracker.finish("load_positions", "failed")
        error_msg = "No position data found to transform."
        structured_logger.error(
            event="load_positions_failed",
            message=error_msg,
            status="FAILED",
        )
        handle_failure_event(state, deps, structured_logger, "load_positions", error_msg)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_failed_log_record(error_msg)
        raise ValueError(error_msg)
    tracker.finish("load_positions", "success")
    structured_logger.info(
        event="raw_schema_validation_started",
        message="Starting raw schema validation",
        status="STARTED",
    )
    tracker.begin("raw_schema_validation")
    is_valid, validation_errors = deps.validate_json_data_schema(
        raw_positions, state.pipeline_config["raw_data_json_schema"]
    )
    if not is_valid:
        tracker.finish("raw_schema_validation", "failed")
        error_msg = f"Raw data validation failed: {validation_errors}"
        structured_logger.error(
            event="raw_schema_validation_failed",
            message="Raw schema validation failed",
            status="FAILED",
            error_message=error_msg,
        )
        handle_failure_event(state, deps, structured_logger, "raw_schema_validation", error_msg)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_failed_log_record(error_msg)
        raise ValueError(error_msg)
    tracker.finish("raw_schema_validation", "success")
    structured_logger.info(
        event="raw_schema_validation_succeeded",
        message="Raw schema validation succeeded",
        status="SUCCEEDED",
    )
    structured_logger.info(
        event="transform_started",
        message="Starting transformation",
        status="STARTED",
    )
    tracker.begin("transform")
    try:
        state.transform_result = deps.transform_positions(state.pipeline_config, raw_positions)
        structured_logger.info(
            event="transform_succeeded",
            message="Transformation succeeded",
            status="SUCCEEDED",
        )
    except Exception as e:
        tracker.finish("transform", "failed")
        error_msg = "Transformation failed."
        structured_logger.error(
            event="transform_failed",
            message=error_msg,
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        handle_failure_event(state, deps, structured_logger, "transform", error_msg)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_failed_log_record(error_msg)
        raise
    if (
        not state.transform_result
        or state.transform_result.get("positions") is None
        or state.transform_result["positions"].empty
    ):
        tracker.finish("transform", "failed")
        error_msg = "No valid position records found after transformation."
        structured_logger.error(
            event="transform_failed",
            message=error_msg,
            status="FAILED",
        )
        handle_failure_event(state, deps, structured_logger, "transform", error_msg)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_failed_log_record(error_msg)
        raise ValueError(error_msg)
    tracker.finish("transform", "success")
    positions_df = state.transform_result["positions"]
    structured_logger.info(
        event="expectations_validation_started",
        message="Starting expectations validation",
        status="STARTED",
    )
    tracker.begin("expectations_validation")
    try:
        state.expectations_result = deps.validate_expectations(
            positions_df,
            state.pipeline_config["data_expectations"],
        )
        tracker.finish("expectations_validation", "success")
        structured_logger.info(
            event="expectations_validation_succeeded",
            message="Expectations validation succeeded",
            status="SUCCEEDED",
        )
    except Exception as e:
        tracker.finish("expectations_validation", "failed")
        error_msg = "Expectations validation failed."
        structured_logger.error(
            event="expectations_validation_failed",
            message=error_msg,
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        handle_failure_event(state, deps, structured_logger, "expectations", error_msg)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_failed_log_record(error_msg)
        raise
    valid_positions_df = state.expectations_result["valid_df"]
    invalid_positions_df = state.expectations_result["invalid_df"]
    structured_logger.info(
        event="save_trusted_started",
        message="Starting trusted positions save",
        status="STARTED",
    )
    tracker.begin("save_trusted")
    try:
        deps.save_positions_to_storage(state.pipeline_config, valid_positions_df, "trusted")
        tracker.finish("save_trusted", "success")
        structured_logger.info(
            event="save_trusted_succeeded",
            message="Trusted positions save succeeded",
            status="SUCCEEDED",
            metadata={"records_saved": int(valid_positions_df.shape[0])},
        )
    except Exception as e:
        tracker.finish("save_trusted", "failed")
        error_msg = "Failed to save trusted positions."
        structured_logger.error(
            event="save_trusted_failed",
            message=error_msg,
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        handle_failure_event(state, deps, structured_logger, "save_trusted", error_msg)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_failed_log_record(error_msg)
        raise
    transform_invalid_df = state.transform_result["invalid_positions"]
    invalid_frames = [
        df for df in [transform_invalid_df, invalid_positions_df] if df is not None
    ]
    combined_invalid_df = (
        pd.concat(invalid_frames, ignore_index=True)
        if len(invalid_frames) > 0
        else None
    )
    if combined_invalid_df is not None and not combined_invalid_df.empty:
        structured_logger.info(
            event="save_quarantine_started",
            message="Starting quarantined positions save",
            status="STARTED",
        )
        tracker.begin("save_quarantine")
        try:
            deps.save_positions_to_storage(
                config=state.pipeline_config,
                positions_df=combined_invalid_df,
                target_bucket="quarantined",
            )
            tracker.finish("save_quarantine", "success")
            state.quarantine_save_status = "SUCCESS"
            structured_logger.info(
                event="save_quarantine_succeeded",
                message="Quarantined positions save succeeded",
                status="SUCCEEDED",
                metadata={"records_saved": int(combined_invalid_df.shape[0])},
            )
        except Exception as e:
            tracker.finish("save_quarantine", "failed")
            state.quarantine_save_status = "FAILED"
            state.quarantine_save_error = str(e)
            error_msg = "Failed to save quarantined positions."
            structured_logger.error(
                event="save_quarantine_failed",
                message=error_msg,
                status="FAILED",
                error_type=type(e).__name__,
                error_message=str(e),
            )
            handle_failure_event(state, deps, structured_logger, "save_quarantine", error_msg)
            handle_phase_metrics_event(state, tracker, structured_logger, "failed")
            create_execution_failed_log_record(error_msg)
            raise
    else:
        tracker.finish("save_quarantine", "skipped")
        structured_logger.info(
            event="save_quarantine_skipped",
            message="No invalid positions to save in quarantine",
            status="SKIPPED",
        )
    structured_logger.info(
        event="mark_processed_started",
        message="Starting request mark as processed",
        status="STARTED",
    )
    tracker.begin("mark_processed")
    try:
        deps.mark_request_as_processed(state.pipeline_config, logical_date_string)
        tracker.finish("mark_processed", "success")
        structured_logger.info(
            event="mark_processed_succeeded",
            message="Request marked as processed",
            status="SUCCEEDED",
        )
    except Exception as e:
        tracker.finish("mark_processed", "failed")
        error_msg = f"Failed to mark request as processed: {str(e)}"
        structured_logger.error(
            event="mark_processed_failed",
            message="Failed to mark request as processed",
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        handle_failure_event(state, deps, structured_logger, "mark_processed", error_msg)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_failed_log_record(error_msg)
        raise
    structured_logger.info(
        event="quality_report_started",
        message="Starting quality report generation",
        status="STARTED",
    )
    tracker.begin("quality_report")
    try:
        report_log_metadata = deps.create_data_quality_report(
            config=state.pipeline_config,
            execution_id=state.execution_id,
            logical_date_utc=logical_date_string,
            source_file=state.source_file,
            transform_result=state.transform_result,
            expectations_result=state.expectations_result,
            pass_threshold=1.0,
            warn_threshold=0.980,
            quarantine_save_status=state.quarantine_save_status,
            quarantine_save_error=state.quarantine_save_error,
        )
        tracker.finish("quality_report", "success")
        structured_logger.info(
            event="quality_report_metrics",
            message="Quality report metrics",
            status="SUCCEEDED",
            metadata=report_log_metadata,
        )
        structured_logger.info(
            event="quality_report_succeeded",
            message="Quality report generation succeeded",
            status="SUCCEEDED",
        )
    except Exception as e:
        tracker.finish("quality_report", "failed")
        error_msg = "Quality report generation failed"
        structured_logger.error(
            event="quality_report_failed",
            message=error_msg,
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_failed_log_record(error_msg)
        raise
    handle_phase_metrics_event(state, tracker, structured_logger, "success")
    structured_logger.info(
        event="execution_finished",
        message="Execution finished successfully",
        status="SUCCEEDED",
    )

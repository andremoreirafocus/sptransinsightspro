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
    def create_execution_aborted_log_record(message: str, phase: str) -> None:
        structured_logger.error(
            event="execution_aborted",
            message=f"Pipeline aborted: {message}",
            status="FAILED",
            metadata={"phase": phase},
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
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record(error_msg, phase="config_load")
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
    except Exception:
        tracker.finish("load_positions", "failed")
        error_msg = "Load positions failed."
        handle_failure_event(state, deps, structured_logger, "load_positions", error_msg)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record(error_msg, phase="load_positions")
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
        create_execution_aborted_log_record(error_msg, phase="load_positions")
        raise ValueError(error_msg)
    tracker.finish("load_positions", "success")
    structured_logger.info(
        event="raw_schema_validation_started",
        message="Starting raw schema validation",
        status="STARTED",
    )
    tracker.begin("raw_schema_validation")
    try:
        is_valid, validation_errors = deps.validate_json_data_schema(
            raw_positions, state.pipeline_config["raw_data_json_schema"]
        )
    except Exception:
        tracker.finish("raw_schema_validation", "failed")
        error_msg = "Raw schema validation failed."
        handle_failure_event(state, deps, structured_logger, "raw_schema_validation", error_msg)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record(error_msg, phase="raw_schema_validation")
        raise
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
        create_execution_aborted_log_record(error_msg, phase="raw_schema_validation")
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
    except Exception:
        tracker.finish("transform", "failed")
        error_msg = "Transformation failed."
        handle_failure_event(state, deps, structured_logger, "transform", error_msg)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record(error_msg, phase="transform")
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
        create_execution_aborted_log_record(error_msg, phase="transform")
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
    except Exception:
        tracker.finish("expectations_validation", "failed")
        error_msg = "Expectations validation failed."
        handle_failure_event(state, deps, structured_logger, "expectations", error_msg)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record(error_msg, phase="expectations_validation")
        raise
    valid_positions_df = state.expectations_result["valid_df"]
    invalid_positions_df = state.expectations_result["invalid_df"]
    structured_logger.info(
        event="save_positions_started",
        message="Starting trusted positions save",
        status="STARTED",
        metadata={"target": "trusted", "rows": int(valid_positions_df.shape[0])},
    )
    tracker.begin("save_trusted")
    try:
        
        deps.save_positions_to_storage(state.pipeline_config, valid_positions_df, "trusted")
        tracker.finish("save_trusted", "success")
        structured_logger.info(
            event="save_positions_succeeded",
            message="Trusted positions save succeeded",
            status="SUCCEEDED",
            metadata={"target": "trusted", "records_saved": int(valid_positions_df.shape[0])},
        )
    except Exception:
        tracker.finish("save_trusted", "failed")
        error_msg = "Failed to save trusted positions."
        handle_failure_event(state, deps, structured_logger, "save_trusted", error_msg)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record(error_msg, phase="save_trusted")
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
            event="save_positions_started",
            message="Starting quarantined positions save",
            status="STARTED",
            metadata={"target": "quarantined", "rows": int(combined_invalid_df.shape[0])},
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
                event="save_positions_succeeded",
                message="Quarantined positions save succeeded",
                status="SUCCEEDED",
                metadata={"target": "quarantined", "records_saved": int(combined_invalid_df.shape[0])},
            )
        except Exception as e:
            tracker.finish("save_quarantine", "failed")
            state.quarantine_save_status = "FAILED"
            state.quarantine_save_error = str(e)
            error_msg = "Failed to save quarantined positions."
            handle_failure_event(state, deps, structured_logger, "save_quarantine", error_msg)
            handle_phase_metrics_event(state, tracker, structured_logger, "failed")
            create_execution_aborted_log_record(error_msg, phase="save_quarantine")
            raise
    else:
        tracker.finish("save_quarantine", "skipped")
        structured_logger.info(
            event="save_positions_skipped",
            message="No invalid positions to save in quarantine",
            status="SKIPPED",
            metadata={"target": "quarantined"},
        )
    structured_logger.info(
        event="mark_processed_started",
        message="Starting request mark as processed",
        status="STARTED",
        metadata={"logical_date": logical_date_string},
    )
    tracker.begin("mark_processed")
    try:
        deps.mark_request_as_processed(state.pipeline_config, logical_date_string)
        tracker.finish("mark_processed", "success")
        structured_logger.info(
            event="mark_processed_succeeded",
            message="Request marked as processed",
            status="SUCCEEDED",
            metadata={"logical_date": logical_date_string},
        )
    except Exception as e:
        tracker.finish("mark_processed", "failed")
        error_msg = f"Failed to mark request as processed: {str(e)}"
        handle_failure_event(state, deps, structured_logger, "mark_processed", error_msg)
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record(error_msg, phase="mark_processed")
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
    except Exception:
        tracker.finish("quality_report", "failed")
        error_msg = "Quality report generation failed"
        handle_phase_metrics_event(state, tracker, structured_logger, "failed")
        create_execution_aborted_log_record(error_msg, phase="quality_report")
        raise
    handle_phase_metrics_event(state, tracker, structured_logger, "success")
    structured_logger.info(
        event="execution_finished",
        message="Execution finished successfully",
        status="SUCCEEDED",
    )

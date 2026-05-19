from transformlivedata.config.transformlivedata_config_schema import GeneralConfig
from transformlivedata.orchestration_dependencies import (
    TransformLiveDataOrchestrationDependencies,
)
from transformlivedata.services.create_data_quality_report import (
    initialize_collected_quality_metrics,
    update_collected_metrics_from_expectations_result,
    update_collected_metrics_from_transform_result,
)
from observability.third_party_log_bridge import configure_third_party_log_bridge
from quality.execution_phase_metrics import (
    ExecutionPhaseMetricsTracker,
)
from observability.structured_event_logger import StructuredEventLogger, get_structured_logger
import pandas as pd
import logging
import uuid
from typing import Callable


logger = logging.getLogger(__name__)
THIRD_PARTY_LOGGER_NAMESPACES = (
    "urllib3.connectionpool",
    "py.warnings",
    "great_expectations.validator.validator",
    "great_expectations.data_context.types.base",
    "great_expectations.data_context.data_context.context_factory",
    "great_expectations.datasource.fluent.config",
)


def _send_quality_summary_webhook(
    summary: dict,
    pipeline_config: dict,
    send_webhook_fn: Callable[[dict, str], None],
) -> None:
    """Send quality summary via webhook if enabled."""
    webhook_url = pipeline_config["general"]["notifications"]["webhook_url"]
    if webhook_url.strip().lower() in {"disabled", "none", "null"}:
        # logger.info("Webhook notification disabled")
        return

    try:
        send_webhook_fn(summary, webhook_url)
        # logger.info("Webhook notification sent")
    except Exception:
        pass # logger.error("Webhook notification failed: %s", e)


def load_transform_save_positions(
    pipeline_name: str,
    logical_date_string: str,
    deps: TransformLiveDataOrchestrationDependencies,
) -> None:
    def write_failure_report(phase: str, message: str) -> None:
        if pipeline_config is None:
            logger.error(
                "Failed to write quality report on failure: pipeline_config is not available"
            )
            return
        try:
            failure_report = deps.create_failure_quality_report(
                config=pipeline_config,
                execution_id=execution_id,
                logical_date_utc=logical_date_string,
                source_file=logical_date_context["source_file"],
                failure_phase=phase,
                failure_message=message,
                batch_ts=(
                    transform_result.get("batch_ts")
                    if transform_result is not None
                    else logical_date_string
                ),
                transform_result=transform_result,
                expectations_result=expectations_result,
                quarantine_save_status=quarantine_save_status,
                quarantine_save_error=quarantine_save_error,
                collected_metrics=collected_metrics,
                execution_phase_metrics=tracker.to_log_payload("failed"),
            )
            summary = failure_report.get("summary", {})
            _send_quality_summary_webhook(summary, pipeline_config, deps.send_webhook)
        except Exception as e:
            logger.error("Failed to write quality report on failure: %s", e)

    execution_id = str(uuid.uuid4())
    correlation_id = logical_date_string
    structured_logger: StructuredEventLogger = get_structured_logger(
        service=pipeline_name,
        component="orchestrator",
        logger_name=__name__,
    )
    configure_third_party_log_bridge(
        structured_logger=structured_logger,
        execution_id=execution_id,
        correlation_id=correlation_id,
        namespaces=THIRD_PARTY_LOGGER_NAMESPACES,
    )
    collected_metrics = initialize_collected_quality_metrics()

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
        execution_id=execution_id,
        logical_date_utc=logical_date_string,
        phase_order=phase_order,
    )
    pipeline_config: dict | None = None
    logical_date_context: dict[str, str] = {}
    transform_result = None
    expectations_result = None
    quarantine_save_status = "SKIPPED"
    quarantine_save_error = None
    structured_logger.info(
        event="config_load_started",
        message="Starting configuration load",
        execution_id=execution_id,
        correlation_id=correlation_id,
        status="STARTED",
    )
    tracker.begin("config_load")
    try:
        logical_date_context = deps.build_logical_date_context(logical_date_string)
        pipeline_config = deps.get_config(
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
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="SUCCEEDED",
        )
    except Exception as e:
        tracker.finish("config_load", "failed")
        tracker.emit(logger, "failed")
        structured_logger.error(
            event="config_load_failed",
            message="Configuration load and validation failed",
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        raise ValueError("Pipeline configuration validation failed") from e
    structured_logger.info(
        event="execution_started",
        message="Starting execution",
        execution_id=execution_id,
        correlation_id=correlation_id,
        status="STARTED",
    )
    structured_logger.info(
        event="load_positions_started",
        message="Starting positions load",
        execution_id=execution_id,
        correlation_id=correlation_id,
        status="STARTED",
    )
    tracker.begin("load_positions")
    try:
        raw_positions = deps.load_positions(
            pipeline_config,
            logical_date_context["partition_path"],
            logical_date_context["source_file"],
        )
        structured_logger.info(
            event="load_positions_succeeded",
            message="Positions load succeeded",
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="SUCCEEDED",
        )
    except Exception as e:
        tracker.finish("load_positions", "failed")
        error_msg = "Load positions failed."
        structured_logger.error(
            event="load_positions_failed",
            message=error_msg,
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        write_failure_report("load_positions", error_msg)
        tracker.emit(logger, "failed")
        raise
    if not raw_positions:
        tracker.finish("load_positions", "failed")
        error_msg = "No position data found to transform."
        structured_logger.error(
            event="load_positions_failed",
            message=error_msg,
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="FAILED",
        )
        write_failure_report("load_positions", error_msg)
        tracker.emit(logger, "failed")
        raise ValueError(error_msg)
    tracker.finish("load_positions", "success")
    structured_logger.info(
        event="raw_schema_validation_started",
        message="Starting raw schema validation",
        execution_id=execution_id,
        correlation_id=correlation_id,
        status="STARTED",
    )
    tracker.begin("raw_schema_validation")
    is_valid, validation_errors = deps.validate_json_data_schema(
        raw_positions, pipeline_config["raw_data_json_schema"]
    )
    if not is_valid:
        tracker.finish("raw_schema_validation", "failed")
        error_msg = f"Raw data validation failed: {validation_errors}"
        structured_logger.error(
            event="raw_schema_validation_failed",
            message="Raw schema validation failed",
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="FAILED",
            error_message=error_msg,
        )
        write_failure_report("raw_schema_validation", error_msg)
        tracker.emit(logger, "failed")
        raise ValueError(error_msg)
    tracker.finish("raw_schema_validation", "success")
    structured_logger.info(
        event="raw_schema_validation_succeeded",
        message="Raw schema validation succeeded",
        execution_id=execution_id,
        correlation_id=correlation_id,
        status="SUCCEEDED",
    )
    structured_logger.info(
        event="transform_started",
        message="Starting transformation",
        execution_id=execution_id,
        correlation_id=correlation_id,
        status="STARTED",
    )
    tracker.begin("transform")
    try:
        transform_result = deps.transform_positions(pipeline_config, raw_positions)
        structured_logger.info(
            event="transform_succeeded",
            message="Transformation succeeded",
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="SUCCEEDED",
        )
    except Exception as e:
        tracker.finish("transform", "failed")
        error_msg = "Transformation failed."
        structured_logger.error(
            event="transform_failed",
            message=error_msg,
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        write_failure_report("transform", error_msg)
        tracker.emit(logger, "failed")
        raise
    if (
        not transform_result
        or transform_result.get("positions") is None
        or transform_result["positions"].empty
    ):
        tracker.finish("transform", "failed")
        error_msg = "No valid position records found after transformation."
        structured_logger.error(
            event="transform_failed",
            message=error_msg,
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="FAILED",
        )
        write_failure_report("transform", error_msg)
        tracker.emit(logger, "failed")
        raise ValueError(error_msg)
    tracker.finish("transform", "success")
    positions_df = transform_result["positions"]
    update_collected_metrics_from_transform_result(collected_metrics, transform_result)
    structured_logger.info(
        event="expectations_validation_started",
        message="Starting expectations validation",
        execution_id=execution_id,
        correlation_id=correlation_id,
        status="STARTED",
    )
    tracker.begin("expectations_validation")
    try:
        expectations_result = deps.validate_expectations(
            positions_df,
            pipeline_config["data_expectations"],
        )
        tracker.finish("expectations_validation", "success")
        structured_logger.info(
            event="expectations_validation_succeeded",
            message="Expectations validation succeeded",
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="SUCCEEDED",
        )
    except Exception as e:
        tracker.finish("expectations_validation", "failed")
        error_msg = "Expectations validation failed."
        structured_logger.error(
            event="expectations_validation_failed",
            message=error_msg,
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        write_failure_report("expectations", error_msg)
        tracker.emit(logger, "failed")
        raise
    valid_positions_df = expectations_result["valid_df"]
    invalid_positions_df = expectations_result["invalid_df"]
    update_collected_metrics_from_expectations_result(
        collected_metrics, expectations_result
    )
    structured_logger.info(
        event="save_trusted_started",
        message="Starting trusted positions save",
        execution_id=execution_id,
        correlation_id=correlation_id,
        status="STARTED",
    )
    tracker.begin("save_trusted")
    try:
        deps.save_positions_to_storage(pipeline_config, valid_positions_df, "trusted")
        tracker.finish("save_trusted", "success")
        structured_logger.info(
            event="save_trusted_succeeded",
            message="Trusted positions save succeeded",
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="SUCCEEDED",
            metadata={"records_saved": int(valid_positions_df.shape[0])},
        )
    except Exception as e:
        tracker.finish("save_trusted", "failed")
        error_msg = "Failed to save trusted positions."
        structured_logger.error(
            event="save_trusted_failed",
            message=error_msg,
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        write_failure_report("save_trusted", error_msg)
        tracker.emit(logger, "failed")
        raise
    transform_invalid_df = transform_result["invalid_positions"]
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
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="STARTED",
        )
        tracker.begin("save_quarantine")
        try:
            deps.save_positions_to_storage(
                config=pipeline_config,
                positions_df=combined_invalid_df,
                target_bucket="quarantined",
            )
            tracker.finish("save_quarantine", "success")
            quarantine_save_status = "SUCCESS"
            structured_logger.info(
                event="save_quarantine_succeeded",
                message="Quarantined positions save succeeded",
                execution_id=execution_id,
                correlation_id=correlation_id,
                status="SUCCEEDED",
                metadata={"records_saved": int(combined_invalid_df.shape[0])},
            )
        except Exception as e:
            tracker.finish("save_quarantine", "failed")
            quarantine_save_status = "FAILED"
            quarantine_save_error = str(e)
            error_msg = "Failed to save quarantined positions."
            structured_logger.error(
                event="save_quarantine_failed",
                message=error_msg,
                execution_id=execution_id,
                correlation_id=correlation_id,
                status="FAILED",
                error_type=type(e).__name__,
                error_message=str(e),
            )
            write_failure_report("save_quarantine", error_msg)
            tracker.emit(logger, "failed")
            raise
    else:
        tracker.finish("save_quarantine", "skipped")
        quarantine_save_status = "SKIPPED"
        structured_logger.info(
            event="save_quarantine_skipped",
            message="No invalid positions to save in quarantine",
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="SKIPPED",
        )
    structured_logger.info(
        event="mark_processed_started",
        message="Starting request mark as processed",
        execution_id=execution_id,
        correlation_id=correlation_id,
        status="STARTED",
    )
    tracker.begin("mark_processed")
    try:
        deps.mark_request_as_processed(pipeline_config, logical_date_string)
        tracker.finish("mark_processed", "success")
        structured_logger.info(
            event="mark_processed_succeeded",
            message="Request marked as processed",
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="SUCCEEDED",
        )
    except Exception as e:
        tracker.finish("mark_processed", "failed")
        error_msg = f"Failed to mark request as processed: {str(e)}"
        structured_logger.error(
            event="mark_processed_failed",
            message="Failed to mark request as processed",
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        write_failure_report("mark_processed", error_msg)
        tracker.emit(logger, "failed")
        raise
    structured_logger.info(
        event="quality_report_started",
        message="Starting quality report generation",
        execution_id=execution_id,
        correlation_id=correlation_id,
        status="STARTED",
    )
    tracker.begin("quality_report")
    try:
        report = deps.create_data_quality_report(
            config=pipeline_config,
            execution_id=execution_id,
            logical_date_utc=logical_date_string,
            source_file=logical_date_context["source_file"],
            transform_result=transform_result,
            expectations_result=expectations_result,
            pass_threshold=1.0,
            warn_threshold=0.980,
            quarantine_save_status=quarantine_save_status,
            quarantine_save_error=quarantine_save_error,
        )
        tracker.finish("quality_report", "success")
        structured_logger.info(
            event="quality_report_succeeded",
            message="Quality report generation succeeded",
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="SUCCEEDED",
        )
    except Exception as e:
        tracker.finish("quality_report", "failed")
        structured_logger.error(
            event="quality_report_failed",
            message="Quality report generation failed",
            execution_id=execution_id,
            correlation_id=correlation_id,
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        tracker.emit(logger, "failed")
        raise
    summary = report.get("summary", {})
    _send_quality_summary_webhook(summary, pipeline_config, deps.send_webhook)
    tracker.emit(logger, "success")
    structured_logger.info(
        event="execution_finished",
        message="Execution finished successfully",
        execution_id=execution_id,
        correlation_id=correlation_id,
        status="SUCCEEDED",
    )

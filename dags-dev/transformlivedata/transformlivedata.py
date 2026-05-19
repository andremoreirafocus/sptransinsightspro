from transformlivedata.config.transformlivedata_config_schema import GeneralConfig
from transformlivedata.orchestration_dependencies import (
    TransformLiveDataOrchestrationDependencies,
)
from quality.execution_phase_metrics import (
    ExecutionPhaseMetricsTracker,
)
from observability.structured_event_logger import StructuredEventLogger, get_structured_logger
from transformlivedata.domain.events import EventType
import pandas as pd
import logging
import uuid
from typing import Callable, Dict, Optional


logger = logging.getLogger(__name__)


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
    correlation_id = logical_date_string
    structured_logger: StructuredEventLogger = get_structured_logger(
        service=pipeline_name,
        component="orchestrator",
        logger_name=__name__,
        base_metadata={"pipeline": pipeline_name},
    )

    def emit_structured_event(
        *,
        level: str,
        event: EventType,
        message: str,
        execution_id: Optional[str] = None,
        phase: Optional[str] = None,
        status: Optional[str] = None,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, object]] = None,
    ) -> None:
        payload_metadata: Dict[str, object] = {
            "pipeline": pipeline_name,
            "logical_date_utc": logical_date_string,
        }
        if phase is not None:
            payload_metadata["phase"] = phase
        if metadata:
            payload_metadata.update(metadata)

        emit_kwargs = {
            "event": event,
            "message": message,
            "execution_id": execution_id,
            "correlation_id": correlation_id,
            "metadata": payload_metadata,
        }
        if status is not None:
            emit_kwargs["status"] = status
        if error_type is not None:
            emit_kwargs["error_type"] = error_type
        if error_message is not None:
            emit_kwargs["error_message"] = error_message

        if level == "INFO":
            structured_logger.info(**emit_kwargs)
        elif level == "ERROR":
            structured_logger.error(**emit_kwargs)
        elif level == "WARNING":
            structured_logger.warning(**emit_kwargs)
        elif level == "DEBUG":
            structured_logger.debug(**emit_kwargs)
        else:
            structured_logger.critical(**emit_kwargs)

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
            )
            summary = failure_report.get("summary", {})
            _send_quality_summary_webhook(summary, pipeline_config, deps.send_webhook)
        except Exception as e:
            logger.error("Failed to write quality report on failure: %s", e)

    execution_id = str(uuid.uuid4())
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
    except Exception as e:
        tracker.finish("config_load", "failed")
        tracker.emit(logger, "failed")
        logger.error("Pipeline configuration validation failed")
        raise ValueError("Pipeline configuration validation failed") from e
    emit_structured_event(
        level="INFO",
        event="execution_started",
        message=f"Starting execution {execution_id}",
        execution_id=execution_id,
        phase="config_load",
        status="STARTED",
    )
    logger.info(f"Loading positions for {logical_date_string}...")
    tracker.begin("load_positions")
    try:
        raw_positions = deps.load_positions(
            pipeline_config,
            logical_date_context["partition_path"],
            logical_date_context["source_file"],
        )
    except Exception:
        tracker.finish("load_positions", "failed")
        error_msg = "Load positions failed."
        logger.error(error_msg)
        write_failure_report("load_positions", error_msg)
        tracker.emit(logger, "failed")
        raise
    if not raw_positions:
        tracker.finish("load_positions", "failed")
        error_msg = "No position data found to transform."
        logger.error(error_msg)
        write_failure_report("load_positions", error_msg)
        tracker.emit(logger, "failed")
        raise ValueError(error_msg)
    tracker.finish("load_positions", "success")
    logger.info("Starting raw data validation...")
    tracker.begin("raw_schema_validation")
    is_valid, validation_errors = deps.validate_json_data_schema(
        raw_positions, pipeline_config["raw_data_json_schema"]
    )
    if not is_valid:
        tracker.finish("raw_schema_validation", "failed")
        error_msg = f"Raw data validation failed: {validation_errors}"
        logger.error(error_msg)
        write_failure_report("raw_schema_validation", error_msg)
        tracker.emit(logger, "failed")
        raise ValueError(error_msg)
    tracker.finish("raw_schema_validation", "success")
    logger.info("Raw data validation passed ✓")
    logger.info("Starting transformation...")
    tracker.begin("transform")
    try:
        transform_result = deps.transform_positions(pipeline_config, raw_positions)
    except Exception:
        tracker.finish("transform", "failed")
        error_msg = "Transformation failed."
        logger.error(error_msg)
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
        logger.error(error_msg)
        write_failure_report("transform", error_msg)
        tracker.emit(logger, "failed")
        raise ValueError(error_msg)
    tracker.finish("transform", "success")
    positions_df = transform_result["positions"]
    logger.info("Validating positions expectations...")
    tracker.begin("expectations_validation")
    try:
        expectations_result = deps.validate_expectations(
            positions_df,
            pipeline_config["data_expectations"],
        )
        tracker.finish("expectations_validation", "success")
    except Exception:
        tracker.finish("expectations_validation", "failed")
        error_msg = "Expectations validation failed."
        logger.error(error_msg)
        write_failure_report("expectations", error_msg)
        tracker.emit(logger, "failed")
        raise
    valid_positions_df = expectations_result["valid_df"]
    invalid_positions_df = expectations_result["invalid_df"]
    logger.info("Saving valid positions to storage...")
    tracker.begin("save_trusted")
    try:
        deps.save_positions_to_storage(pipeline_config, valid_positions_df, "trusted")
        tracker.finish("save_trusted", "success")
        logger.info(f"Saved {valid_positions_df.shape[0]} records to trusted layer")
    except Exception:
        tracker.finish("save_trusted", "failed")
        error_msg = "Failed to save trusted positions."
        logger.error(error_msg)
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
        logger.info("Saving invalid positions to quarantine...")
        tracker.begin("save_quarantine")
        try:
            deps.save_positions_to_storage(
                config=pipeline_config,
                positions_df=combined_invalid_df,
                target_bucket="quarantined",
            )
            tracker.finish("save_quarantine", "success")
            quarantine_save_status = "SUCCESS"
            logger.info(
                f"Saved {combined_invalid_df.shape[0]} records to quarantined layer"
            )
        except Exception as e:
            tracker.finish("save_quarantine", "failed")
            quarantine_save_status = "FAILED"
            quarantine_save_error = str(e)
            error_msg = "Failed to save quarantined positions."
            logger.error(error_msg)
            write_failure_report("save_quarantine", error_msg)
            tracker.emit(logger, "failed")
            raise
    else:
        tracker.finish("save_quarantine", "skipped")
        quarantine_save_status = "SKIPPED"
    tracker.begin("mark_processed")
    try:
        deps.mark_request_as_processed(pipeline_config, logical_date_string)
        tracker.finish("mark_processed", "success")
    except Exception as e:
        tracker.finish("mark_processed", "failed")
        error_msg = f"Failed to mark request as processed: {str(e)}"
        write_failure_report("mark_processed", error_msg)
        tracker.emit(logger, "failed")
        raise 
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
    except Exception:
        tracker.finish("quality_report", "failed")
        tracker.emit(logger, "failed")
        raise
    summary = report.get("summary", {})
    _send_quality_summary_webhook(summary, pipeline_config, deps.send_webhook)
    tracker.emit(logger, "success")
    logger.info(f"Execution {execution_id} completed successfully")

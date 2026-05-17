from transformlivedata.services.load_positions import load_positions
from transformlivedata.services.transform_positions import (
    transform_positions,
)
from transformlivedata.services.save_positions_to_storage import (
    save_positions_to_storage,
)
from transformlivedata.services.processed_requests_helper import (
    mark_request_as_processed,
)
from quality.validate_expectations import (
    validate_expectations,
)
from pipeline_configurator.config import get_config
from transformlivedata.config.transformlivedata_config_schema import GeneralConfig
from quality.validate_json_data_schema import (
    validate_json_data_schema,
)
from transformlivedata.services.create_data_quality_report import (
    create_data_quality_report,
    create_failure_quality_report,
)
from transformlivedata.services.build_logical_date_context import (
    build_logical_date_context,
)
from infra.notifications import send_webhook
import pandas as pd
import logging
import uuid
import time
import json


logger = logging.getLogger(__name__)


def _send_quality_summary_webhook(summary: dict, pipeline_config: dict) -> None:
    """Send quality summary via webhook if enabled."""
    webhook_url = pipeline_config["general"]["notifications"]["webhook_url"]
    if webhook_url.strip().lower() in {"disabled", "none", "null"}:
        logger.info("Webhook notification disabled")
        return

    try:
        send_webhook(summary, webhook_url)
        logger.info("Webhook notification sent")
    except Exception as e:
        logger.error("Webhook notification failed: %s", e)


def load_transform_save_positions(pipeline_name: str, logical_date_string: str) -> None:
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
    phase_metrics = {
        phase: {"duration_seconds": 0.0, "status": "skipped"} for phase in phase_order
    }
    phase_start_times = {}
    pipeline_config = None
    logical_date_context = {}
    execution_start = time.perf_counter()

    def begin_phase(phase_name: str) -> None:
        phase_start_times[phase_name] = time.perf_counter()

    def finish_phase(phase_name: str, status: str) -> None:
        started_at = phase_start_times.get(phase_name)
        duration = 0.0
        if started_at is not None:
            duration = time.perf_counter() - started_at
        phase_metrics[phase_name] = {
            "duration_seconds": round(duration, 6),
            "status": status,
        }

    def emit_execution_phase_metrics(overall_status: str) -> None:
        payload = {
            "event": "execution_phase_metrics",
            "pipeline": pipeline_name,
            "execution_id": execution_id,
            "logical_date_utc": logical_date_string,
            "overall_status": overall_status,
            "total_duration_seconds": round(time.perf_counter() - execution_start, 6),
            "phase_metrics": phase_metrics,
        }
        log_message = json.dumps(payload, ensure_ascii=True)
        if overall_status == "success":
            logger.info(log_message)
        else:
            logger.error(log_message)

    begin_phase("config_load")
    try:
        logical_date_context = build_logical_date_context(logical_date_string)
        pipeline_config = get_config(
            pipeline_name,
            None,
            GeneralConfig,
            None,
            "minio_conn",
            "airflow_postgres_conn",
        )
        finish_phase("config_load", "success")
    except Exception as e:
        finish_phase("config_load", "failed")
        emit_execution_phase_metrics("failed")
        logger.error(f"Pipeline configuration validation failed: {e}")
        raise ValueError(f"Pipeline configuration validation failed: {e}")
    logger.info(f"Starting execution {execution_id}")
    logger.info(f"Transforming position for {logical_date_string}...")
    transform_result = None
    expectations_result = None
    quarantine_save_status = "SKIPPED"
    quarantine_save_error = None

    def write_failure_report(phase: str, message: str) -> None:
        try:
            failure_report = create_failure_quality_report(
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
            _send_quality_summary_webhook(summary, pipeline_config)
        except Exception as e:
            logger.error("Failed to write quality report on failure: %s", e)

    logger.info("=== LOAD STAGE: load_positions ===")
    begin_phase("load_positions")
    try:
        raw_positions = load_positions(
            pipeline_config,
            logical_date_context["partition_path"],
            logical_date_context["source_file"],
        )
    except Exception as e:
        finish_phase("load_positions", "failed")
        error_msg = f"Load positions failed: {e}"
        logger.error(error_msg)
        write_failure_report("load_positions", error_msg)
        emit_execution_phase_metrics("failed")
        raise
    if not raw_positions:
        finish_phase("load_positions", "failed")
        error_msg = "No position data found to transform."
        logger.error(error_msg)
        write_failure_report("load_positions", error_msg)
        emit_execution_phase_metrics("failed")
        raise ValueError(error_msg)
    finish_phase("load_positions", "success")
    logger.info("=== RAW DATA VALIDATION STAGE ===")
    begin_phase("raw_schema_validation")
    is_valid, validation_errors = validate_json_data_schema(
        raw_positions, pipeline_config["raw_data_json_schema"]
    )
    if not is_valid:
        finish_phase("raw_schema_validation", "failed")
        error_msg = f"Raw data validation failed: {validation_errors}"
        logger.error(error_msg)
        write_failure_report("raw_schema_validation", error_msg)
        emit_execution_phase_metrics("failed")
        raise ValueError(error_msg)
    finish_phase("raw_schema_validation", "success")
    logger.info("Raw data validation passed ✓")
    logger.info("=== TRANSFORM STAGE: transform_positions ===")
    begin_phase("transform")
    try:
        transform_result = transform_positions(pipeline_config, raw_positions)
    except Exception as e:
        finish_phase("transform", "failed")
        error_msg = f"Transform failed: {e}"
        logger.error(error_msg)
        write_failure_report("transform", error_msg)
        emit_execution_phase_metrics("failed")
        raise
    if (
        not transform_result
        or transform_result.get("positions") is None
        or transform_result["positions"].empty
    ):
        finish_phase("transform", "failed")
        error_msg = "No valid position records found after transformation."
        logger.error(error_msg)
        write_failure_report("transform", error_msg)
        emit_execution_phase_metrics("failed")
        raise ValueError(error_msg)
    finish_phase("transform", "success")
    positions_df = transform_result["positions"]
    logger.info("=== EXPECTATIONS VALIDATION STAGE: validate_expectations ===")
    logger.info("Validating positions expectations...")
    begin_phase("expectations_validation")
    try:
        expectations_result = validate_expectations(
            positions_df,
            pipeline_config["data_expectations"],
        )
        finish_phase("expectations_validation", "success")
    except Exception as e:
        finish_phase("expectations_validation", "failed")
        error_msg = f"Expectations validation failed: {e}"
        logger.error(error_msg)
        write_failure_report("expectations", error_msg)
        emit_execution_phase_metrics("failed")
        raise
    valid_positions_df = expectations_result["valid_df"]
    invalid_positions_df = expectations_result["invalid_df"]
    logger.info("=== SAVE STAGE: save_positions_to_storage ===")
    logger.info("Saving valid positions to storage...")
    begin_phase("save_trusted")
    try:
        save_positions_to_storage(pipeline_config, valid_positions_df, "trusted")
        finish_phase("save_trusted", "success")
        logger.info(f"Saved {valid_positions_df.shape[0]} records to trusted layer")
    except Exception as e:
        finish_phase("save_trusted", "failed")
        error_msg = f"Failed to save trusted positions: {e}"
        logger.error(error_msg)
        write_failure_report("save_trusted", error_msg)
        emit_execution_phase_metrics("failed")
        raise
    transform_invalid_df = transform_result.get("invalid_positions")
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
        begin_phase("save_quarantine")
        try:
            save_positions_to_storage(
                pipeline_config, combined_invalid_df, "quarantined"
            )
            finish_phase("save_quarantine", "success")
            quarantine_save_status = "SUCCESS"
            logger.info(
                f"Saved {combined_invalid_df.shape[0]} records to quarantined layer"
            )
        except Exception as e:
            finish_phase("save_quarantine", "failed")
            quarantine_save_status = "FAILED"
            quarantine_save_error = str(e)
            error_msg = f"Failed to save quarantined positions: {e}"
            logger.error(error_msg)
            write_failure_report("save_quarantine", error_msg)
            emit_execution_phase_metrics("failed")
            raise
    else:
        finish_phase("save_quarantine", "skipped")
        quarantine_save_status = "SKIPPED"
    begin_phase("mark_processed")
    try:
        mark_request_as_processed(pipeline_config, logical_date_string)
        finish_phase("mark_processed", "success")
    except Exception as e:
        finish_phase("mark_processed", "failed")
        error_msg = f"Failed to mark request as processed: {e}"
        logger.error(error_msg)
        write_failure_report("mark_processed", error_msg)
        emit_execution_phase_metrics("failed")
        raise
    begin_phase("quality_report")
    try:
        report = create_data_quality_report(
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
        finish_phase("quality_report", "success")
    except Exception:
        finish_phase("quality_report", "failed")
        emit_execution_phase_metrics("failed")
        raise
    summary = report.get("summary", {})
    _send_quality_summary_webhook(summary, pipeline_config)
    emit_execution_phase_metrics("success")
    logger.info(f"Execution {execution_id} completed successfully")

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
    logical_date_context = build_logical_date_context(logical_date_string)
    try:
        pipeline_config = get_config(
            pipeline_name,
            None,
            GeneralConfig,
            None,
            "minio_conn",
            "airflow_postgres_conn",
        )
    except Exception as e:
        logger.error(f"Pipeline configuration validation failed: {e}")
        raise ValueError(f"Pipeline configuration validation failed: {e}")
    execution_id = str(uuid.uuid4())
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
    try:
        raw_positions = load_positions(
            pipeline_config,
            logical_date_context["partition_path"],
            logical_date_context["source_file"],
        )
    except Exception as e:
        error_msg = f"Load positions failed: {e}"
        logger.error(error_msg)
        write_failure_report("load_positions", error_msg)
        raise
    if not raw_positions:
        error_msg = "No position data found to transform."
        logger.error(error_msg)
        write_failure_report("load_positions", error_msg)
        raise ValueError(error_msg)
    logger.info("=== RAW DATA VALIDATION STAGE ===")
    is_valid, validation_errors = validate_json_data_schema(
        raw_positions, pipeline_config["raw_data_json_schema"]
    )
    if not is_valid:
        error_msg = f"Raw data validation failed: {validation_errors}"
        logger.error(error_msg)
        write_failure_report("raw_schema_validation", error_msg)
        raise ValueError(error_msg)
    logger.info("Raw data validation passed ✓")
    logger.info("=== TRANSFORM STAGE: transform_positions ===")
    try:
        transform_result = transform_positions(pipeline_config, raw_positions)
    except Exception as e:
        error_msg = f"Transform failed: {e}"
        logger.error(error_msg)
        write_failure_report("transform", error_msg)
        raise
    if (
        not transform_result
        or transform_result.get("positions") is None
        or transform_result["positions"].empty
    ):
        error_msg = "No valid position records found after transformation."
        logger.error(error_msg)
        write_failure_report("transform", error_msg)
        raise ValueError(error_msg)
    positions_df = transform_result["positions"]
    logger.info("=== EXPECTATIONS VALIDATION STAGE: validate_expectations ===")
    logger.info("Validating positions expectations...")
    try:
        expectations_result = validate_expectations(
            positions_df,
            pipeline_config["data_expectations"],
        )
    except Exception as e:
        error_msg = f"Expectations validation failed: {e}"
        logger.error(error_msg)
        write_failure_report("expectations", error_msg)
        raise
    valid_positions_df = expectations_result["valid_df"]
    invalid_positions_df = expectations_result["invalid_df"]
    logger.info("=== SAVE STAGE: save_positions_to_storage ===")
    logger.info("Saving valid positions to storage...")
    try:
        save_positions_to_storage(pipeline_config, valid_positions_df, "trusted")
        logger.info(f"Saved {valid_positions_df.shape[0]} records to trusted layer")
    except Exception as e:
        error_msg = f"Failed to save trusted positions: {e}"
        logger.error(error_msg)
        write_failure_report("save_trusted", error_msg)
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
        try:
            save_positions_to_storage(
                pipeline_config, combined_invalid_df, "quarantined"
            )
            quarantine_save_status = "SUCCESS"
            logger.info(
                f"Saved {combined_invalid_df.shape[0]} records to quarantined layer"
            )
        except Exception as e:
            quarantine_save_status = "FAILED"
            quarantine_save_error = str(e)
            error_msg = f"Failed to save quarantined positions: {e}"
            logger.error(error_msg)
            write_failure_report("save_quarantine", error_msg)
            raise
    else:
        quarantine_save_status = "SKIPPED"
    try:
        mark_request_as_processed(pipeline_config, logical_date_string)
    except Exception as e:
        error_msg = f"Failed to mark request as processed: {e}"
        logger.error(error_msg)
        write_failure_report("mark_processed", error_msg)
        raise
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
    summary = report.get("summary", {})
    _send_quality_summary_webhook(summary, pipeline_config)
    logger.info(f"Execution {execution_id} completed successfully")

from gtfs.services.extract_gtfs_files import extract_gtfs_files
from gtfs.services.save_files_to_raw_storage import (
    save_files_to_raw_storage,
)
from gtfs.services.validate_raw_gtfs_files import validate_raw_gtfs_files
from gtfs.services.data_quality_report import create_failure_quality_report
from gtfs.config.gtfs_config_schema import GeneralConfig
from pipeline_configurator.config import get_config
from infra.notifications import send_webhook

from gtfs.services.create_save_trip_details import (
    create_trip_details_table_and_fill_missing_data,
)
from gtfs.services.transforms import transform_and_validate_table
from gtfs.services.relocate_staged_trusted_files import relocate_staged_trusted_files
import logging
from logging.handlers import RotatingFileHandler
import uuid

LOG_FILENAME = "gtfs.log"

# In Airflow just remove this logging configuration block
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        # Rotation: 5MB per file, keeping the last 5 files
        RotatingFileHandler(LOG_FILENAME, maxBytes=5 * 1024 * 1024, backupCount=5),
        logging.StreamHandler(),  # Also keeps console output
    ],
)

logger = logging.getLogger(__name__)

PIPELINE_NAME = "gtfs"


def _load_pipeline_config():
    try:
        pipeline_config = get_config(
            PIPELINE_NAME,
            None,
            GeneralConfig,
            "gtfs_conn",
            "minio_conn",
            None,
        )
    except Exception as e:
        logger.error(f"Pipeline configuration validation failed: {e}")
        raise ValueError(f"Pipeline configuration validation failed: {e}")
    return pipeline_config


def extract_load_files():
    pipeline_config = _load_pipeline_config()
    execution_id = str(uuid.uuid4())
    files_list = None
    error_details = None
    validated_items_count = 0
    relocation_status = "NOT_APPLICABLE"
    relocation_error = None

    def write_failure_report(phase, message):
        try:
            failure_report = create_failure_quality_report(
                stage="extract_load_files",
                execution_id=execution_id,
                failure_phase=phase,
                failure_message=message,
                error_details=error_details,
                validated_items_count=validated_items_count,
                relocation_status=relocation_status,
                relocation_error=relocation_error,
            )
            summary = failure_report.get("summary", {})
            webhook_url = pipeline_config["general"]["notifications"]["webhook_url"]
            if webhook_url.strip().lower() in {"disabled", "none", "null"}:
                logger.info("Webhook notification disabled (failure path)")
            else:
                try:
                    send_webhook(summary, webhook_url)
                    logger.info("Webhook notification sent (failure path)")
                except Exception as e:
                    logger.error("Webhook notification failed: %s", e)
        except Exception as e:
            logger.error("Failed to build/send failure report: %s", e)

    try:
        logger.info("=== EXTRACT & LOAD STAGE: extract_gtfs_files ===")
        files_list = extract_gtfs_files(pipeline_config)
        if not files_list:
            raise ValueError("No GTFS files extracted.")

        logger.info("=== EXTRACT & LOAD STAGE: validate_raw_gtfs_files ===")
        validation_result = validate_raw_gtfs_files(
            pipeline_config,
            files_list,
        )
        error_details = {
            "errors_by_file": validation_result.get("errors_by_file", {}),
        }
        validated_items_count = validation_result.get("validated_files_count", 0)

        if not validation_result.get("is_valid", False):
            errors_by_file = error_details.get("errors_by_file", {})
            consolidated_reasons = "; ".join(
                [f"{file}:{reasons}" for file, reasons in errors_by_file.items()]
            )
            logger.error("Raw GTFS validation failed: %s", consolidated_reasons)
            logger.info("Saving all extracted files to quarantine...")
            try:
                save_files_to_raw_storage(
                    pipeline_config, files_list, failed=True
                )
                relocation_status = "SUCCESS"
                logger.info("All extracted files saved to quarantine.")
            except Exception as e:
                relocation_status = "FAILED"
                relocation_error = str(e)
                logger.error("Failed to save files to quarantine: %s", e)
                raise ValueError(f"Validation failed and quarantine save failed: {e}")
            raise ValueError(f"Raw GTFS validation failed: {consolidated_reasons}")

        logger.info("All raw GTFS files passed validation.")
        save_files_to_raw_storage(pipeline_config, files_list)
        logger.info("Stage 1 completed successfully.")
    except Exception as e:
        error_msg = f"Stage-1 extract/load failed: {e}"
        logger.error(error_msg)
        write_failure_report("extract_load_files", error_msg)
        raise


def transform():
    logger.info("=== TRANSFORMATION STAGE: transform_and_validate_table ===")
    pipeline_config = _load_pipeline_config()
    execution_id = str(uuid.uuid4())
    validated_items_count = 0
    relocation_status = "NOT_APPLICABLE"
    relocation_error = None
    table_results = []
    error_details = None
    table_names = [
        "stops",
        "stop_times",
        "routes",
        "trips",
        "frequencies",
        "calendar",
    ]

    def write_failure_report(phase, message):
        try:
            failure_report = create_failure_quality_report(
                stage="transformation",
                execution_id=execution_id,
                failure_phase=phase,
                failure_message=message,
                error_details=error_details,
                validated_items_count=validated_items_count,
                relocation_status=relocation_status,
                relocation_error=relocation_error,
            )
            summary = failure_report.get("summary", {})
            webhook_url = pipeline_config["general"]["notifications"]["webhook_url"]
            if webhook_url.strip().lower() in {"disabled", "none", "null"}:
                logger.info("Webhook notification disabled (failure path)")
            else:
                try:
                    send_webhook(summary, webhook_url)
                    logger.info("Webhook notification sent (failure path)")
                except Exception as e:
                    logger.error("Webhook notification failed: %s", e)
        except Exception as e:
            logger.error("Failed to build/send failure report: %s", e)

    try:
        for table_name in table_names:
            result = transform_and_validate_table(pipeline_config, table_name)
            table_results.append(result)
        validated_items_count = len(table_results)
        errors_by_table = {
            row["table_name"]: row["errors"]
            for row in table_results
            if not row.get("is_valid", False)
        }
        error_details = {
            "errors_by_table": errors_by_table,
            "table_results": [
                {
                    "table_name": row["table_name"],
                    "staged_written": row["staged_written"],
                }
                for row in table_results
            ],
        }

        staged_results = [row for row in table_results if row.get("staged_written")]
        if errors_by_table:
            relocation = relocate_staged_trusted_files(
                pipeline_config,
                staged_results,
                target="quarantine",
            )
            relocation_status = relocation["status"]
            if relocation.get("errors"):
                relocation_error = str(relocation["errors"])
                error_details["relocation_errors"] = relocation["errors"]
            consolidated_reasons = "; ".join(
                [f"{table}:{reasons}" for table, reasons in errors_by_table.items()]
            )
            error_message = (
                "TRANSFORMATION STAGE failed: "
                f"Validation failures detected: {consolidated_reasons}"
            )
            if relocation_error:
                error_message = f"{error_message}; relocation_error:{relocation_error}"
            raise ValueError(error_message)

        relocation = relocate_staged_trusted_files(
            pipeline_config,
            staged_results,
            target="final",
        )
        relocation_status = relocation["status"]
        if relocation.get("errors"):
            relocation_error = str(relocation["errors"])
            error_details = {
                "errors_by_table": {},
                "table_results": [
                    {
                        "table_name": row["table_name"],
                        "staged_written": row["staged_written"],
                    }
                    for row in table_results
                ],
                "relocation_errors": relocation["errors"],
            }
            raise ValueError(
                "TRANSFORMATION STAGE failed: "
                f"staging_to_final_relocation_error:{relocation_error}"
            )

        logger.info("TRANSFORMATION STAGE completed successfully.")
    except Exception as e:
        error_msg = f"TRANSFORMATION STAGE failed: {e}"
        logger.error(error_msg)
        write_failure_report("transform", error_msg)
        raise


def create_trip_details():
    logger.info("Creating trip details...")
    pipeline_config = _load_pipeline_config()
    create_trip_details_table_and_fill_missing_data(pipeline_config)
    logger.info("Trip details transformation completed successfully.")


def main():
    extract_load_files()
    transform()
    create_trip_details()


if __name__ == "__main__":
    main()

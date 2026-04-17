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
from gtfs.services.transforms import (
    transform_calendar,
    transform_frequencies,
    transform_routes,
    transform_stop_times,
    transform_stops,
    transform_trips,
)
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
    validation_result = None
    quarantine_save_status = "NOT_APPLICABLE"
    quarantine_save_error = None

    def write_failure_report(phase, message):
        try:
            failure_report = create_failure_quality_report(
                execution_id=execution_id,
                failure_phase=phase,
                failure_message=message,
                validation_result=validation_result,
                quarantine_save_status=quarantine_save_status,
                quarantine_save_error=quarantine_save_error,
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

        if not validation_result.get("is_valid", False):
            errors_by_file = validation_result.get("errors_by_file", {})
            consolidated_reasons = "; ".join(
                [f"{file}:{reasons}" for file, reasons in errors_by_file.items()]
            )
            logger.error("Raw GTFS validation failed: %s", consolidated_reasons)
            logger.info("Saving all extracted files to quarantine...")
            try:
                save_files_to_raw_storage(
                    pipeline_config, files_list, failed=True
                )
                quarantine_save_status = "SUCCESS"
                logger.info("All extracted files saved to quarantine.")
            except Exception as e:
                quarantine_save_status = "FAILED"
                quarantine_save_error = str(e)
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
    logging.info("Starting GTFS Transformations...")
    pipeline_config = _load_pipeline_config()
    transform_routes(pipeline_config)
    transform_trips(pipeline_config)
    transform_stops(pipeline_config)
    transform_stop_times(pipeline_config)
    transform_frequencies(pipeline_config)
    transform_calendar(pipeline_config)
    logger.info("All transformations completed successfully.")


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

from transformlivedata.services.load_positions import load_positions
from transformlivedata.services.transform_positions import (
    transform_positions,
    get_transformation_metrics_and_issues_report,
)
from transformlivedata.services.save_positions_to_storage import (
    save_positions_to_storage,
)
from transformlivedata.services.processed_requests_helper import (
    mark_request_as_processed,
)
from transformlivedata.quality.validate_expectations import (
    validate_expectations,
)
from transformlivedata.services.lineage_report import create_lineage_report
from transformlivedata.config import get_config
from transformlivedata.quality.RawDataExpectations import RawDataExpectations
from datetime import datetime
from zoneinfo import ZoneInfo
import logging
from logging.handlers import RotatingFileHandler
import uuid
import os

LOG_FILENAME = "transformlivedata.log"

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


def load_transform_save_positions(logical_date_string):
    """
    Load, transform, and save positions with JSON-driven raw validation and GE lineage tracking.

    INTEGRATION POINT 1: Raw validation using RawDataExpectations (jsonschema-driven)
    """
    # Create unique execution ID for tracking
    execution_id = str(uuid.uuid4())
    logger.info(f"Starting execution {execution_id}")
    config = get_config()
    dt_utc = datetime.fromisoformat(logical_date_string)
    dt = dt_utc.astimezone(ZoneInfo("America/Sao_Paulo"))
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    hour = dt.strftime("%H")
    minute = dt.strftime("%M")
    logger.info(f"Transforming position for {dt}...")
    logger.info("=== LOAD STAGE: load_positions ===")
    raw_positions = load_positions(config, year, month, day, hour, minute)
    if not raw_positions:
        logger.error("No position data found to transform.")
        raise ValueError("No position data found to transform.")
    logger.info("=== PHASE 2: RAW DATA VALIDATION ===")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    raw_expectations_config = os.path.join(
        script_dir, "transformlivedata", "config", "raw_expectations.json"
    )
    raw_validator = RawDataExpectations(config_file=raw_expectations_config)
    is_valid, validation_errors = raw_validator.validate(raw_positions)
    if not is_valid:
        error_msg = f"Raw data validation failed: {validation_errors}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    logger.info("Raw data validation passed ✓")
    logger.info("=== TRANSFORM STAGE: transform_positions ===")
    transform_result = transform_positions(config, raw_positions)
    if not transform_result or not transform_result.get("positions_table"):
        logger.error("No valid position records found after transformation.")
        raise ValueError("No valid position records found after transformation.")
    positions_table = transform_result["positions_table"]
    validation_report = get_transformation_metrics_and_issues_report(transform_result)
    logger.info(validation_report)
    expectations_config = os.path.join(
        script_dir, "transformlivedata", "config", "expectations.json"
    )
    valid_df, invalid_df = validate_expectations(
        positions_table,
        expectations_config,
    )
    logger.info("=== SAVE STAGE: save_positions_to_storage ===")
    save_positions_to_storage(config, positions_table)
    logger.info(f"Saved {len(positions_table)} records to trusted layer")
    report_filename, validation_filename = create_lineage_report(
        config, validation_report, execution_id
    )
    mark_request_as_processed(config, logical_date_string)
    logger.info(f"Execution {execution_id} completed successfully")
    return {
        "execution_id": execution_id,
        "records_processed": len(positions_table),
        # "validation_passed": validation_results["overall_success"],
        # "lineage_report": report_filename,
        # "validation_report": validation_filename,
    }


def main():
    logical_date_string = (
        "2026-02-26T20:36:00+00:00"  # Replace with the actual logical_date_string
    )
    result = load_transform_save_positions(logical_date_string)
    logger.info(f"Pipeline result: {result}")


if __name__ == "__main__":
    main()

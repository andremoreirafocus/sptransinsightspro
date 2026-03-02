"""
transformlivedata-v4 with Great Expectations framework integration.

Adds column-level lineage tracking and data quality validations using GE patterns.
"""

from transformlivedata.services.load_positions import load_positions
from transformlivedata.services.transform_positions import transform_positions
from transformlivedata.services.save_positions_to_storage import (
    save_positions_to_storage,
)
from transformlivedata.services.validate_positions import (
    validate_raw_positions,
    validate_transformed_positions,
    generate_lineage_report,
)
from transformlivedata.services.processed_requests_helper import (
    mark_request_as_processed,
)
from transformlivedata.config import get_config
from transformlivedata.quality.ge_column_lineage import (
    build_transformlivedata_lineage,
    get_transformlivedata_output_columns,
)
from datetime import datetime
from zoneinfo import ZoneInfo
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import uuid

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
    Load, transform, and save positions with GE lineage tracking and validation.
    """
    # Create unique execution ID for tracking
    execution_id = str(uuid.uuid4())
    # Initialize Great Expectations framework
    lineage_tracker = build_transformlivedata_lineage(execution_id)
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
    # ============================================================================
    # LOAD STAGE: Load raw positions from MinIO
    # ============================================================================
    logger.info("=== LOAD STAGE: load_positions ===")
    raw_positions = load_positions(config, year, month, day, hour, minute)
    if not raw_positions:
        logger.error("No position data found to transform.")
        raise ValueError("No position data found to transform.")
    validate_raw_positions(raw_positions, execution_id)
    # ============================================================================
    # TRANSFORM STAGE: Transform positions with enrichment
    # ============================================================================
    logger.info("=== TRANSFORM STAGE: transform_positions ===")
    positions_table = transform_positions(config, raw_positions)
    if not positions_table:
        logger.error("No valid position records found after transformation.")
        raise ValueError("No valid position records found after transformation.")
    columns = get_transformlivedata_output_columns()
    df = pd.DataFrame(positions_table, columns=columns)
    logger.info(f"Transformed {len(df)} records")
    # ============================================================================
    # VALIDATE STAGE: Run Great Expectations validations
    # ============================================================================
    validation_results, validation_report, _ = validate_transformed_positions(
        raw_positions, df, execution_id
    )
    logger.info("=== SAVE STAGE: save_positions_to_storage ===")
    save_positions_to_storage(config, positions_table)
    logger.info(f"Saved {len(positions_table)} records to trusted layer")
    report_filename, validation_filename = generate_lineage_report(
        lineage_tracker, validation_report, execution_id
    )
    # ============================================================================
    # FINALIZE: Mark request as processed
    # ============================================================================
    mark_request_as_processed(config, logical_date_string)
    logger.info(f"Execution {execution_id} completed successfully")

    return {
        "execution_id": execution_id,
        "records_processed": len(df),
        "validation_passed": validation_results["overall_success"],
        "lineage_report": report_filename,
        "validation_report": validation_filename,
    }


def main():
    logical_date_string = (
        "2026-02-26T20:36:00+00:00"  # Replace with the actual logical_date_string
    )
    result = load_transform_save_positions(logical_date_string)
    logger.info(f"Pipeline result: {result}")


if __name__ == "__main__":
    main()

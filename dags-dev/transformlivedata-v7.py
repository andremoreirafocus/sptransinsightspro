"""
transformlivedata-v7 with JSON-driven raw validation integration.

Adds RawDataExpectations (jsonschema-driven) validation for raw API responses
before any data transformation. Built on top of v6 with minimal integration points.
"""

from transformlivedata.services.load_positions import load_positions
from transformlivedata.services.transform_positions import transform_positions
from transformlivedata.services.save_positions_to_storage import (
    save_positions_to_storage,
)
from transformlivedata.services.validate_positions import (
    validate_raw_positions,
    validate_transformed_positions,
    create_lineage_report,
)
from transformlivedata.services.processed_requests_helper import (
    mark_request_as_processed,
)
from transformlivedata.config import get_config
from transformlivedata.quality.raw_data_expectations import RawDataExpectations
from transformlivedata.quality.transformed_data_expectations import (
    TransformedDataExpectations,
)
from transformlivedata.quality.gx_data_docs import GXDataDocsGenerator
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

# Initialize RawDataExpectations validator
# Get the directory where this script is located


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
    # ============================================================================
    # LOAD STAGE: Load raw positions from MinIO
    # ============================================================================
    logger.info("=== LOAD STAGE: load_positions ===")
    raw_positions = load_positions(config, year, month, day, hour, minute)
    if not raw_positions:
        logger.error("No position data found to transform.")
        raise ValueError("No position data found to transform.")

    # ============================================================================
    # PHASE 2 INTEGRATION: Raw Data Validation (RawDataExpectations)
    # ============================================================================
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

    metadata, raw_schema_validation = validate_raw_positions(
        config, raw_positions, execution_id
    )
    # ============================================================================
    # TRANSFORM STAGE: Transform positions with enrichment
    # ============================================================================
    logger.info("=== TRANSFORM STAGE: transform_positions ===")
    transform_result = transform_positions(config, raw_positions)
    if not transform_result or not transform_result.get("positions_table"):
        logger.error("No valid position records found after transformation.")
        raise ValueError("No valid position records found after transformation.")

    positions_table = transform_result["positions_table"]
    logger.info(f"Transformation metrics: {transform_result['metrics']}")
    if transform_result["issues"]:
        logger.warning(f"Transformation issues detected: {transform_result['issues']}")
    logger.info(f"Quality score: {transform_result['quality_score']}%")

    # ============================================================================
    # PHASE 5 INTEGRATION: Transformed Data Validation (TransformedDataExpectations)
    # ============================================================================
    logger.info("=== PHASE 5: TRANSFORMED DATA VALIDATION (GX) ===")
    transformed_expectations_config = os.path.join(
        script_dir, "transformlivedata", "config", "expectations.json"
    )
    transformed_validator = TransformedDataExpectations(
        expectations_config=transformed_expectations_config
    )

    # Convert positions_table list to DataFrame for GX validation
    import pandas as pd

    # Extract column names from expectations.json
    expectations_list = transformed_validator.get_expectations_list()
    column_names = None
    for expectation in expectations_list:
        if (
            expectation.get("expectation_type")
            == "expect_table_columns_to_match_ordered_list"
        ):
            column_names = expectation.get("kwargs", {}).get("column_list")
            break

    if column_names:
        positions_df = pd.DataFrame(positions_table, columns=column_names)
    else:
        # Fallback if no column_list found in expectations
        positions_df = pd.DataFrame(positions_table)

    gx_result = transformed_validator.validate_output_dataframe(positions_df)
    if not gx_result["success"]:
        # Log detailed validation violations as warnings (do NOT fail pipeline)
        logger.warning("=== TRANSFORMED DATA VALIDATION VIOLATIONS ===")
        validation_results = gx_result["validation_results"]["results"]
        for i, result in enumerate(validation_results, 1):
            if not result.get("success", False):
                exp_type = result["expectation_type"]
                message = result.get("message", "Unknown error")
                logger.warning(f"  {i}. [{exp_type}] {message}")
                if "kwargs" in result and result["kwargs"]:
                    logger.warning(f"     Params: {result['kwargs']}")

        passed = gx_result["validation_results"]["passed"]
        total = gx_result["expectation_count"]
        logger.warning(
            f"Transformed data validation warnings: {passed}/{total} expectations passed. "
            f"Pipeline continues (validation issues logged for review)."
        )
    else:
        logger.info(
            f"Transformed data validation passed ✓ "
            f"({gx_result['expectation_count']} expectations evaluated)"
        )

    # ============================================================================
    # PHASE 7 INTEGRATION: GX Data Docs Generation
    # ============================================================================
    logger.info("=== PHASE 7: GX DATA DOCS GENERATION ===")
    docs_generator = GXDataDocsGenerator(
        docs_root_dir=os.path.join(script_dir, "transformlivedata", "gx_data_docs")
    )
    docs_path = docs_generator.generate_docs(gx_result)
    logger.info(f"GX data docs generated at {docs_path}")

    # ============================================================================
    # VALIDATE STAGE: Validate transformed positions
    # ============================================================================
    validation_results, validation_report, df = validate_transformed_positions(
        config,
        raw_positions,
        positions_table,
        execution_id,
        transform_result,
        raw_schema_validation,
    )
    logger.info("=== SAVE STAGE: save_positions_to_storage ===")
    save_positions_to_storage(config, positions_table)
    logger.info(f"Saved {len(positions_table)} records to trusted layer")
    # Initialize Great Expectations framework
    report_filename, validation_filename = create_lineage_report(
        config, validation_report, execution_id
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

"""
Data validation service for transformlivedata pipeline.

Provides validation functions for raw and transformed position data,
including generation of validation and lineage reports.
"""

from typing import Dict, Any, Tuple
import logging
import pandas as pd
from transformlivedata.quality.ge_expectations import DataExpectations
from transformlivedata.quality.ge_column_lineage import build_transformlivedata_lineage

logger = logging.getLogger(__name__)


def validate_raw_positions(
    raw_positions: Dict[str, Any], execution_id: str
) -> Dict[str, Any]:
    """
    Validate raw positions API response structure.

    Checks that raw positions data has required fields and structure.
    Also extracts and logs metadata information.

    Args:
        raw_positions: Raw API response dictionary with metadata and payload
        execution_id: Unique execution identifier for tracking

    Returns:
        Dictionary with metadata information (total_vehicles, source, extracted_at)

    Raises:
        ValueError: If raw positions structure is invalid
    """
    logger.info("Validating raw positions structure...")
    expectations = DataExpectations(execution_id)
    is_valid, errors = expectations.expect_raw_positions_structure(raw_positions)
    if not is_valid:
        logger.error(f"Raw positions structure validation failed: {errors}")
        raise ValueError(f"Invalid raw positions structure: {errors}")
    logger.info("✓ Raw positions structure valid")
    # Record lineage inputs from metadata
    metadata = raw_positions.get("metadata", {})
    logger.info(
        f"Loaded {metadata.get('total_vehicles', 0)} vehicles "
        f"from {metadata.get('source', 'unknown')} "
        f"at {metadata.get('extracted_at', 'unknown')}"
    )
    return metadata


def validate_transformed_positions(
    raw_positions: Dict[str, Any],
    df: pd.DataFrame,
    execution_id: str,
) -> Tuple[Dict[str, Any], str, bool]:
    """
    Validate transformed positions data using Great Expectations framework.

    Runs all configured data quality validations and generates a validation report.

    Args:
        raw_positions: Original raw API response (for reference validations)
        df: Transformed DataFrame with position data
        execution_id: Unique execution identifier for tracking

    Returns:
        Tuple of:
        - validation_results: Dictionary with detailed validation results
        - validation_report: Human-readable validation report string
        - success: Boolean indicating if all validations passed
    """
    logger.info("=== VALIDATE STAGE: Running expectations ===")
    expectations = DataExpectations(execution_id)
    validation_results = expectations.run_all_validations(
        raw_data=raw_positions, output_df=df
    )
    # Log validation results
    validation_report = expectations.generate_validation_report(validation_results)
    logger.info(validation_report)

    if not validation_results["overall_success"]:
        logger.warning(
            f"Data quality validations failed: {validation_results['failure_count']} failures"
        )
    else:
        logger.info("✓ All data quality expectations passed")
    return validation_results, validation_report, validation_results["overall_success"]


def generate_lineage_report(
    lineage_tracker,
    validation_report: str,
    execution_id: str,
) -> Tuple[str, str]:
    """
    Generate and save lineage and validation reports to files.

    Creates human-readable reports documenting:
    - Column-level lineage (input → transformation → output)
    - Data quality validation results

    Args:
        lineage_tracker: ColumnLineageTracker instance with lineage mappings
        validation_report: Pre-generated validation report string
        execution_id: Unique execution identifier (used for filenames)

    Returns:
        Tuple of (lineage_report_filename, validation_report_filename)
    """
    logger.info("=== GENERATING LINEAGE REPORT ===")
    # Save lineage report to file
    lineage_report_filename = f"column_lineage_report_{execution_id}.txt"
    lineage_tracker.write_lineage_report(lineage_report_filename)
    logger.info(f"Lineage report saved to {lineage_report_filename}")
    # Save validation report to file
    validation_report_filename = f"validation_report_{execution_id}.txt"
    with open(validation_report_filename, "w") as f:
        f.write(validation_report)
    logger.info(f"Validation report saved to {validation_report_filename}")
    return lineage_report_filename, validation_report_filename

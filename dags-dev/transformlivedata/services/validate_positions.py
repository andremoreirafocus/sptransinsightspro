"""
Data validation service for transformlivedata pipeline.

Provides validation functions for raw and transformed position data,
including generation of validation and lineage reports.
"""

from typing import Dict, Any, List, Tuple
import logging
import pandas as pd
import os
import json
from transformlivedata.quality.ge_expectations import (
    DataExpectations,
)
from transformlivedata.quality.ge_column_lineage import (
    build_transformlivedata_lineage,
    get_transformlivedata_output_columns,
)

logger = logging.getLogger(__name__)


def load_quality_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Load column lineage configuration from external JSON file.

    Args:
        config: Dictionary with loaded configuration

    Returns:
        Dictionary with 'columns' key containing all column mappings

    Raises:
        FileNotFoundError: If config file not found
        json.JSONDecodeError: If config file is invalid JSON
    """

    def get_config():
        try:
            config_path = config["DATA_QUALITY_CONFIG"]
            print(f"Using data quality config from {config_path}")
        except KeyError:
            raise KeyError("Missing 'DATA_QUALITY_CONFIG' in configuration. ")
        return config_path

    config_path = get_config()
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Data quality configuration not found at {config_path}\n"
        )
    logger.info(f"Loading data quality configuration from {config_path}")
    with open(config_path, "r") as f:
        quality_config = json.load(f)
    if not quality_config or "columns" not in quality_config:
        raise ValueError(
            "Invalid data quality configuration: missing 'columns' section"
        )
    logger.info(
        f"Loaded data quality configuration with {len(quality_config['columns'])} columns"
    )
    return quality_config


def validate_raw_positions(
    config: Dict[str, Any], raw_positions: Dict[str, Any], execution_id: str
) -> Dict[str, Any]:
    """
    Validate raw positions API response structure.

    Checks that raw positions data has required fields and structure.
    Also extracts and logs metadata information.

    Args:
        config: Configuration dictionary
        raw_positions: Raw API response dictionary with metadata and payload
        execution_id: Unique execution identifier for tracking

    Returns:
        Dictionary with metadata information (total_vehicles, source, extracted_at)

    Raises:
        ValueError: If raw positions structure is invalid
    """
    logger.info("Validating raw positions structure...")
    quality_config = load_quality_config(config)
    expectations = DataExpectations(quality_config, execution_id)
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
    config: Dict[str, Any],
    raw_positions: Dict[str, Any],
    positions_table: List[Dict[str, Any]],
    execution_id: str,
    transformation_result: Dict[str, Any] = None,
) -> Tuple[Dict[str, Any], str, pd.DataFrame]:
    """
    Transform positions data to DataFrame and validate using Great Expectations framework.

    Converts raw position list to DataFrame with schema from validation-schema.json,
    then runs all configured data quality validations and generates a validation report
    that includes transformation metrics and issues.

    Args:
        raw_positions: Original raw API response (for reference validations)
        positions_table: List of position dictionaries from transform stage
        execution_id: Unique execution identifier for tracking
        transformation_result: Optional dict containing transformation metrics and issues

    Returns:
        Tuple of:
        - validation_results: Dictionary with detailed validation results
        - validation_report: Human-readable validation report string
        - df: Transformed DataFrame with validated data

    Raises:
        ValueError: If positions_table is empty or DataFrame creation fails
    """
    quality_config = load_quality_config(config)
    columns = get_transformlivedata_output_columns(quality_config)
    df = pd.DataFrame(positions_table, columns=columns)
    logger.info(f"Transformed {len(df)} records")
    logger.info("=== VALIDATE STAGE: Running expectations ===")
    expectations = DataExpectations(quality_config, execution_id)
    validation_results = expectations.run_all_validations(
        raw_data=raw_positions, output_df=df
    )
    # Integrate transformation metrics into validation results if available
    if transformation_result:
        validation_results["transformation_metrics"] = transformation_result["metrics"]
        validation_results["transformation_issues"] = transformation_result["issues"]
        validation_results["transformation_quality_score"] = transformation_result[
            "quality_score"
        ]

    # Log validation results
    validation_report = expectations.generate_validation_report(validation_results)
    logger.info(validation_report)
    if not validation_results["overall_success"]:
        logger.warning(
            f"Data quality validations failed: {validation_results['failure_count']} failures"
        )
    else:
        logger.info("✓ All data quality expectations passed")
    return validation_results, validation_report, df


def create_lineage_report(
    config: Dict[str, Any],
    validation_report: str,
    execution_id: str,
) -> Tuple[str, str]:
    """
    Generate and save lineage and validation reports to files.

    Creates human-readable reports documenting:
    - Column-level lineage (input → transformation → output)
    - Data quality validation results

    Args:
        config: Dictionary with loaded configuration
        validation_report: Pre-generated validation report string
        execution_id: Unique execution identifier (used for filenames)

    Returns:
        Tuple of (lineage_report_filename, validation_report_filename)
    """
    logger.info("=== GENERATING LINEAGE REPORT ===")
    # Save lineage report to file
    lineage_report_filename = f"column_lineage_report_{execution_id}.txt"
    quality_config = load_quality_config(config)
    lineage_tracker = build_transformlivedata_lineage(quality_config, execution_id)
    lineage_tracker.write_lineage_report(lineage_report_filename)
    logger.info(f"Lineage report saved to {lineage_report_filename}")
    # Save validation report to file
    validation_report_filename = f"validation_report_{execution_id}.txt"
    with open(validation_report_filename, "w") as f:
        f.write(validation_report)
    logger.info(f"Validation report saved to {validation_report_filename}")
    return lineage_report_filename, validation_report_filename

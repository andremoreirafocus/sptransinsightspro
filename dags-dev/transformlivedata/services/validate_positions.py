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


# Raw schema definition: critical vs optional fields
RAW_SCHEMA = {
    "metadata": {
        "critical": {"extracted_at": str},
        "optional": {"source": str, "total_vehicles": int},
    },
    "payload": {
        "critical": {"hr": (int, list, str), "l": list},
        "optional": {},
    },
    "line": {
        "critical": {"c": str, "cl": int, "sl": int, "vs": list},
        "optional": {"lt0": str, "lt1": str, "qv": int},
    },
    "vehicle": {
        "critical": {
            "p": int,
            "a": bool,
            "ta": str,
            "py": (int, float),
            "px": (int, float),
        },
        "optional": {},
    },
}


def validate_field_type(value: Any, expected_type) -> bool:
    """
    Validate if value matches expected type.
    Handles tuples of types (e.g., (int, float)).
    """
    if expected_type is None:
        return value is None
    if isinstance(expected_type, tuple):
        return isinstance(value, expected_type)
    return isinstance(value, expected_type)


def validate_raw_schema(raw_positions: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate raw positions API response schema.

    Checks critical fields (must exist and have correct type) and optional fields.
    Raises exception only if critical fields fail.

    Args:
        raw_positions: Raw API response dictionary

    Returns:
        Dict with:
        - schema_valid: bool (critical fields valid)
        - critical_field_errors: [list of critical failures]
        - optional_field_warnings: [list of optional field issues]

    Raises:
        ValueError: If critical fields are missing or have wrong type
    """
    critical_errors = []
    optional_warnings = []

    # Validate metadata critical fields
    metadata = raw_positions.get("metadata")
    if metadata is None:
        critical_errors.append("Missing 'metadata' at root level")
    else:
        for field, expected_type in RAW_SCHEMA["metadata"]["critical"].items():
            if field not in metadata:
                critical_errors.append(f"Critical field 'metadata.{field}' is missing")
            elif not validate_field_type(metadata.get(field), expected_type):
                critical_errors.append(
                    f"Critical field 'metadata.{field}' has wrong type: "
                    f"expected {expected_type.__name__}, got {type(metadata.get(field)).__name__}"
                )

        # Check optional metadata fields
        for field, expected_type in RAW_SCHEMA["metadata"]["optional"].items():
            if field in metadata and not validate_field_type(
                metadata.get(field), expected_type
            ):
                optional_warnings.append(
                    f"Optional field 'metadata.{field}' has wrong type: "
                    f"expected {expected_type.__name__}, got {type(metadata.get(field)).__name__}"
                )

    # Validate payload critical fields
    payload = raw_positions.get("payload")
    if payload is None:
        critical_errors.append("Missing 'payload' at root level")
    else:
        for field, expected_type in RAW_SCHEMA["payload"]["critical"].items():
            if field not in payload:
                critical_errors.append(f"Critical field 'payload.{field}' is missing")
            elif not validate_field_type(payload.get(field), expected_type):
                critical_errors.append(
                    f"Critical field 'payload.{field}' has wrong type: "
                    f"expected {expected_type}, got {type(payload.get(field)).__name__}"
                )

    # Validate lines structure
    if payload and "l" in payload and isinstance(payload["l"], list):
        for line_idx, line in enumerate(payload["l"]):
            for field, expected_type in RAW_SCHEMA["line"]["critical"].items():
                if field not in line:
                    critical_errors.append(
                        f"Critical field 'line[{line_idx}].{field}' is missing"
                    )
                elif not validate_field_type(line.get(field), expected_type):
                    critical_errors.append(
                        f"Critical field 'line[{line_idx}].{field}' has wrong type: "
                        f"expected {expected_type}, got {type(line.get(field)).__name__}"
                    )

            # Check optional line fields
            for field, expected_type in RAW_SCHEMA["line"]["optional"].items():
                if field in line and not validate_field_type(
                    line.get(field), expected_type
                ):
                    optional_warnings.append(
                        f"Optional field 'line[{line_idx}].{field}' has wrong type: "
                        f"expected {expected_type.__name__}, got {type(line.get(field)).__name__}"
                    )

            # Validate vehicles structure
            if "vs" in line and isinstance(line["vs"], list):
                for vehicle_idx, vehicle in enumerate(line["vs"]):
                    for field, expected_type in RAW_SCHEMA["vehicle"][
                        "critical"
                    ].items():
                        if field not in vehicle:
                            critical_errors.append(
                                f"Critical field 'line[{line_idx}].vs[{vehicle_idx}].{field}' is missing"
                            )
                        elif not validate_field_type(vehicle.get(field), expected_type):
                            critical_errors.append(
                                f"Critical field 'line[{line_idx}].vs[{vehicle_idx}].{field}' has wrong type: "
                                f"expected {expected_type}, got {type(vehicle.get(field)).__name__}"
                            )

    # Prepare result
    schema_valid = len(critical_errors) == 0

    if critical_errors:
        logger.error(
            f"Raw schema validation failed with {len(critical_errors)} critical errors"
        )
        for error in critical_errors:
            logger.error(f"  ✗ {error}")
        raise ValueError(f"Raw schema contract violation: {critical_errors[0]}")

    if optional_warnings:
        logger.warning(f"Raw schema has {len(optional_warnings)} optional field issues")
        for warning in optional_warnings:
            logger.warning(f"  ⚠ {warning}")

    return {
        "schema_valid": schema_valid,
        "critical_field_errors": critical_errors,
        "optional_field_warnings": optional_warnings,
    }


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
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Validate raw positions API response (both schema and structure).

    Phase 1 Validation:
    - Raw schema validation: field existence and types
    - Raw structure validation: payload organization

    Args:
        config: Configuration dictionary
        raw_positions: Raw API response dictionary with metadata and payload
        execution_id: Unique execution identifier for tracking

    Returns:
        Tuple of:
        - metadata: Dictionary with metadata information (total_vehicles, source, extracted_at)
        - raw_schema_validation: Dictionary with schema validation results

    Raises:
        ValueError: If raw schema contract is violated
    """
    logger.info("=== PHASE 1: RAW LAYER VALIDATION ===")

    # Step 1: Validate schema (critical and optional fields)
    logger.info("Validating raw schema (field existence and types)...")
    raw_schema_validation = validate_raw_schema(raw_positions)
    logger.info("✓ Raw schema validation passed")

    # Step 2: Validate structure (organize check)
    logger.info("Validating raw positions structure...")
    quality_config = load_quality_config(config)
    expectations = DataExpectations(quality_config, execution_id)
    is_valid, errors = expectations.expect_raw_positions_structure(raw_positions)
    if not is_valid:
        logger.error(f"Raw positions structure validation failed: {errors}")
        raise ValueError(f"Invalid raw positions structure: {errors}")
    logger.info("✓ Raw positions structure valid")

    # Extract metadata
    metadata = raw_positions.get("metadata", {})
    logger.info(
        f"Loaded {metadata.get('total_vehicles', 0)} vehicles "
        f"from {metadata.get('source', 'unknown')} "
        f"at {metadata.get('extracted_at', 'unknown')}"
    )

    return metadata, raw_schema_validation


def validate_transformed_positions(
    config: Dict[str, Any],
    raw_positions: Dict[str, Any],
    positions_table: List[Dict[str, Any]],
    execution_id: str,
    transformation_result: Dict[str, Any] = None,
    raw_schema_validation: Dict[str, Any] = None,
) -> Tuple[Dict[str, Any], str, pd.DataFrame]:
    """
    Transform positions data to DataFrame and validate using Great Expectations framework.

    Phase 3 Validation:
    - Validates transformed output data quality
    - Includes transformation metrics and issues from Phase 2
    - Includes raw schema validation results from Phase 1

    Args:
        raw_positions: Original raw API response (for reference validations)
        positions_table: List of position dictionaries from transform stage
        execution_id: Unique execution identifier for tracking
        transformation_result: Optional dict with transformation metrics and issues (Phase 2)
        raw_schema_validation: Optional dict with raw schema validation results (Phase 1)

    Returns:
        Tuple of:
        - validation_results: Dictionary with all validation phases results
        - validation_report: Human-readable validation report string
        - df: Transformed DataFrame with validated data

    Raises:
        ValueError: If positions_table is empty or DataFrame creation fails
    """
    logger.info("=== PHASE 3: TRUSTED LAYER VALIDATION ===")

    quality_config = load_quality_config(config)
    columns = get_transformlivedata_output_columns(quality_config)
    df = pd.DataFrame(positions_table, columns=columns)
    logger.info(f"Transformed {len(df)} records")
    logger.info("Running data quality expectations...")
    expectations = DataExpectations(quality_config, execution_id)
    validation_results = expectations.run_all_validations(
        raw_data=raw_positions, output_df=df
    )

    # Integrate Phase 1: Raw schema validation
    if raw_schema_validation:
        validation_results["raw_schema_validation"] = raw_schema_validation

    # Integrate Phase 2: Transformation metrics
    if transformation_result:
        validation_results["transformation_metrics"] = transformation_result["metrics"]
        validation_results["transformation_issues"] = transformation_result["issues"]
        validation_results["transformation_quality_score"] = transformation_result[
            "quality_score"
        ]

    # Generate comprehensive validation report
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

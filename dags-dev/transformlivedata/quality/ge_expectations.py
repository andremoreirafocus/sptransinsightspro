"""
Great Expectations-based data validation and profiling for transformlivedata.

Defines expectations (assertions) about data structure and quality,
generating data docs and validation reports.
"""

from typing import Dict, Any, List, Tuple
import logging
import pandas as pd
from datetime import datetime
from transformlivedata.quality.ge_column_lineage import (
    get_transformlivedata_column_lineage,
)

logger = logging.getLogger(__name__)


def get_transformlivedata_expectations(
    quality_config: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """
    Get data quality expectations configuration for transformlivedata pipeline.

    Loads from external YAML file.

    Args:
        quality_config: Dictionary with loaded quality configuration

    Returns:
        Dictionary mapping expectation_name → {enabled, type, params, ...}
    """
    if "expectations" not in quality_config:
        logger.warning("No expectations found in data quality configuration file")
        return {}
    expectations = quality_config["expectations"]
    logger.info(
        f"Loaded {len(expectations)} expectations from data quality configuration file: "
        f"{', '.join(e for e, v in expectations.items() if v.get('enabled', False))}"
    )
    return expectations


class DataExpectations:
    """Define and validate data expectations for transformlivedata pipeline."""

    def __init__(self, config: Dict[str, Any], execution_id: str):
        """Initialize expectations tracker."""
        self.config = config
        self.execution_id = execution_id
        self.expectations: Dict[str, Any] = {}
        self.validation_results: Dict[str, bool] = {}

    # INPUT SCHEMA EXPECTATIONS
    def expect_raw_positions_structure(
        self, data: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        """Validate raw positions API response structure.
        Configuration REQUIRED in validation-schema.yml expectations.raw_positions_structure
        """
        expectations_config = get_transformlivedata_expectations(self.config)
        if "raw_positions_structure" not in expectations_config:
            raise ValueError(
                "Configuration 'expectations.raw_positions_structure' not found in validation-schema.yml. "
                "This expectation must be explicitly configured."
            )
        struct_config = expectations_config["raw_positions_structure"]
        if "required_keys" not in struct_config:
            raise ValueError(
                "Configuration 'expectations.raw_positions_structure.required_keys' not found in validation-schema.yml. "
                "Must define: required_keys: [list of root-level keys]"
            )
        if "payload_required_keys" not in struct_config:
            raise ValueError(
                "Configuration 'expectations.raw_positions_structure.payload_required_keys' not found in validation-schema.yml. "
                "Must define: payload_required_keys: [list of payload keys]"
            )
        required_root_keys = struct_config["required_keys"]
        payload_required_keys = struct_config["payload_required_keys"]
        errors = []
        for key in required_root_keys:
            if key not in data:
                errors.append(f"Missing '{key}' in API response")
        if "payload" in data:
            payload = data["payload"]
            for key in payload_required_keys:
                if key not in payload:
                    errors.append(f"Missing payload.{key}")
        return len(errors) == 0, errors

    # OUTPUT SCHEMA EXPECTATIONS
    def expect_output_columns_exist(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate all required output columns exist in transformed data.
        Columns are sourced from lineage configuration (validation-schema.yml).
        """
        try:
            lineage_config = get_transformlivedata_column_lineage(self.config)
            required_columns = list(lineage_config.keys())
        except (FileNotFoundError, ValueError) as e:
            logger.error(f"Failed to load lineage configuration: {e}")
            return False, [f"Could not load lineage config: {str(e)}"]
        missing = [col for col in required_columns if col not in df.columns]
        return len(missing) == 0, [f"Missing columns: {missing}"] if missing else []

    def expect_column_data_types(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate output column data types from configuration."""
        expectations_config = get_transformlivedata_expectations(self.config)
        if "column_data_types" not in expectations_config:
            raise ValueError(
                "Configuration 'expectations.column_data_types' not found in validation-schema.yml. "
                "This expectation must be explicitly configured."
            )
        column_types_config = expectations_config["column_data_types"]
        if "expectations" not in column_types_config:
            raise ValueError(
                "Configuration 'expectations.column_data_types.expectations' not found in validation-schema.yml. "
                "Must define: expectations: {column_name: data_type, ...}"
            )
        expected_types = column_types_config["expectations"]
        errors = []
        for col, expected in expected_types.items():
            if col in df.columns:
                actual = str(df[col].dtype)
                if expected not in actual:
                    errors.append(f"{col}: expected {expected}, got {actual}")
        return len(errors) == 0, errors

    # DATA QUALITY EXPECTATIONS
    def expect_vehicle_ids_not_null(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate vehicle_id is never null."""
        expectations_config = get_transformlivedata_expectations(self.config)
        vehicle_id_config = expectations_config.get("vehicle_ids_not_null", {})
        if "column" not in vehicle_id_config:
            raise ValueError(
                "Configuration 'expectations.vehicle_ids_not_null.column' not found in validation-schema.yml. "
                "Must define: column: <column_name>"
            )
        col_vehicle_id = vehicle_id_config["column"]
        if col_vehicle_id not in df.columns:
            return False, [f"Column {col_vehicle_id} not found"]
        null_count = df[col_vehicle_id].isna().sum()
        if null_count > 0:
            return False, [f"Found {null_count} null values in {col_vehicle_id}"]
        return True, []

    def expect_coordinates_in_rmsp(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate coordinates are within RMSP (São Paulo metro region) with tolerance.
        Bounds and tolerance REQUIRED in validation-schema.yml expectations.coordinates_in_rmsp
        """
        expectations_config = get_transformlivedata_expectations(self.config)
        coord_config = expectations_config.get("coordinates_in_rmsp", {})
        columns_config = coord_config.get("columns", {})
        if "latitude" not in columns_config or "longitude" not in columns_config:
            raise ValueError(
                "Configuration 'expectations.coordinates_in_rmsp.columns' not found in validation-schema.yml. "
                "Must define: columns: {latitude: <col_name>, longitude: <col_name>}"
            )
        col_lat = columns_config["latitude"]
        col_lon = columns_config["longitude"]
        if col_lat not in df.columns or col_lon not in df.columns:
            return False, ["Coordinate columns not found"]
        if "coordinates_in_rmsp" not in expectations_config:
            raise ValueError(
                "Configuration 'expectations.coordinates_in_rmsp' not found in validation-schema.yml. "
                "This expectation must be explicitly configured."
            )
        if "bounds" not in coord_config:
            raise ValueError(
                "Configuration 'expectations.coordinates_in_rmsp.bounds' not found in validation-schema.yml. "
                "Must define: bounds: {latitude_min_strict, latitude_max_strict, longitude_min_strict, longitude_max_strict, tolerance_km}"
            )
        bounds = coord_config["bounds"]
        required_bounds = [
            "latitude_min_strict",
            "latitude_max_strict",
            "longitude_min_strict",
            "longitude_max_strict",
            "tolerance_km",
        ]
        missing_bounds = [b for b in required_bounds if b not in bounds]
        if missing_bounds:
            raise ValueError(
                f"Missing required bounds in expectations.coordinates_in_rmsp.bounds: {missing_bounds}"
            )
        lat_min = bounds["latitude_min_strict"]
        lat_max = bounds["latitude_max_strict"]
        lon_min = bounds["longitude_min_strict"]
        lon_max = bounds["longitude_max_strict"]
        tolerance_km = bounds["tolerance_km"]
        tolerance_degrees = tolerance_km / 111.0
        lat_min_with_tolerance = lat_min - tolerance_degrees
        lat_max_with_tolerance = lat_max + tolerance_degrees
        lon_min_with_tolerance = lon_min - tolerance_degrees
        lon_max_with_tolerance = lon_max + tolerance_degrees
        valid_lat = (df[col_lat] >= lat_min_with_tolerance) & (
            df[col_lat] <= lat_max_with_tolerance
        )
        valid_lon = (df[col_lon] >= lon_min_with_tolerance) & (
            df[col_lon] <= lon_max_with_tolerance
        )
        invalid_count = (~(valid_lat & valid_lon)).sum()
        if invalid_count > 0:
            return False, [
                f"Found {invalid_count} coordinates outside RMSP bounds (with {tolerance_km}km tolerance)"
            ]
        return True, []

    def expect_distances_non_negative(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate distance calculations are non-negative."""
        expectations_config = get_transformlivedata_expectations(self.config)
        distances_config = expectations_config.get("distances_non_negative", {})
        if "columns" not in distances_config:
            raise ValueError(
                "Configuration 'expectations.distances_non_negative.columns' not found in validation-schema.yml. "
                "Must define: columns: [list of distance column names]"
            )
        distance_columns = distances_config["columns"]
        errors = []
        for col in distance_columns:
            if col in df.columns:
                negative = (df[col] < 0).sum()
                if negative > 0:
                    errors.append(f"Found {negative} negative values in {col}")
        return len(errors) == 0, errors

    def run_all_validations(
        self,
        raw_data: Dict[str, Any] = None,
        vehicles: List[Dict[str, Any]] = None,
        output_df: pd.DataFrame = None,
    ) -> Dict[str, Any]:
        """Run expectations configured in validation-schema.yml.
        Only runs validations marked as enabled: true in expectations section.
        """
        try:
            expectations_config = get_transformlivedata_expectations(self.config)
        except Exception as e:
            logger.warning(
                f"Could not load expectations config: {e}. Running all validations."
            )
            expectations_config = {}
        results = {
            "execution_id": self.execution_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "validations": {},
            "overall_success": True,
            "failure_count": 0,
            "config_source": "validation-schema.yml"
            if expectations_config
            else "hardcoded",
        }

        def should_run(expectation_name: str) -> bool:
            if expectation_name not in expectations_config:
                raise ValueError(
                    f"Expectation '{expectation_name}' not configured in validation-schema.yml. "
                    f"All expectations must be explicitly defined in expectations section. "
                    f"Available: {list(expectations_config.keys())}"
                )
            config = expectations_config[expectation_name]
            if "enabled" not in config:
                raise ValueError(
                    f"Configuration 'expectations.{expectation_name}.enabled' not found in validation-schema.yml. "
                    f"Must define: enabled: true or enabled: false"
                )
            return config["enabled"]

        if raw_data and should_run("raw_positions_structure"):
            is_valid, errors = self.expect_raw_positions_structure(raw_data)
            results["validations"]["raw_positions_structure"] = {
                "passed": is_valid,
                "errors": errors,
                "config_driven": "raw_positions_structure" in expectations_config,
            }
            if not is_valid:
                results["overall_success"] = False
                results["failure_count"] += 1
        if vehicles and should_run("vehicle_fields"):
            invalid_vehicles = []
            for i, vehicle in enumerate(vehicles):
                is_valid, errors = self.expect_vehicle_fields(vehicle)
                if not is_valid:
                    invalid_vehicles.append({"index": i, "errors": errors})
            results["validations"]["vehicle_fields"] = {
                "passed": len(invalid_vehicles) == 0,
                "invalid_count": len(invalid_vehicles),
                "samples": invalid_vehicles[:5] if invalid_vehicles else [],
                "config_driven": "vehicle_fields" in expectations_config,
            }
            if invalid_vehicles:
                results["overall_success"] = False
                results["failure_count"] += 1
        if output_df is not None:
            if should_run("output_columns_exist"):
                col_check, col_errors = self.expect_output_columns_exist(output_df)
                results["validations"]["output_columns_exist"] = {
                    "passed": col_check,
                    "errors": col_errors,
                    "config_driven": "output_columns_exist" in expectations_config,
                }
            if should_run("column_data_types"):
                type_check, type_errors = self.expect_column_data_types(output_df)
                results["validations"]["column_data_types"] = {
                    "passed": type_check,
                    "errors": type_errors,
                    "config_driven": "column_data_types" in expectations_config,
                }
            if should_run("vehicle_ids_not_null"):
                null_check, null_errors = self.expect_vehicle_ids_not_null(output_df)
                results["validations"]["vehicle_ids_not_null"] = {
                    "passed": null_check,
                    "errors": null_errors,
                    "config_driven": "vehicle_ids_not_null" in expectations_config,
                }
            if should_run("coordinates_in_rmsp"):
                coord_check, coord_errors = self.expect_coordinates_in_rmsp(output_df)
                results["validations"]["coordinates_in_rmsp"] = {
                    "passed": coord_check,
                    "error_count": len(coord_errors),
                    "errors": coord_errors,
                    "config_driven": "coordinates_in_rmsp" in expectations_config,
                }
            if should_run("distances_non_negative"):
                dist_check, dist_errors = self.expect_distances_non_negative(output_df)
                results["validations"]["distances_non_negative"] = {
                    "passed": dist_check,
                    "errors": dist_errors,
                    "config_driven": "distances_non_negative" in expectations_config,
                }
            validation_results = []
            for k in [
                "output_columns_exist",
                "column_data_types",
                "vehicle_ids_not_null",
                "coordinates_in_rmsp",
                "distances_non_negative",
            ]:
                if k in results["validations"]:
                    if "passed" not in results["validations"][k]:
                        raise KeyError(
                            f"Validation result for '{k}' missing required 'passed' key. "
                            f"Got: {results['validations'][k].keys()}"
                        )
                    validation_results.append(results["validations"][k]["passed"])
            if validation_results and not all(validation_results):
                results["overall_success"] = False
                results["failure_count"] += sum(1 for v in validation_results if not v)
        return results

    def generate_validation_report(self, results: Dict[str, Any]) -> str:
        """Generate human-readable validation report including transformation metrics."""
        lines = [
            "=" * 80,
            "DATA EXPECTATIONS VALIDATION REPORT",
            "=" * 80,
            f"Execution ID: {results['execution_id']}",
            f"Timestamp: {results['timestamp']}",
            f"Overall Result: {'✓ PASSED' if results['overall_success'] else '✗ FAILED'}",
            f"Failures: {results['failure_count']}",
            "",
            "DATA QUALITY VALIDATIONS:",
            "-" * 80,
        ]

        for check_name, check_result in results["validations"].items():
            status = "✓" if check_result.get("passed") else "✗"
            lines.append(f"{status} {check_name}")
            if "errors" in check_result and check_result["errors"]:
                for error in check_result["errors"]:
                    lines.append(f"    → {error}")
            if "invalid_count" in check_result:
                lines.append(f"    Count: {check_result['invalid_count']}")
            if "error_count" in check_result:
                lines.append(f"    Errors: {check_result['error_count']}")

        # Add transformation metrics if available
        if "transformation_metrics" in results:
            lines.extend(
                [
                    "",
                    "TRANSFORMATION METRICS:",
                    "-" * 80,
                    f"Total Vehicles Processed: {results['transformation_metrics']['total_vehicles_processed']}",
                    f"Valid Vehicles: {results['transformation_metrics']['valid_vehicles']}",
                    f"Invalid Vehicles: {results['transformation_metrics']['invalid_vehicles']}",
                    f"Lines Processed: {results['transformation_metrics']['total_lines_processed']}",
                    f"Quality Score: {results['transformation_quality_score']}%",
                ]
            )

            # Add transformation issues if any
            issues = results["transformation_issues"]
            if (
                issues["invalid_trips"]
                or issues["distance_calculation_errors"]
                or issues["vehicle_count_discrepancies"]
            ):
                lines.append("")
                lines.append("Issues Detected During Transformation:")
                if issues["invalid_trips"]:
                    lines.append(
                        f"  • Invalid Trips: {len(issues['invalid_trips'])} - {issues['invalid_trips'][:5]}{'...' if len(issues['invalid_trips']) > 5 else ''}"
                    )
                if issues["distance_calculation_errors"]:
                    lines.append(
                        f"  • Distance Calculation Errors: {len(issues['distance_calculation_errors'])}"
                    )
                if issues["vehicle_count_discrepancies"]:
                    lines.append(
                        f"  • Vehicle Count Discrepancies: {len(issues['vehicle_count_discrepancies'])}"
                    )

        lines.append("=" * 80)
        return "\n".join(lines)

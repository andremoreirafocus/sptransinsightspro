from typing import Dict, Any, Tuple
import pandas as pd
from transformlivedata.quality.ge_column_lineage import (
    build_transformlivedata_lineage,
)
import json
import os
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
import logging

logger = logging.getLogger(__name__)


lat_long_limits = {
    "lat_min_value": -23.916334,
    "lat_max_value": -23.189743,
    "long_min_value": -46.792037,  # wrong
    # "long_min_value": -46.984037,  # corect
    # "long_max_value": -46.184930,  # correct
    "long_max_value": -46.184930,
}


def validate_transformed_positions_gx(positions_table, transformed_expectations_config):
    def get_df_from_tuples_list(tuples_table):
        column_names = [
            "extracao_ts",
            "veiculo_id",
            "linha_lt",
            "linha_code",
            "linha_sentido",
            "lt_destino",
            "lt_origem",
            "veiculo_prefixo",
            "veiculo_acessivel",
            "veiculo_ts",
            "veiculo_lat",
            "veiculo_long",
            "is_circular",
            "first_stop_id",
            "first_stop_lat",
            "first_stop_lon",
            "last_stop_id",
            "last_stop_lat",
            "last_stop_lon",
            "distance_to_first_stop",
            "distance_to_last_stop",
        ]
        if column_names:
            df = pd.DataFrame(tuples_table, columns=column_names)
        else:
            df = pd.DataFrame(tuples_table)
        return df

    def extract_unmatched_expectations_details(checkpoint_result):
        bad_indices = set()
        for run_result in checkpoint_result.run_results.values():
            validation_results = run_result["validation_result"]["results"]
            for expectation_result in validation_results:
                result = expectation_result.get("result", {})
                unexpected_indices = result.get("unexpected_index_list") or []
                if len(unexpected_indices) > 0:
                    expectation_config = expectation_result.get(
                        "expectation_config", {}
                    )
                    expectation_type = expectation_config.get("expectation_type")
                    column = expectation_config.get("kwargs", {}).get("column")
                    unexpected_indices = result["unexpected_index_list"]
                    violation_count = len(unexpected_indices)
                    print(
                        f"{expectation_type}"
                        + (f" (column: {column})" if column else "")
                        + f" -> violations: {violation_count}"
                    )
                    bad_indices.update(result["unexpected_index_list"])
        print(f"Amount of invalid records: {len(bad_indices)}")
        return bad_indices

    positions_df = get_df_from_tuples_list(positions_table)
    gx_context = gx.get_context()
    with open(transformed_expectations_config, "r") as f:
        suite_dict = json.load(f)
    suite = ExpectationSuite(**suite_dict)
    gx_context.add_or_update_expectation_suite(expectation_suite=suite)
    datasource_name = "pandas_datasource"
    datasource = gx_context.sources.add_pandas(datasource_name)
    data_asset = datasource.add_dataframe_asset(name="positions_asset")
    batch_request = data_asset.build_batch_request(dataframe=positions_df)
    checkpoint = gx_context.add_or_update_checkpoint(
        name="prod_checkpoint_with_alerts",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite.expectation_suite_name,
            }
        ],
        action_list=[
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
        ],
    )
    checkpoint_result = checkpoint.run(
        result_format="COMPLETE", evaluation_parameters=lat_long_limits
    )
    # checkpoint_result = checkpoint.run(result_format="BOOLEAN_ONLY")
    # checkpoint_result = checkpoint.run(result_format="BASIC")
    # checkpoint_result = checkpoint.run(result_format="SUMMARY")
    # checkpoint_result = checkpoint.run(result_format="COMPLETE")
    if checkpoint_result.success:
        print("Validation successful!")
        valid_df = positions_df
        invalid_df = None
    else:
        print("Validation failures detected!")
        print("Checking for unmatched expectations...")
        bad_indices = extract_unmatched_expectations_details(checkpoint_result)
        valid_df = positions_df.drop(index=list(bad_indices))
        invalid_df = positions_df.loc[list(bad_indices)]
        print(f"Amount of valid records: {valid_df.shape[0]}")
        print(f"Content of valid records:\n {valid_df.head()}")
        print(f"Amount of invalid records: {invalid_df.shape[0]}")
        print(f"Content of invalid records:\n {invalid_df.head()}")
    return
    # --- DATA DOCS GENERATION ---
    gx_context.build_data_docs()
    # This finds the local path to the 'index.html' of your documentation
    docs_url = gx_context.get_docs_sites_urls()[0]["site_url"]
    # # Optional: Automatically open the docs in your default browser
    import webbrowser

    webbrowser.open(docs_url)
    print(f"Data Docs generated at: {docs_url}")


def validate_transformation_metrics(results):
    lines = []
    if "metrics" in results:
        lines.extend(
            [
                "PHASE 2: TRANSFORMATION METRICS QUALITY ASSESSMENT",
                "-" * 80,
                f"Total Vehicles Processed: {results['metrics']['total_vehicles_processed']}",
                f"Valid Vehicles: {results['metrics']['valid_vehicles']}",
                f"Invalid Vehicles: {results['metrics']['invalid_vehicles']}",
                f"Bus lines identified: {results['metrics']['total_lines_processed']}",
                f"Quality Score: {results['quality_score']}%",
            ]
        )
        issues = results["issues"]
        if (
            issues["invalid_trips"]
            or issues["distance_calculation_errors"]
            or issues["vehicle_count_discrepancies"]
        ):
            lines.append("")
            lines.append("Issues Detected:")
            if issues["invalid_trips"]:
                lines.append(
                    f"  • Invalid Trips: {len(issues['invalid_trips'])} - {issues['invalid_trips'][:5]}{'...' if len(issues['invalid_trips']) > 5 else ''}"
                )
            if issues["invalid_vehicle_ids"]:
                lines.append(
                    f"  • Invalid Vehicles: {len(issues['invalid_vehicle_ids'])} - {issues['invalid_vehicle_ids'][:5]}{'...' if len(issues['invalid_vehicle_ids']) > 5 else ''}"
                )
            if issues["distance_calculation_errors"]:
                lines.append(
                    f"  • Distance Calculation Errors: {len(issues['distance_calculation_errors'])}"
                )
            if issues["vehicle_count_discrepancies"]:
                lines.append(
                    f"  • Vehicle Count Discrepancies: {len(issues['vehicle_count_discrepancies'])}"
                )
        lines.append("")
    return "\n".join(lines)


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

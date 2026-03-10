from transformlivedata.quality.ColumnLineageTracker import ColumnLineageTracker
from typing import Any, Dict, List, Tuple
import os
import json
import logging


logger = logging.getLogger(__name__)


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
    # Build lineage tracker
    quality_config = load_lineage_config(config)
    lineage_config = get_transformlivedata_column_lineage(quality_config)
    lineage_tracker = ColumnLineageTracker(execution_id)
    for output_col, lineage_info in lineage_config.items():
        lineage_tracker.add_field_mapping(
            output_column=output_col,
            input_sources=lineage_info["input_sources"],
            transformation_rule=lineage_info["transformation"],
            stage="transform",
        )
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


def load_lineage_config(config: Dict[str, Any]) -> Dict[str, Any]:
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


def get_transformlivedata_column_lineage(
    quality_config: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    """
    Get column lineage configuration for transformlivedata pipeline.

    Loads from external YAML file and converts to standard format.

    Args:
        quality_config: Dictionary with loaded quality configuration

    Returns:
        Dictionary mapping output_column → {input_sources, transformation}
    """
    # Convert YAML format to lineage dict format
    lineage = {}
    for output_col, col_config in quality_config["columns"].items():
        lineage[output_col] = {
            "input_sources": col_config.get("input_sources", []),
            "transformation": col_config.get("transformation", ""),
        }
    return lineage

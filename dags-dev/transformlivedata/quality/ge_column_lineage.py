"""
Great Expectations-based column-level lineage tracking for transformlivedata.

Tracks input fields → transformations → output columns with full traceability.
"""

from typing import Dict, Any, List
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ColumnLineageTracker:
    """Track column-level lineage from input API fields to output parquet columns."""

    def __init__(self, execution_id: str):
        """
        Initialize column lineage tracker.

        Args:
            execution_id: Unique execution identifier
        """
        self.execution_id = execution_id
        self.lineage_map: Dict[str, Dict[str, Any]] = {}
        self.transformation_rules: Dict[str, List[str]] = {}

    def add_field_mapping(
        self,
        output_column: str,
        input_sources: List[str],
        transformation_rule: str,
        stage: str = "transform",
    ):
        """
        Record a field mapping from input source(s) to output column.

        Args:
            output_column: Name of output column (e.g., 'veiculo_id')
            input_sources: List of input fields (e.g., ['payload.l[i].p'])
            transformation_rule: Description of transformation applied
            stage: Pipeline stage (load, transform, save)
        """
        self.lineage_map[output_column] = {
            "output_column": output_column,
            "input_sources": input_sources,
            "transformation_rule": transformation_rule,
            "stage": stage,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        # Track transformations by stage
        if stage not in self.transformation_rules:
            self.transformation_rules[stage] = []
        if transformation_rule not in self.transformation_rules[stage]:
            self.transformation_rules[stage].append(transformation_rule)

        logger.debug(
            f"Lineage: {output_column} ← {input_sources} ({transformation_rule})"
        )

    def get_output_schema(self) -> Dict[str, str]:
        """Get all output columns with their transformations."""
        return {
            col: mapping["transformation_rule"]
            for col, mapping in self.lineage_map.items()
        }

    def get_input_schema(self) -> List[str]:
        """Get all input fields referenced in lineage."""
        all_inputs = []
        for mapping in self.lineage_map.values():
            all_inputs.extend(mapping["input_sources"])
        return sorted(list(set(all_inputs)))

    def generate_lineage_report(self) -> str:
        """Generate human-readable lineage report showing field mappings."""
        lines = [
            "=" * 80,
            "COLUMN-LEVEL DATA LINEAGE REPORT",
            "=" * 80,
            f"Execution ID: {self.execution_id}",
            f"Generated: {datetime.utcnow().isoformat()}Z",
            "",
            "INPUT SCHEMA (API payload fields):",
            "-" * 80,
        ]
        input_fields = self.get_input_schema()
        for field in input_fields:
            lines.append(f"  • {field}")
        lines.extend(
            [
                "",
                "OUTPUT SCHEMA (Parquet columns):",
                "-" * 80,
            ]
        )
        output_schema = self.get_output_schema()
        for col, rule in sorted(output_schema.items()):
            lines.append(f"  • {col:25s} := {rule}")
        lines.extend(
            [
                "",
                "COLUMN-LEVEL LINEAGE MAPPING:",
                "-" * 80,
            ]
        )
        for col in sorted(self.lineage_map.keys()):
            mapping = self.lineage_map[col]
            inputs = ", ".join(mapping["input_sources"])
            rule = mapping["transformation_rule"]
            lines.append(f"{col}")
            lines.append(f"  ← {inputs}")
            lines.append(f"  ∘ {rule}")
            lines.append("")
        lines.append("=" * 80)
        return "\n".join(lines)

    def write_lineage_report(self, output_file: str = "column_lineage_report.txt"):
        """Write lineage report to file."""
        report = self.generate_lineage_report()
        with open(output_file, "w") as f:
            f.write(report)
        logger.info(f"Column lineage report written to {output_file}")
        return report

    def to_json(self) -> str:
        """Export lineage as JSON for machine consumption."""
        return json.dumps(
            {
                "execution_id": self.execution_id,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "lineage_map": self.lineage_map,
                "transformation_rules": self.transformation_rules,
            },
            indent=2,
            default=str,
        )


# ============================================================================
# CONFIGURATION LOADING
# ============================================================================


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


def get_transformlivedata_output_columns(quality_config: Dict[str, Any]) -> List[str]:
    """
    Get the ordered list of output column names from data quality configuration file.

    This is the single source of truth for output column names and order.
    Used by load_transform_save_positions to create DataFrames with correct schema.

    Args:
        quality_config: Dictionary with loaded quality configuration

    Returns:
        List[str]: Column names in the order defined in data quality configuration file

    Raises:
        FileNotFoundError: If data quality configuration file not found
        ValueError: If columns section missing from config
    """

    if "columns" not in quality_config:
        raise ValueError(
            "Missing 'columns' section in data quality configuration file."
        )
    columns = list(quality_config["columns"].keys())
    if not columns:
        raise ValueError("No columns defined in data quality configuration file")
    logger.debug(f"Loaded {len(columns)} output columns from configuration")
    return columns


def build_transformlivedata_lineage(
    quality_config: Dict[str, Any], execution_id: str
) -> "ColumnLineageTracker":
    """
    Build column lineage tracker for transformlivedata pipeline.

    Loads configuration from external YAML file and builds tracker.

    Args:
        quality_config: Dictionary with loaded quality configuration
        execution_id: Unique execution identifier

    Returns:
        Configured ColumnLineageTracker with all field mappings.
    """
    lineage_config = get_transformlivedata_column_lineage(quality_config)
    tracker = ColumnLineageTracker(execution_id)
    for output_col, lineage_info in lineage_config.items():
        tracker.add_field_mapping(
            output_column=output_col,
            input_sources=lineage_info["input_sources"],
            transformation_rule=lineage_info["transformation"],
            stage="transform",
        )
    return tracker

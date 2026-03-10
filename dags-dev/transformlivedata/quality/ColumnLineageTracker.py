"""
Column-level lineage tracking for transformlivedata.

Tracks input fields → transformations → output columns with full traceability.
"""

from typing import Dict, Any, List
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def build_lineage_map(
    lineage_config: Dict[str, Dict[str, Any]], execution_id: str
) -> Dict[str, Any]:
    """Build column lineage map from configuration.

    Args:
        lineage_config: Dictionary of output_col → {input_sources, transformation}
        execution_id: Unique execution identifier

    Returns:
        Dictionary containing lineage_map and transformation_rules by stage
    """
    lineage_map = {}
    transformation_rules = {}

    for output_column, lineage_info in lineage_config.items():
        stage = "transform"
        transformation_rule = lineage_info.get("transformation", "")
        input_sources = lineage_info.get("input_sources", [])

        lineage_map[output_column] = {
            "output_column": output_column,
            "input_sources": input_sources,
            "transformation_rule": transformation_rule,
            "stage": stage,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        # Track transformations by stage
        if stage not in transformation_rules:
            transformation_rules[stage] = []
        if transformation_rule not in transformation_rules[stage]:
            transformation_rules[stage].append(transformation_rule)

        logger.debug(
            f"Lineage: {output_column} ← {input_sources} ({transformation_rule})"
        )

    return {
        "execution_id": execution_id,
        "lineage_map": lineage_map,
        "transformation_rules": transformation_rules,
    }


def get_output_schema(lineage_data: Dict[str, Any]) -> Dict[str, str]:
    """Get all output columns with their transformations.

    Args:
        lineage_data: Lineage data dictionary

    Returns:
        Dictionary mapping column names to transformation rules
    """
    return {
        col: mapping["transformation_rule"]
        for col, mapping in lineage_data["lineage_map"].items()
    }


def get_input_schema(lineage_data: Dict[str, Any]) -> List[str]:
    """Get all input fields referenced in lineage.

    Args:
        lineage_data: Lineage data dictionary

    Returns:
        Sorted list of unique input field references
    """
    all_inputs = []
    for mapping in lineage_data["lineage_map"].values():
        all_inputs.extend(mapping["input_sources"])
    return sorted(list(set(all_inputs)))


def generate_lineage_report(lineage_data: Dict[str, Any]) -> str:
    """Generate human-readable lineage report showing field mappings.

    Args:
        lineage_data: Lineage data dictionary

    Returns:
        Formatted lineage report as string
    """
    lines = [
        "=" * 80,
        "COLUMN-LEVEL DATA LINEAGE REPORT",
        "=" * 80,
        f"Execution ID: {lineage_data['execution_id']}",
        f"Generated: {datetime.utcnow().isoformat()}Z",
        "",
        "INPUT SCHEMA (API payload fields):",
        "-" * 80,
    ]
    input_fields = get_input_schema(lineage_data)
    for field in input_fields:
        lines.append(f"  • {field}")
    lines.extend(
        [
            "",
            "OUTPUT SCHEMA (Parquet columns):",
            "-" * 80,
        ]
    )
    output_schema = get_output_schema(lineage_data)
    for col, rule in sorted(output_schema.items()):
        lines.append(f"  • {col:25s} := {rule}")
    lines.extend(
        [
            "",
            "COLUMN-LEVEL LINEAGE MAPPING:",
            "-" * 80,
        ]
    )
    lineage_map = lineage_data["lineage_map"]
    for col in sorted(lineage_map.keys()):
        mapping = lineage_map[col]
        inputs = ", ".join(mapping["input_sources"])
        rule = mapping["transformation_rule"]
        lines.append(f"{col}")
        lines.append(f"  ← {inputs}")
        lines.append(f"  ∘ {rule}")
        lines.append("")
    lines.append("=" * 80)
    return "\n".join(lines)


def write_lineage_report(
    lineage_data: Dict[str, Any], output_file: str = "column_lineage_report.txt"
) -> str:
    """Write lineage report to file.

    Args:
        lineage_data: Lineage data dictionary
        output_file: Output filename

    Returns:
        Generated report text
    """
    report = generate_lineage_report(lineage_data)
    with open(output_file, "w") as f:
        f.write(report)
    logger.info(f"Column lineage report written to {output_file}")
    return report


def lineage_to_json(lineage_data: Dict[str, Any]) -> str:
    """Export lineage as JSON for machine consumption.

    Args:
        lineage_data: Lineage data dictionary

    Returns:
        JSON string representation of lineage
    """
    return json.dumps(
        {
            "execution_id": lineage_data["execution_id"],
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "lineage_map": lineage_data["lineage_map"],
            "transformation_rules": lineage_data["transformation_rules"],
        },
        indent=2,
        default=str,
    )

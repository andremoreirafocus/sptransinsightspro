import pytest

from refinedtripfacts.lineage.trip_facts_lineage import (
    get_trip_facts_lineage,
    get_trip_facts_table_columns,
    validate_trip_facts_lineage,
)

pytestmark = pytest.mark.integration


def test_table_columns_match_lineage_names_and_types(test_config):
    actual = get_trip_facts_table_columns(test_config)
    assert len(actual) > 0
    result = validate_trip_facts_lineage(get_trip_facts_lineage(), actual)
    assert result["drift_detected"] is False


def test_type_drift_detected(test_config):
    lineage = get_trip_facts_lineage()
    lineage["columns"]["direction"]["expected_type"] = "bigint"
    actual = get_trip_facts_table_columns(test_config)
    result = validate_trip_facts_lineage(lineage, actual)
    assert result["drift_detected"] is True
    assert any(
        e["column"] == "direction" for e in result["validation"]["columns_type_changed"]
    )

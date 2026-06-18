import pytest

from refinedtripfacts.config.refinedtripfacts_config_schema import (
    GeneralConfig,
    validate_general_input,
)


def make_valid_general():
    return {
        "tables": {
            "finished_trips_table_name": "refined.finished_trips",
            "trip_facts_table_name": "refined.trip_facts",
            "dim_time_table_name": "refined.dim_time",
        },
        "quality": {
            "completeness_loss_rate_warn_threshold": 0.01,
            "completeness_loss_rate_fail_threshold": 0.05,
            "avg_speed_kmh_max": 120.0,
        },
        "storage": {
            "metadata_bucket": "metadata",
            "quality_report_folder": "quality-reports",
        },
    }


def test_valid_config_loads_without_error():
    result = validate_general_input(make_valid_general(), GeneralConfig)
    assert result.tables.trip_facts_table_name == "refined.trip_facts"


def test_missing_table_name_raises_value_error():
    general = make_valid_general()
    del general["tables"]["trip_facts_table_name"]
    with pytest.raises(ValueError):
        validate_general_input(general, GeneralConfig)


def test_extra_key_in_tables_raises_value_error():
    general = make_valid_general()
    general["tables"]["unknown_table"] = "refined.something"
    with pytest.raises(ValueError):
        validate_general_input(general, GeneralConfig)

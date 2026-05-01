import pytest

from refinedfinishedtrips.config.refinedfinishedtrips_config_schema import (
    GeneralConfig,
    validate_general_input,
)


def make_valid_general():
    return {
        "analysis": {"hours_window": 3},
        "storage": {
            "app_folder": "sptrans",
            "trusted_bucket": "trusted",
            "metadata_bucket": "metadata",
            "quality_report_folder": "quality-reports",
        },
        "tables": {
            "positions_table_name": "positions",
            "finished_trips_table_name": "refined.finished_trips",
        },
        "quality": {
            "freshness_warn_staleness_minutes": 10,
            "freshness_fail_staleness_minutes": 30,
            "gaps_warn_gap_minutes": 5,
            "gaps_fail_gap_minutes": 15,
            "gaps_recent_window_minutes": 60,
            "trips_effective_window_threshold_minutes": 60,
            "trips_min_trips_threshold": 5,
        },
        "notifications": {"webhook_url": "disabled"},
        "trip_detection": {"stop_proximity_threshold_meters": 100},
    }


def test_valid_config_passes():
    result = validate_general_input(make_valid_general(), GeneralConfig)
    assert result.trip_detection.stop_proximity_threshold_meters == 100


def test_missing_trip_detection_section_raises():
    general = make_valid_general()
    del general["trip_detection"]
    with pytest.raises(ValueError):
        validate_general_input(general, GeneralConfig)


def test_missing_stop_proximity_threshold_raises():
    general = make_valid_general()
    general["trip_detection"] = {}
    with pytest.raises(ValueError):
        validate_general_input(general, GeneralConfig)


def test_wrong_type_for_threshold_raises():
    general = make_valid_general()
    general["trip_detection"] = {"stop_proximity_threshold_meters": 100.5}
    with pytest.raises(ValueError):
        validate_general_input(general, GeneralConfig)


def test_unknown_extra_field_in_trip_detection_raises():
    general = make_valid_general()
    general["trip_detection"] = {"stop_proximity_threshold_meters": 100, "unknown_field": 1}
    with pytest.raises(ValueError):
        validate_general_input(general, GeneralConfig)

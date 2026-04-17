from gtfs.lineage.trip_details_lineage import (
    LINEAGE_DRIFT_WARNING,
    get_trip_details_lineage,
    validate_trip_details_lineage,
)


def test_get_trip_details_lineage_has_expected_columns():
    lineage = get_trip_details_lineage()
    columns = lineage["columns"]
    assert lineage["table_name"] == "trip_details"
    assert "trip_id" in columns
    assert "trip_linear_distance" in columns
    assert "is_circular" in columns


def test_validate_trip_details_lineage_without_drift():
    lineage = get_trip_details_lineage()
    actual_columns = [
        "trip_id",
        "first_stop_id",
        "first_stop_name",
        "first_stop_lat",
        "first_stop_lon",
        "last_stop_id",
        "last_stop_name",
        "last_stop_lat",
        "last_stop_lon",
        "trip_linear_distance",
        "is_circular",
    ]
    validated = validate_trip_details_lineage(lineage, actual_columns)
    assert validated["drift_detected"] is False
    assert validated["warning"] is None
    assert validated["validation"]["missing_in_lineage"] == []
    assert validated["validation"]["missing_in_dataframe"] == []


def test_validate_trip_details_lineage_with_drift():
    lineage = get_trip_details_lineage()
    actual_columns = [
        "trip_id",
        "first_stop_id",
        "unexpected_new_column",
    ]
    validated = validate_trip_details_lineage(lineage, actual_columns)
    assert validated["drift_detected"] is True
    assert validated["warning"] == LINEAGE_DRIFT_WARNING
    assert "unexpected_new_column" in validated["validation"]["missing_in_lineage"]

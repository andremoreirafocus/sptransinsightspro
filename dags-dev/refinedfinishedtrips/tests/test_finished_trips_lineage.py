from refinedfinishedtrips.lineage.finished_trips_lineage import (
    get_finished_trips_lineage,
    get_finished_trips_output_columns,
    validate_finished_trips_lineage,
)


def test_get_finished_trips_lineage_has_expected_columns():
    lineage = get_finished_trips_lineage()
    columns = lineage["columns"]
    assert lineage["table_name"] == "finished_trips"
    assert sorted(columns.keys()) == sorted(get_finished_trips_output_columns())


def test_validate_finished_trips_lineage_without_drift():
    lineage = get_finished_trips_lineage()
    validated = validate_finished_trips_lineage(
        lineage, get_finished_trips_output_columns()
    )
    assert validated["drift_detected"] is False
    assert validated["warning"] is None
    assert validated["validation"]["missing_in_lineage"] == []
    assert validated["validation"]["missing_in_dataframe"] == []


def test_validate_finished_trips_lineage_with_drift():
    lineage = get_finished_trips_lineage()
    validated = validate_finished_trips_lineage(
        lineage,
        [
            "trip_id",
            "vehicle_id",
            "trip_start_time",
            "trip_end_time",
            "duration",
            "is_circular",
            "unexpected_new_column",
        ],
    )
    assert validated["drift_detected"] is True
    assert validated["warning"] == "lineage drift detected"
    assert "unexpected_new_column" in validated["validation"]["missing_in_lineage"]
    assert "average_speed" in validated["validation"]["missing_in_dataframe"]

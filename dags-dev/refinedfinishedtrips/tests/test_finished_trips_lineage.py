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
            "is_circular",
            "unexpected_new_column",
        ],
    )
    assert validated["drift_detected"] is True
    assert validated["warning"] == "lineage drift detected"
    assert "unexpected_new_column" in validated["validation"]["missing_in_lineage"]
    assert "duration_seconds" in validated["validation"]["missing_in_dataframe"]


def test_output_columns_returns_stage1_contract():
    assert get_finished_trips_output_columns() == [
        "trip_id",
        "vehicle_id",
        "trip_start_time",
        "trip_end_time",
        "duration_seconds",
        "is_circular",
        "distance_meters",
        "avg_speed_kmh",
        "logic_date",
    ]


def test_column_map_excludes_duration_and_average_speed():
    lineage = get_finished_trips_lineage()
    column_map = lineage["columns"]
    assert "duration" not in column_map
    assert "average_speed" not in column_map


def test_column_map_covers_all_output_columns():
    lineage = get_finished_trips_lineage()
    column_map = lineage["columns"]
    for col in get_finished_trips_output_columns():
        assert col in column_map, f"Column '{col}' missing from column_map"


def test_validate_lineage_detects_drift():
    lineage = get_finished_trips_lineage()
    extra_column_list = get_finished_trips_output_columns() + ["phantom_column"]
    validated = validate_finished_trips_lineage(lineage, extra_column_list)
    assert validated["drift_detected"] is True

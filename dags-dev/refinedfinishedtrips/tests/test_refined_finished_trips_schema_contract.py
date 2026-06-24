from refinedfinishedtrips.lineage.finished_trips_lineage import get_finished_trips_output_columns

STAGE1_REQUIRED = {"duration_seconds", "distance_meters", "avg_speed_kmh", "logic_date"}
REMOVED_COLUMNS = {"duration", "average_speed"}


def test_output_columns_contains_all_stage1_fields():
    columns = set(get_finished_trips_output_columns())
    assert STAGE1_REQUIRED.issubset(columns), f"Missing Stage 1 columns: {STAGE1_REQUIRED - columns}"


def test_output_columns_excludes_removed_fields():
    columns = set(get_finished_trips_output_columns())
    present = REMOVED_COLUMNS & columns
    assert not present, f"Removed columns still present: {present}"

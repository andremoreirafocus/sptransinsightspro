from refinedtripfacts.lineage.trip_facts_lineage import (
    get_trip_facts_lineage,
    get_trip_facts_output_columns,
    validate_trip_facts_lineage,
)


def test_output_columns_matches_schema_contract():
    columns = get_trip_facts_output_columns()
    assert len(columns) == 15
    assert set(columns) == {
        "trip_id", "vehicle_id", "route_id", "direction",
        "started_at", "ended_at", "duration_seconds", "duration",
        "is_circular", "distance_meters", "avg_speed_kmh",
        "started_at_time_dim_key", "ended_at_time_dim_key",
        "logic_date", "created_at",
    }


def test_lineage_object_shape():
    lineage = get_trip_facts_lineage()
    assert lineage["table_name"] == "trip_facts"
    assert "columns" in lineage
    assert lineage["drift_detected"] is False
    assert lineage["warning"] is None


def test_lineage_covers_all_output_columns():
    lineage = get_trip_facts_lineage()
    columns = lineage["columns"]
    for col in get_trip_facts_output_columns():
        assert col in columns, f"column '{col}' missing from lineage"
        assert "sources" in columns[col]
        assert "derivation" in columns[col]
        assert "expected_type" in columns[col]


def _matching_actual(lineage):
    return [
        {"column_name": name, "data_type": meta["expected_type"]}
        for name, meta in lineage["columns"].items()
    ]


def test_validate_returns_enriched_lineage_with_column_map():
    lineage = get_trip_facts_lineage()
    result = validate_trip_facts_lineage(lineage, _matching_actual(lineage))
    assert "columns" in result
    assert "validation" in result
    v = result["validation"]
    assert "expected_columns" in v
    assert "actual_columns" in v
    assert "columns_added" in v
    assert "columns_removed" in v
    assert "columns_type_changed" in v


def test_validate_detects_column_in_data_not_in_lineage():
    lineage = get_trip_facts_lineage()
    actual = _matching_actual(lineage) + [{"column_name": "extra_col", "data_type": "text"}]
    result = validate_trip_facts_lineage(lineage, actual)
    assert result["drift_detected"] is True
    assert "extra_col" in result["validation"]["columns_added"]


def test_validate_detects_column_in_lineage_not_in_data():
    lineage = get_trip_facts_lineage()
    actual = [r for r in _matching_actual(lineage) if r["column_name"] != "route_id"]
    result = validate_trip_facts_lineage(lineage, actual)
    assert result["drift_detected"] is True
    assert "route_id" in result["validation"]["columns_removed"]


def test_validate_detects_type_mismatch():
    lineage = get_trip_facts_lineage()
    actual = [
        {"column_name": r["column_name"], "data_type": "integer" if r["column_name"] == "direction" else r["data_type"]}
        for r in _matching_actual(lineage)
    ]
    result = validate_trip_facts_lineage(lineage, actual)
    assert result["drift_detected"] is True
    changed = result["validation"]["columns_type_changed"]
    assert any(
        e["column"] == "direction"
        and e["expected_type"] == "smallint"
        and e["actual_type"] == "integer"
        for e in changed
    )


def test_no_drift_when_names_and_types_match():
    lineage = get_trip_facts_lineage()
    result = validate_trip_facts_lineage(lineage, _matching_actual(lineage))
    assert result["drift_detected"] is False
    assert result["warning"] is None

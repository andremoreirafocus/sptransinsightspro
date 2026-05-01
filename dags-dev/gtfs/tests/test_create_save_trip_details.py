import duckdb
import duckdb
import pytest
from fakes.fake_duckdb_connection import FakeDuckDBConnection
from gtfs.services.create_save_trip_details import (
    create_trip_details_table_and_fill_missing_data,
)


def make_config():
    return {
        "general": {
            "storage": {
                "trusted_bucket": "trusted",
                "gtfs_folder": "gtfs",
                "staging_subfolder": "staging",
            },
            "tables": {
                "trip_details_table_name": "trip_details",
            },
        },
        "connections": {
            "object_storage": {
                "endpoint": "localhost",
                "access_key": "key",
                "secret_key": "secret",
            }
        },
    }


def test_missing_config_key_raises_value_error():
    config = make_config()
    del config["general"]["storage"]["trusted_bucket"]
    with pytest.raises(ValueError, match="Missing required configuration key"):
        create_trip_details_table_and_fill_missing_data(config)


def test_duckdb_error_raises_value_error():
    fake_con = FakeDuckDBConnection(raises=RuntimeError("duckdb boom"))
    with pytest.raises(ValueError, match="An unexpected error occurred"):
        create_trip_details_table_and_fill_missing_data(
            make_config(), duckdb_client=fake_con
        )


def test_connection_closed_after_error():
    fake_con = FakeDuckDBConnection(raises=RuntimeError("boom"))
    try:
        create_trip_details_table_and_fill_missing_data(
            make_config(), duckdb_client=fake_con
        )
    except ValueError:
        pass
    assert fake_con.closed


def test_trip_details_exported_to_staging_path():
    fake_con = FakeDuckDBConnection()
    result = create_trip_details_table_and_fill_missing_data(
        make_config(),
        duckdb_client=fake_con,
    )
    assert result["table_name"] == "trip_details"
    assert result["staging_object_name"] == "gtfs/staging/trip_details.parquet"
    assert result["staged_written"] is True
    assert any(
        "s3://trusted/gtfs/staging/trip_details.parquet" in command
        for command in fake_con.executed_sql
    )


# ---------------------------------------------------------------------------
# normalized_base and inferred_sentido_2_trips SQL — uses real DuckDB in-memory
# ---------------------------------------------------------------------------

_NORMALIZED_BASE_SQL = """
    CREATE TABLE normalized_base AS
    SELECT
        base.trip_id,
        COALESCE(ref.first_stop_id, base.first_stop_id) AS first_stop_id,
        COALESCE(ref.first_stop_name, base.first_stop_name) AS first_stop_name,
        COALESCE(ref.first_stop_lat, base.first_stop_lat) AS first_stop_lat,
        COALESCE(ref.first_stop_lon, base.first_stop_lon) AS first_stop_lon,
        COALESCE(ref.last_stop_id, base.last_stop_id) AS last_stop_id,
        COALESCE(ref.last_stop_name, base.last_stop_name) AS last_stop_name,
        COALESCE(ref.last_stop_lat, base.last_stop_lat) AS last_stop_lat,
        COALESCE(ref.last_stop_lon, base.last_stop_lon) AS last_stop_lon,
        COALESCE(ref.trip_linear_distance, base.trip_linear_distance) AS trip_linear_distance,
        COALESCE(ref.is_circular, base.is_circular) AS is_circular
    FROM calculated_base base
    LEFT JOIN calculated_base ref
        ON base.trip_id LIKE '%-1'
        AND ref.trip_id = REGEXP_REPLACE(base.trip_id, '-1$', '-0')
"""

_INFERRED_SENTIDO_2_TRIPS_SQL = """
    SELECT
        REPLACE(t0.trip_id, '-0', '-1') AS trip_id,
        t0.first_stop_id,
        t0.first_stop_name,
        t0.first_stop_lat,
        t0.first_stop_lon,
        t0.last_stop_id,
        t0.last_stop_name,
        t0.last_stop_lat,
        t0.last_stop_lon,
        t0.trip_linear_distance,
        t0.is_circular
    FROM normalized_base t0
    LEFT JOIN normalized_base t1 ON t1.trip_id = REPLACE(t0.trip_id, '-0', '-1')
    WHERE t0.trip_id LIKE '%-0' AND t1.trip_id IS NULL
"""


def _setup_calculated_base(con, rows):
    con.execute("""
        CREATE TABLE calculated_base (
            trip_id VARCHAR,
            first_stop_id INTEGER,
            first_stop_name VARCHAR,
            first_stop_lat DOUBLE,
            first_stop_lon DOUBLE,
            last_stop_id INTEGER,
            last_stop_name VARCHAR,
            last_stop_lat DOUBLE,
            last_stop_lon DOUBLE,
            trip_linear_distance DOUBLE,
            is_circular BOOLEAN
        )
    """)
    for row in rows:
        con.execute(
            "INSERT INTO calculated_base VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", row
        )


def test_normalized_base_real_gtfs_sentido_2_adopts_sentido_1_stops():
    # Real GTFS provides both directions with direction-specific stops:
    # -0 goes A→B, -1 goes B→A. normalized_base must make both use A/B as
    # fixed reference points so the distance-based algorithm works correctly.
    con = duckdb.connect(":memory:")
    _setup_calculated_base(con, [
        ("1234-10-0", 10, "Stop A", -23.5, -46.6, 20, "Stop B", -23.4, -46.5, 5000.0, False),
        ("1234-10-1", 20, "Stop B", -23.4, -46.5, 10, "Stop A", -23.5, -46.6, 5000.0, False),
    ])
    con.execute(_NORMALIZED_BASE_SQL)
    result = con.execute("SELECT * FROM normalized_base WHERE trip_id = '1234-10-1'").df()
    con.close()

    assert len(result) == 1
    row = result.iloc[0]
    assert row["first_stop_id"] == 10     # same as -0 (Stop A), not swapped
    assert row["first_stop_name"] == "Stop A"
    assert row["last_stop_id"] == 20      # same as -0 (Stop B), not swapped
    assert row["last_stop_name"] == "Stop B"


def test_normalized_base_sentido_1_unchanged():
    con = duckdb.connect(":memory:")
    _setup_calculated_base(con, [
        ("1234-10-0", 10, "Stop A", -23.5, -46.6, 20, "Stop B", -23.4, -46.5, 5000.0, False),
    ])
    con.execute(_NORMALIZED_BASE_SQL)
    result = con.execute("SELECT * FROM normalized_base WHERE trip_id = '1234-10-0'").df()
    con.close()

    assert len(result) == 1
    row = result.iloc[0]
    assert row["first_stop_id"] == 10
    assert row["last_stop_id"] == 20


def test_normalized_base_sentido_2_without_counterpart_keeps_own_stops():
    # Edge case: a -1 trip in GTFS with no corresponding -0. Keeps its own stops.
    con = duckdb.connect(":memory:")
    _setup_calculated_base(con, [
        ("8888-10-1", 30, "Stop X", -23.6, -46.7, 40, "Stop Y", -23.5, -46.6, 3000.0, False),
    ])
    con.execute(_NORMALIZED_BASE_SQL)
    result = con.execute("SELECT * FROM normalized_base WHERE trip_id = '8888-10-1'").df()
    con.close()

    assert len(result) == 1
    row = result.iloc[0]
    assert row["first_stop_id"] == 30
    assert row["last_stop_id"] == 40


def test_inferred_sentido_2_non_circular_keeps_same_stops():
    con = duckdb.connect(":memory:")
    _setup_calculated_base(con, [
        ("1234-10-0", 10, "Stop A", -23.5, -46.6, 20, "Stop B", -23.4, -46.5, 5000.0, False),
    ])
    con.execute(_NORMALIZED_BASE_SQL)
    result = con.execute(_INFERRED_SENTIDO_2_TRIPS_SQL).df()
    con.close()

    assert len(result) == 1
    row = result.iloc[0]
    assert row["trip_id"] == "1234-10-1"
    assert row["first_stop_id"] == 10    # same as -0, NOT swapped to last_stop_id (20)
    assert row["first_stop_name"] == "Stop A"
    assert row["last_stop_id"] == 20     # same as -0, NOT swapped to first_stop_id (10)
    assert row["last_stop_name"] == "Stop B"


def test_inferred_sentido_2_circular_keeps_same_stops():
    con = duckdb.connect(":memory:")
    _setup_calculated_base(con, [
        ("9999-10-0", 10, "Terminal", -23.5, -46.6, 10, "Terminal", -23.5, -46.6, 0.0, True),
    ])
    con.execute(_NORMALIZED_BASE_SQL)
    result = con.execute(_INFERRED_SENTIDO_2_TRIPS_SQL).df()
    con.close()

    assert len(result) == 1
    row = result.iloc[0]
    assert row["trip_id"] == "9999-10-1"
    assert row["first_stop_id"] == 10
    assert row["last_stop_id"] == 10


def test_inferred_sentido_2_skips_trip_already_present_in_gtfs():
    con = duckdb.connect(":memory:")
    _setup_calculated_base(con, [
        ("1234-10-0", 10, "Stop A", -23.5, -46.6, 20, "Stop B", -23.4, -46.5, 5000.0, False),
        ("1234-10-1", 20, "Stop B", -23.4, -46.5, 10, "Stop A", -23.5, -46.6, 5000.0, False),
    ])
    con.execute(_NORMALIZED_BASE_SQL)
    result = con.execute(_INFERRED_SENTIDO_2_TRIPS_SQL).df()
    con.close()

    assert len(result) == 0

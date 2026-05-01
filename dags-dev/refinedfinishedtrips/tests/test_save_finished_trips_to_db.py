from datetime import datetime, timedelta, timezone

import pytest

from refinedfinishedtrips.tests.fakes.fake_db_engine import make_fake_engine_factory
from refinedfinishedtrips.services.save_finished_trips_to_db import save_finished_trips_to_db

BASE_TS = datetime(2026, 4, 14, 10, 0, 0, tzinfo=timezone.utc)


def make_config():
    return {
        "general": {"tables": {"finished_trips_table_name": "finished_trips"}},
        "connections": {
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "testdb",
                "user": "user",
                "password": "pass",
            }
        },
    }


def make_trip_tuple(trip_end_offset_seconds: int = 3600):
    start = BASE_TS
    end = BASE_TS + timedelta(seconds=trip_end_offset_seconds)
    return ("1234-10-0", 100, start, end, end - start, False, 0.0)


def test_missing_config_key_raises_value_error():
    config = make_config()
    del config["connections"]
    factory = make_fake_engine_factory()
    with pytest.raises(ValueError):
        save_finished_trips_to_db(config, [], engine_factory=factory)


def test_empty_trips_no_parameterized_insert_executed():
    factory = make_fake_engine_factory()
    save_finished_trips_to_db(make_config(), [], engine_factory=factory)
    parameterized_calls = [params for _, params in factory.engine.executed_statements if params is not None]
    assert parameterized_calls == []


def test_non_empty_trips_insert_statement_executed():
    factory = make_fake_engine_factory(rowcount=1)
    save_finished_trips_to_db(make_config(), [make_trip_tuple()], engine_factory=factory)
    statements = [stmt for stmt, _ in factory.engine.executed_statements]
    assert any("INSERT" in stmt.upper() for stmt in statements)


def test_engine_error_raises_value_error():
    factory = make_fake_engine_factory(raises=Exception("DB unavailable"))
    with pytest.raises(ValueError, match="Persistence failed"):
        save_finished_trips_to_db(make_config(), [make_trip_tuple()], engine_factory=factory)


def test_returns_new_and_skipped_row_counts():
    factory = make_fake_engine_factory(rowcount=1)
    result = save_finished_trips_to_db(make_config(), [make_trip_tuple()], engine_factory=factory)
    assert result == {"new_rows": 1, "skipped_rows": 0}


def test_returns_zero_counts_for_empty_trips():
    factory = make_fake_engine_factory(rowcount=0)
    result = save_finished_trips_to_db(make_config(), [], engine_factory=factory)
    assert result == {"new_rows": 0, "skipped_rows": 0}


def test_skipped_rows_computed_from_rowcount():
    factory = make_fake_engine_factory(rowcount=2)
    trips = [make_trip_tuple(), make_trip_tuple(), make_trip_tuple()]
    result = save_finished_trips_to_db(make_config(), trips, engine_factory=factory)
    assert result == {"new_rows": 2, "skipped_rows": 1}


def test_db_empty_max_returns_none_all_trips_saved():
    trip = make_trip_tuple(trip_end_offset_seconds=3600)
    factory = make_fake_engine_factory(rowcount=1, max_trip_end_time=None)
    result = save_finished_trips_to_db(make_config(), [trip], engine_factory=factory)
    assert result["new_rows"] == 1


def test_trips_older_than_latest_saved_are_filtered_out():
    latest_saved_ts = BASE_TS + timedelta(seconds=3600)
    old_trip = make_trip_tuple(trip_end_offset_seconds=3600)   # trip_end_time == latest_saved_ts → filtered
    new_trip = make_trip_tuple(trip_end_offset_seconds=7200)   # trip_end_time > latest_saved_ts → kept

    factory = make_fake_engine_factory(rowcount=1, max_trip_end_time=latest_saved_ts)
    result = save_finished_trips_to_db(make_config(), [old_trip, new_trip], engine_factory=factory)

    parameterized_calls = [params for _, params in factory.engine.executed_statements if params is not None]
    inserted_end_times = [row["t_end"] for batch in parameterized_calls for row in batch]
    assert BASE_TS + timedelta(seconds=7200) in inserted_end_times
    assert BASE_TS + timedelta(seconds=3600) not in inserted_end_times
    assert result["skipped_rows"] == 1


def test_all_trips_older_than_latest_saved_results_in_no_insert():
    latest_saved_ts = BASE_TS + timedelta(hours=2)
    old_trip = make_trip_tuple(trip_end_offset_seconds=3600)   # older than latest_saved_ts

    factory = make_fake_engine_factory(rowcount=0, max_trip_end_time=latest_saved_ts)
    result = save_finished_trips_to_db(make_config(), [old_trip], engine_factory=factory)

    parameterized_calls = [params for _, params in factory.engine.executed_statements if params is not None]
    assert parameterized_calls == []
    assert result == {"new_rows": 0, "skipped_rows": 1}

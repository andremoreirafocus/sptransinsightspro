from datetime import datetime, timedelta, timezone

import pytest

from fakes.fake_db_engine import make_fake_engine_factory
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


def make_trip_tuple():
    start = BASE_TS
    end = BASE_TS + timedelta(seconds=3600)
    return ("1234-10-0", 100, start, end, end - start, False, 0.0)


def test_missing_config_key_raises_value_error():
    config = make_config()
    del config["connections"]
    factory = make_fake_engine_factory()
    with pytest.raises(ValueError):
        save_finished_trips_to_db(config, [], engine_factory=factory)


def test_empty_trips_no_parameterized_insert_executed():
    # With empty trips, the batch INSERT into staging (with bound params) must not run.
    # The upsert INSERT...SELECT FROM staging always runs but carries no params.
    factory = make_fake_engine_factory()
    save_finished_trips_to_db(make_config(), [], engine_factory=factory)
    parameterized_calls = [params for _, params in factory.engine.executed_statements if params is not None]
    assert parameterized_calls == []


def test_non_empty_trips_insert_statement_executed():
    factory = make_fake_engine_factory(rowcount=1)
    save_finished_trips_to_db(make_config(), [make_trip_tuple()], engine_factory=factory)
    statements = [stmt for stmt, _ in factory.engine.executed_statements]
    assert any("INSERT" in s.upper() for s in statements)


def test_engine_error_raises_value_error():
    factory = make_fake_engine_factory(raises=Exception("DB unavailable"))
    with pytest.raises(ValueError, match="Persistence failed"):
        save_finished_trips_to_db(make_config(), [make_trip_tuple()], engine_factory=factory)

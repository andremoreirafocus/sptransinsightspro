from datetime import datetime, timezone

import pytest

from refinedtripfacts.services.create_trip_facts import create_trip_facts
from refinedtripfacts.tests.fakes.fake_db_engine import (
    FakeExecuteResult,
    FakeRow,
    make_fake_engine_factory,
)

LOGIC_DATE = datetime(2026, 6, 8, 15, 0, 0, tzinfo=timezone.utc)

CONFIG = {
    "general": {
        "tables": {
            "finished_trips_table_name": "refined.finished_trips",
            "trip_facts_table_name": "refined.trip_facts",
        }
    },
    "connections": {
        "database": {
            "host": "localhost",
            "port": 5432,
            "database": "sptrans_insights",
            "user": "postgres",
            "password": "postgres",
        }
    },
}


def _factory(facts_derived: int = 0, inserted_rows: int = 0):
    return make_fake_engine_factory(
        responses=[FakeExecuteResult(row=FakeRow(facts_derived, inserted_rows))]
    )


def test_insert_select_statement_issued():
    factory = _factory(facts_derived=3, inserted_rows=3)
    create_trip_facts(LOGIC_DATE, CONFIG, engine_factory=factory)
    sql, _ = factory.engine.executed_statements[0]
    assert "INSERT INTO refined.trip_facts" in sql
    assert "refined.finished_trips" in sql


def test_statement_contains_derivation_expressions():
    factory = _factory()
    create_trip_facts(LOGIC_DATE, CONFIG, engine_factory=factory)
    sql, _ = factory.engine.executed_statements[0]
    assert "left(trip_id" in sql.lower()
    assert "CASE right(trip_id, 1) WHEN '0' THEN 1 WHEN '1' THEN 2 END" in sql
    assert "make_interval" in sql.lower()
    assert sql.count("AT TIME ZONE 'America/Sao_Paulo'") >= 2


def test_statement_has_on_conflict_do_nothing():
    factory = _factory()
    create_trip_facts(LOGIC_DATE, CONFIG, engine_factory=factory)
    sql, _ = factory.engine.executed_statements[0]
    assert "ON CONFLICT (started_at, vehicle_id, trip_id) DO NOTHING" in sql


def test_logic_date_bound_as_param():
    factory = _factory()
    create_trip_facts(LOGIC_DATE, CONFIG, engine_factory=factory)
    sql, params = factory.engine.executed_statements[0]
    assert params == {"logic_date": LOGIC_DATE}
    assert str(LOGIC_DATE) not in sql


def test_returns_measured_counts():
    factory = _factory(facts_derived=5, inserted_rows=2)
    result = create_trip_facts(LOGIC_DATE, CONFIG, engine_factory=factory)
    assert result == {"facts_derived": 5, "inserted_rows": 2, "skipped_rows": 3}


def test_db_error_raises_value_error():
    factory = make_fake_engine_factory(raises=RuntimeError("db down"))
    with pytest.raises(ValueError) as exc_info:
        create_trip_facts(LOGIC_DATE, CONFIG, engine_factory=factory)
    msg = str(exc_info.value)
    assert "refined.trip_facts" in msg
    assert str(LOGIC_DATE) in msg

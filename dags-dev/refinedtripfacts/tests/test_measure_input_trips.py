from datetime import datetime, timezone

import pytest

from refinedtripfacts.services.measure_input_trips import measure_input_trips
from refinedtripfacts.tests.fakes.fake_db_engine import make_fake_engine_factory

LOGIC_DATE = datetime(2026, 6, 8, 15, 0, 0, tzinfo=timezone.utc)

CONFIG = {
    "general": {
        "tables": {"finished_trips_table_name": "refined.finished_trips"}
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


def test_returns_count_for_matching_logic_date():
    factory = make_fake_engine_factory(scalar_count=5)
    result = measure_input_trips(LOGIC_DATE, CONFIG, engine_factory=factory)
    assert result == {"finished_trips_read": 5}


def test_returns_zero_when_no_rows_match():
    factory = make_fake_engine_factory(scalar_count=0)
    result = measure_input_trips(LOGIC_DATE, CONFIG, engine_factory=factory)
    assert result == {"finished_trips_read": 0}


def test_count_statement_filters_by_logic_date():
    factory = make_fake_engine_factory(scalar_count=3)
    measure_input_trips(LOGIC_DATE, CONFIG, engine_factory=factory)
    sql, params = factory.engine.executed_statements[0]
    assert "COUNT(*)" in sql.upper()
    assert "logic_date" in sql.lower()
    assert "WHERE" in sql.upper()
    assert params == {"logic_date": LOGIC_DATE}


def test_db_error_raises_value_error():
    factory = make_fake_engine_factory(raises=RuntimeError("db down"))
    with pytest.raises(ValueError) as exc_info:
        measure_input_trips(LOGIC_DATE, CONFIG, engine_factory=factory)
    msg = str(exc_info.value)
    assert str(LOGIC_DATE) in msg
    assert "refined.finished_trips" in msg

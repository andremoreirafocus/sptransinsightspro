from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from refinedtripfacts.services.provision_dim_time import provision_dim_time
from refinedtripfacts.tests.fakes.fake_db_engine import (
    FakeExecuteResult,
    FakeRow,
    make_fake_engine_factory,
)

# São Paulo is UTC-3 (no DST since 2019).
# June 8 2026 00:00 SP = June 8 2026 03:00 UTC
JUNE_8_SP_MIDNIGHT_UTC = datetime(2026, 6, 8, 3, 0, 0, tzinfo=timezone.utc)
JUNE_9_SP_MIDNIGHT_UTC = datetime(2026, 6, 9, 3, 0, 0, tzinfo=timezone.utc)

# Logic date intentionally differs from the trip span to prove they are independent.
LOGIC_DATE = datetime(2026, 6, 10, 3, 0, 0, tzinfo=timezone.utc)

CONFIG = {
    "general": {
        "tables": {
            "finished_trips_table_name": "refined.finished_trips",
            "dim_time_table_name": "refined.dim_time",
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


def _initial_run_factory(min_ts, max_ts, rowcount=24):
    """Simulates a first run: pre-check finds 0 existing rows, INSERT proceeds."""
    return make_fake_engine_factory(
        responses=[
            FakeExecuteResult(row=FakeRow(min_ts, max_ts)),  # span SELECT
            FakeExecuteResult(row=FakeRow(0)),               # count SELECT — nothing exists yet
            FakeExecuteResult(rowcount=rowcount),            # INSERT result
        ]
    )


def test_single_day_span_fills_24_rows():
    factory = _initial_run_factory(JUNE_8_SP_MIDNIGHT_UTC, JUNE_8_SP_MIDNIGHT_UTC)
    provision_dim_time(LOGIC_DATE, CONFIG, engine_factory=factory)
    insert_params = factory.engine.executed_statements[2][1]
    assert len(insert_params) == 24


def test_span_across_midnight_fills_both_days_contiguously():
    factory = _initial_run_factory(JUNE_8_SP_MIDNIGHT_UTC, JUNE_9_SP_MIDNIGHT_UTC, rowcount=48)
    provision_dim_time(LOGIC_DATE, CONFIG, engine_factory=factory)
    insert_params = factory.engine.executed_statements[2][1]
    assert len(insert_params) == 48
    keys = [r["time_key"] for r in insert_params]
    assert keys[0] == 2026060800
    assert keys[-1] == 2026060923
    SP = ZoneInfo("America/Sao_Paulo")
    for i in range(1, len(keys)):
        prev_s, curr_s = str(keys[i - 1]), str(keys[i])
        prev_dt = datetime(int(prev_s[:4]), int(prev_s[4:6]), int(prev_s[6:8]), int(prev_s[8:]), tzinfo=SP)
        curr_dt = datetime(int(curr_s[:4]), int(curr_s[4:6]), int(curr_s[6:8]), int(curr_s[8:]), tzinfo=SP)
        assert (curr_dt - prev_dt).total_seconds() == 3600


def test_time_key_format_is_yyyymmddhh():
    # June 8 2026 14:00 SP = June 8 2026 17:00 UTC (SP is UTC-3)
    ts = datetime(2026, 6, 8, 17, 0, 0, tzinfo=timezone.utc)
    factory = _initial_run_factory(ts, ts)
    provision_dim_time(LOGIC_DATE, CONFIG, engine_factory=factory)
    insert_params = factory.engine.executed_statements[2][1]
    hour_14_rows = [r for r in insert_params if r["hour_of_day"] == 14]
    assert len(hour_14_rows) == 1
    assert hour_14_rows[0]["time_key"] == 2026060814


def test_weekday_and_is_weekend_in_sao_paulo_timezone():
    # June 6 2026 = Saturday (June 8 = Monday, so June 6 = Saturday)
    june_6_utc = datetime(2026, 6, 6, 3, 0, 0, tzinfo=timezone.utc)
    factory = _initial_run_factory(june_6_utc, june_6_utc)
    provision_dim_time(LOGIC_DATE, CONFIG, engine_factory=factory)
    first_row = factory.engine.executed_statements[2][1][0]
    assert first_row["weekday"] == 6
    assert first_row["is_weekend"] is True

    # June 9 2026 = Tuesday
    june_9_utc = datetime(2026, 6, 9, 3, 0, 0, tzinfo=timezone.utc)
    factory2 = _initial_run_factory(june_9_utc, june_9_utc)
    provision_dim_time(LOGIC_DATE, CONFIG, engine_factory=factory2)
    first_row2 = factory2.engine.executed_statements[2][1][0]
    assert first_row2["weekday"] == 2
    assert first_row2["is_weekend"] is False


def test_range_derived_from_source_trip_times():
    # Logic date is June 10 SP; trips span June 8 SP — proves range comes from trip times.
    factory = _initial_run_factory(JUNE_8_SP_MIDNIGHT_UTC, JUNE_8_SP_MIDNIGHT_UTC)
    provision_dim_time(LOGIC_DATE, CONFIG, engine_factory=factory)

    span_sql, span_params = factory.engine.executed_statements[0]
    assert "MIN(trip_start_time)" in span_sql
    assert "MAX(trip_end_time)" in span_sql
    assert span_params == {"logic_date": LOGIC_DATE}

    count_sql, count_params = factory.engine.executed_statements[1]
    assert "date_actual" in count_sql.lower()
    assert count_params["min_date"] == date(2026, 6, 8)
    assert count_params["max_date"] == date(2026, 6, 8)

    insert_params = factory.engine.executed_statements[2][1]
    date_actuals = {r["date_actual"] for r in insert_params}
    assert date(2026, 6, 8) in date_actuals
    assert date(2026, 6, 10) not in date_actuals


def test_returns_zero_when_all_rows_already_exist():
    # Pre-check finds all 24 rows for June 8 already present — INSERT must not be issued.
    factory = make_fake_engine_factory(
        responses=[
            FakeExecuteResult(row=FakeRow(JUNE_8_SP_MIDNIGHT_UTC, JUNE_8_SP_MIDNIGHT_UTC)),
            FakeExecuteResult(row=FakeRow(24)),
        ]
    )
    result = provision_dim_time(LOGIC_DATE, CONFIG, engine_factory=factory)
    assert result == {"rows_ensured": 0}
    assert len(factory.engine.executed_statements) == 2


def test_db_error_raises_value_error():
    factory = make_fake_engine_factory(raises=RuntimeError("db down"))
    with pytest.raises(ValueError) as exc_info:
        provision_dim_time(LOGIC_DATE, CONFIG, engine_factory=factory)
    assert str(LOGIC_DATE) in str(exc_info.value)

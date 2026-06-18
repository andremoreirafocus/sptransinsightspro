import pytest
from datetime import datetime, timezone

from refinedtripfacts.services.measure_persisted_facts import measure_persisted_facts
from refinedtripfacts.tests.fakes.fake_db_engine import (
    FakeExecuteResult,
    FakeRow,
    make_fake_engine_factory,
)

LOGIC_DATE = datetime(2026, 6, 8, 15, 0, 0, tzinfo=timezone.utc)

CONFIG = {
    "general": {
        "tables": {
            "trip_facts_table_name": "refined.trip_facts",
            "dim_time_table_name": "refined.dim_time",
        },
        "quality": {"avg_speed_kmh_max": 120.0},
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


def make_factory(
    persisted=10,
    neg_dur=0,
    neg_dist=0,
    time_inc=0,
    impl_speed=0,
    uncovered=0,
):
    return make_fake_engine_factory(
        responses=[
            FakeExecuteResult(row=FakeRow(persisted, neg_dur, neg_dist, time_inc, impl_speed)),
            FakeExecuteResult(row=FakeRow(uncovered)),
        ]
    )


def test_returns_metrics_dict():
    factory = make_factory(persisted=10, uncovered=2)
    result = measure_persisted_facts(LOGIC_DATE, CONFIG, factory)
    assert result == {
        "persisted_facts": 10,
        "negative_duration": 0,
        "negative_distance": 0,
        "time_incoherent": 0,
        "implausible_speed": 0,
        "uncovered_dim_keys": 2,
    }


def test_count_statement_filters_by_logic_date():
    factory = make_factory()
    measure_persisted_facts(LOGIC_DATE, CONFIG, factory)
    sql, params = factory.engine.executed_statements[0]
    assert "refined.trip_facts" in sql
    assert "logic_date" in sql
    assert params["logic_date"] == LOGIC_DATE


def test_coverage_uses_not_exists_against_dim_time():
    factory = make_factory()
    measure_persisted_facts(LOGIC_DATE, CONFIG, factory)
    sql, params = factory.engine.executed_statements[1]
    assert "NOT EXISTS" in sql.upper()
    assert "refined.dim_time" in sql
    assert "started_at_time_dim_key" in sql
    assert "ended_at_time_dim_key" in sql


def test_value_domain_uses_filter_aggregates_and_config_bound():
    factory = make_factory()
    measure_persisted_facts(LOGIC_DATE, CONFIG, factory)
    sql, params = factory.engine.executed_statements[0]
    assert "FILTER" in sql.upper()
    assert "avg_speed_kmh_max" in params


def test_db_error_raises_value_error():
    factory = make_fake_engine_factory(raises=RuntimeError("db down"))
    with pytest.raises(ValueError) as exc_info:
        measure_persisted_facts(LOGIC_DATE, CONFIG, factory)
    msg = str(exc_info.value)
    assert str(LOGIC_DATE) in msg
    assert "refined.trip_facts" in msg

import datetime as dt
from datetime import timezone

import pytest
from sqlalchemy import text

from refinedtripfacts.services.create_trip_facts import create_trip_facts
from refinedtripfacts.services.measure_persisted_facts import measure_persisted_facts
from refinedtripfacts.services.provision_dim_time import provision_dim_time

pytestmark = pytest.mark.integration

_NOW = dt.datetime.now(timezone.utc).replace(second=0, microsecond=0)
_LOGIC_DATE = _NOW


def _seed_trip(conn, *, trip_id: str, vehicle_id: int, avg_speed_kmh: float = 30.0) -> None:
    conn.execute(
        text(
            "INSERT INTO refined.finished_trips "
            "(trip_id, vehicle_id, trip_start_time, trip_end_time, "
            "duration_seconds, is_circular, distance_meters, avg_speed_kmh, logic_date) "
            "VALUES (:trip_id, :vehicle_id, :trip_start_time, :trip_end_time, "
            ":duration_seconds, :is_circular, :distance_meters, :avg_speed_kmh, :logic_date)"
        ),
        {
            "trip_id": trip_id,
            "vehicle_id": vehicle_id,
            "trip_start_time": _NOW,
            "trip_end_time": _NOW + dt.timedelta(minutes=30),
            "duration_seconds": 1800,
            "is_circular": False,
            "distance_meters": 5000.0,
            "avg_speed_kmh": avg_speed_kmh,
            "logic_date": _LOGIC_DATE,
        },
    )


def test_persisted_facts_counts_actual_rows(pg_engine, test_config):
    with pg_engine.begin() as conn:
        _seed_trip(conn, trip_id="1000-10-0", vehicle_id=1)
        _seed_trip(conn, trip_id="1001-10-0", vehicle_id=2)

    provision_dim_time(_LOGIC_DATE, test_config)
    create_trip_facts(_LOGIC_DATE, test_config)

    result = measure_persisted_facts(_LOGIC_DATE, test_config)

    assert result["persisted_facts"] == 2
    assert result["negative_duration"] == 0
    assert result["negative_distance"] == 0
    assert result["time_incoherent"] == 0
    assert result["implausible_speed"] == 0


def test_uncovered_dim_keys_zero_after_provisioning(pg_engine, test_config):
    with pg_engine.begin() as conn:
        _seed_trip(conn, trip_id="2000-10-0", vehicle_id=3)

    provision_dim_time(_LOGIC_DATE, test_config)
    create_trip_facts(_LOGIC_DATE, test_config)

    result = measure_persisted_facts(_LOGIC_DATE, test_config)

    assert result["uncovered_dim_keys"] == 0


def test_uncovered_dim_keys_detects_missing_dim_row(pg_engine, test_config):
    with pg_engine.begin() as conn:
        _seed_trip(conn, trip_id="3000-10-0", vehicle_id=4)

    provision_dim_time(_LOGIC_DATE, test_config)
    create_trip_facts(_LOGIC_DATE, test_config)

    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM refined.dim_time"))

    result = measure_persisted_facts(_LOGIC_DATE, test_config)

    assert result["uncovered_dim_keys"] > 0


def test_implausible_speed_detected_when_above_max(pg_engine, test_config):
    with pg_engine.begin() as conn:
        _seed_trip(conn, trip_id="4000-10-0", vehicle_id=5, avg_speed_kmh=200.0)

    provision_dim_time(_LOGIC_DATE, test_config)
    create_trip_facts(_LOGIC_DATE, test_config)

    result = measure_persisted_facts(_LOGIC_DATE, test_config)

    assert result["implausible_speed"] == 1

import datetime as dt
from datetime import timezone

import pytest
from sqlalchemy import text

from refinedtripfacts.services.create_trip_facts import create_trip_facts
from refinedtripfacts.services.provision_dim_time import provision_dim_time

pytestmark = pytest.mark.integration

_NOW = dt.datetime.now(timezone.utc).replace(second=0, microsecond=0)
_LOGIC_DATE = _NOW

_COVERAGE_QUERY = """
    SELECT count(*) FROM refined.trip_facts tf
    LEFT JOIN refined.dim_time d1 ON d1.time_key = tf.started_at_time_dim_key
    LEFT JOIN refined.dim_time d2 ON d2.time_key = tf.ended_at_time_dim_key
    WHERE d1.time_key IS NULL OR d2.time_key IS NULL
"""


def _seed_trip(
    conn,
    *,
    trip_id: str,
    vehicle_id: int,
    trip_start_time: dt.datetime,
    trip_end_time: dt.datetime,
    logic_date: dt.datetime,
) -> None:
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
            "trip_start_time": trip_start_time,
            "trip_end_time": trip_end_time,
            "duration_seconds": int((trip_end_time - trip_start_time).total_seconds()),
            "is_circular": False,
            "distance_meters": 5000.0,
            "avg_speed_kmh": 30.0,
            "logic_date": logic_date,
        },
    )


def test_dim_time_covers_all_fact_keys(pg_engine, test_config):
    with pg_engine.begin() as conn:
        _seed_trip(
            conn,
            trip_id="1000-10-0",
            vehicle_id=20,
            trip_start_time=_NOW,
            trip_end_time=_NOW + dt.timedelta(minutes=45),
            logic_date=_LOGIC_DATE,
        )

    provision_dim_time(_LOGIC_DATE, test_config)
    create_trip_facts(_LOGIC_DATE, test_config)

    with pg_engine.connect() as conn:
        uncovered = conn.execute(text(_COVERAGE_QUERY)).fetchone()[0]

    assert uncovered == 0


def test_dim_time_covers_midnight_boundary_trip(pg_engine, test_config):
    # Trip spans SP midnight (03:00 UTC = 00:00 SP, UTC-3, no DST since 2019).
    # trip_start = 02:30 UTC = 23:30 SP (SP date D)
    # trip_end   = 03:30 UTC = 00:30 SP (SP date D+1)
    # ensure_partitions pre-creates the [02:00,03:00) and [03:00,04:00) UTC partitions.
    now = dt.datetime.now(timezone.utc)
    sp_midnight_utc = now.replace(hour=3, minute=0, second=0, microsecond=0)
    if sp_midnight_utc > now:
        sp_midnight_utc -= dt.timedelta(days=1)

    trip_start = sp_midnight_utc - dt.timedelta(minutes=30)
    trip_end = sp_midnight_utc + dt.timedelta(minutes=30)
    logic_date = trip_start

    with pg_engine.begin() as conn:
        _seed_trip(
            conn,
            trip_id="9876-54-0",
            vehicle_id=99,
            trip_start_time=trip_start,
            trip_end_time=trip_end,
            logic_date=logic_date,
        )

    provision_dim_time(logic_date, test_config)
    create_trip_facts(logic_date, test_config)

    with pg_engine.connect() as conn:
        uncovered = conn.execute(text(_COVERAGE_QUERY)).fetchone()[0]

    assert uncovered == 0

    # Confirm the keys genuinely span two different SP dates (boundary was crossed).
    with pg_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT started_at_time_dim_key, ended_at_time_dim_key "
                "FROM refined.trip_facts WHERE logic_date = :logic_date"
            ),
            {"logic_date": logic_date},
        ).fetchone()

    assert str(row[0])[:8] != str(row[1])[:8]

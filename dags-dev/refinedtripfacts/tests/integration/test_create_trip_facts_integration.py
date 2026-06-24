import datetime as dt
from datetime import timezone
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text

from refinedtripfacts.services.create_trip_facts import create_trip_facts

pytestmark = pytest.mark.integration

_SP = ZoneInfo("America/Sao_Paulo")
_NOW = dt.datetime.now(timezone.utc).replace(second=0, microsecond=0)
_LOGIC_DATE = _NOW


def _seed_trip(
    conn,
    *,
    trip_id: str,
    vehicle_id: int,
    trip_start_time: dt.datetime,
    trip_end_time: dt.datetime,
    duration_seconds: int = 1800,
    is_circular: bool = False,
    distance_meters: float = 5000.0,
    avg_speed_kmh: float = 30.0,
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
            "duration_seconds": duration_seconds,
            "is_circular": is_circular,
            "distance_meters": distance_meters,
            "avg_speed_kmh": avg_speed_kmh,
            "logic_date": logic_date,
        },
    )


def test_route_id_and_direction_derived(pg_engine, test_config):
    with pg_engine.begin() as conn:
        _seed_trip(
            conn,
            trip_id="1234-10-0",
            vehicle_id=1,
            trip_start_time=_NOW,
            trip_end_time=_NOW + dt.timedelta(minutes=30),
            logic_date=_LOGIC_DATE,
        )
        _seed_trip(
            conn,
            trip_id="1234-10-1",
            vehicle_id=2,
            trip_start_time=_NOW,
            trip_end_time=_NOW + dt.timedelta(minutes=30),
            logic_date=_LOGIC_DATE,
        )

    create_trip_facts(_LOGIC_DATE, test_config)

    with pg_engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT trip_id, route_id, direction FROM refined.trip_facts "
                "WHERE logic_date = :logic_date ORDER BY trip_id"
            ),
            {"logic_date": _LOGIC_DATE},
        ).fetchall()

    assert len(rows) == 2
    by_trip = {r[0]: r for r in rows}
    assert by_trip["1234-10-0"][1] == "1234-10"
    assert by_trip["1234-10-0"][2] == 1
    assert by_trip["1234-10-1"][1] == "1234-10"
    assert by_trip["1234-10-1"][2] == 2


def test_started_at_time_dim_key_sao_paulo_offset(pg_engine, test_config):
    trip_end = _NOW + dt.timedelta(minutes=30)
    with pg_engine.begin() as conn:
        _seed_trip(
            conn,
            trip_id="5678-20-0",
            vehicle_id=3,
            trip_start_time=_NOW,
            trip_end_time=trip_end,
            logic_date=_LOGIC_DATE,
        )

    create_trip_facts(_LOGIC_DATE, test_config)

    expected_start_key = int(_NOW.astimezone(_SP).strftime("%Y%m%d%H"))
    expected_end_key = int(trip_end.astimezone(_SP).strftime("%Y%m%d%H"))

    with pg_engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT started_at_time_dim_key, ended_at_time_dim_key "
                "FROM refined.trip_facts WHERE logic_date = :logic_date"
            ),
            {"logic_date": _LOGIC_DATE},
        ).fetchone()

    assert row[0] == expected_start_key
    assert row[1] == expected_end_key


def test_duration_interval_derived(pg_engine, test_config):
    with pg_engine.begin() as conn:
        _seed_trip(
            conn,
            trip_id="9999-01-0",
            vehicle_id=4,
            trip_start_time=_NOW,
            trip_end_time=_NOW + dt.timedelta(hours=1),
            duration_seconds=3600,
            logic_date=_LOGIC_DATE,
        )

    create_trip_facts(_LOGIC_DATE, test_config)

    with pg_engine.connect() as conn:
        row = conn.execute(
            text("SELECT duration FROM refined.trip_facts WHERE logic_date = :logic_date"),
            {"logic_date": _LOGIC_DATE},
        ).fetchone()

    assert row[0] == dt.timedelta(hours=1)


def test_idempotent_reload(pg_engine, test_config):
    with pg_engine.begin() as conn:
        _seed_trip(
            conn,
            trip_id="1111-11-0",
            vehicle_id=5,
            trip_start_time=_NOW,
            trip_end_time=_NOW + dt.timedelta(minutes=20),
            logic_date=_LOGIC_DATE,
        )

    result1 = create_trip_facts(_LOGIC_DATE, test_config)
    result2 = create_trip_facts(_LOGIC_DATE, test_config)

    assert result1["inserted_rows"] == 1
    assert result2["facts_derived"] == 1
    assert result2["inserted_rows"] == 0
    assert result2["skipped_rows"] == 1

    with pg_engine.connect() as conn:
        count = conn.execute(
            text(
                "SELECT count(*) FROM refined.trip_facts WHERE logic_date = :logic_date"
            ),
            {"logic_date": _LOGIC_DATE},
        ).fetchone()[0]

    assert count == 1


def test_measured_counts(pg_engine, test_config):
    with pg_engine.begin() as conn:
        for i, (trip_id, vehicle_id) in enumerate(
            [("2222-22-0", 10), ("3333-33-0", 11), ("4444-44-0", 12)]
        ):
            _seed_trip(
                conn,
                trip_id=trip_id,
                vehicle_id=vehicle_id,
                trip_start_time=_NOW + dt.timedelta(minutes=i),
                trip_end_time=_NOW + dt.timedelta(minutes=i + 30),
                logic_date=_LOGIC_DATE,
            )

    result = create_trip_facts(_LOGIC_DATE, test_config)

    assert result["facts_derived"] == 3
    assert result["inserted_rows"] == 3
    assert result["skipped_rows"] == 0

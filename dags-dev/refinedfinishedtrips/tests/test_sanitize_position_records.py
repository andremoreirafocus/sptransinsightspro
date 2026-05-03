from datetime import datetime, timedelta, timezone

from refinedfinishedtrips.services.sanitize_position_records import (
    _as_datetime,
    _calculate_speed_kmh,
    sanitize_position_records,
)

BASE_TS = datetime(2026, 4, 14, 10, 0, 0, tzinfo=timezone.utc)


def _position_record(
    offset_seconds: int,
    veiculo_lat: float,
    veiculo_long: float,
):
    return {
        "veiculo_ts": BASE_TS + timedelta(seconds=offset_seconds),
        "veiculo_lat": veiculo_lat,
        "veiculo_long": veiculo_long,
    }


def test_sanitize_position_records_returns_short_sequences_unchanged():
    position_records = [
        _position_record(0, -23.460811, -46.687363),
        _position_record(60, -23.461000, -46.688000),
    ]

    cleaned_records, sanitization = sanitize_position_records(position_records)

    assert cleaned_records == position_records
    assert sanitization == {"dropped_points_count": 0, "violations": []}


def test_sanitize_position_records_drops_single_spatial_discontinuity():
    position_records = [
        _position_record(0, -23.460811, -46.687363),
        _position_record(60, -23.526252, -46.667517),
        _position_record(120, -23.460857, -46.687422),
    ]

    cleaned_records, sanitization = sanitize_position_records(position_records)

    assert len(cleaned_records) == 2
    assert cleaned_records[0]["veiculo_ts"] == BASE_TS
    assert cleaned_records[1]["veiculo_ts"] == BASE_TS + timedelta(seconds=120)
    assert sanitization["dropped_points_count"] == 1
    assert len(sanitization["violations"]) == 1
    violation = sanitization["violations"][0]
    assert violation["dropped_index"] == 1
    assert violation["veiculo_ts"] == BASE_TS + timedelta(seconds=60)
    assert {
        "prev_to_current_speed_kmh",
        "current_to_next_speed_kmh",
        "prev_to_next_speed_kmh",
    }.issubset(violation.keys())


def test_sanitize_position_records_keeps_normal_sequence_unchanged():
    position_records = [
        _position_record(0, -23.460811, -46.687363),
        _position_record(120, -23.461000, -46.688000),
        _position_record(240, -23.461500, -46.689000),
    ]

    cleaned_records, sanitization = sanitize_position_records(position_records)

    assert cleaned_records == position_records
    assert sanitization["dropped_points_count"] == 0
    assert sanitization["violations"] == []


def test_sanitize_position_records_does_not_drop_two_consecutive_bad_points():
    position_records = [
        _position_record(0, -23.460811, -46.687363),
        _position_record(60, -23.526252, -46.667517),
        _position_record(120, -23.526241, -46.667897),
        _position_record(180, -23.460857, -46.687422),
    ]

    cleaned_records, sanitization = sanitize_position_records(position_records)

    assert cleaned_records == position_records
    assert sanitization["dropped_points_count"] == 0
    assert sanitization["violations"] == []


def test_sanitize_position_records_skips_validation_when_spatial_keys_are_missing():
    position_records = [
        _position_record(0, -23.460811, -46.687363),
        {
            "veiculo_ts": BASE_TS + timedelta(seconds=60),
            "veiculo_lat": None,
            "veiculo_long": -46.667517,
        },
        _position_record(120, -23.460857, -46.687422),
    ]

    cleaned_records, sanitization = sanitize_position_records(position_records)

    assert cleaned_records == position_records
    assert sanitization == {"dropped_points_count": 0, "violations": []}


def test_calculate_speed_kmh_returns_none_for_zero_elapsed_time():
    first_record = _position_record(0, -23.460811, -46.687363)
    second_record = _position_record(0, -23.460857, -46.687422)

    speed = _calculate_speed_kmh(first_record, second_record)

    assert speed is None


def test_calculate_speed_kmh_returns_none_for_negative_elapsed_time():
    first_record = _position_record(120, -23.460811, -46.687363)
    second_record = _position_record(60, -23.460857, -46.687422)

    speed = _calculate_speed_kmh(first_record, second_record)

    assert speed is None


def test_as_datetime_accepts_iso_string_values():
    value = "2026-04-14T10:00:00+00:00"

    result = _as_datetime(value)

    assert result == BASE_TS

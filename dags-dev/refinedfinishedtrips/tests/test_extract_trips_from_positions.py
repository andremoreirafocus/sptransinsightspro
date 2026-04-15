from datetime import datetime, timedelta, timezone

import pytest

from refinedfinishedtrips.services.extract_trips_from_positions import (
    extract_raw_trips_metadata,
    filter_healthy_trips,
    generate_trips_table,
    get_trip_id,
)
from refinedfinishedtrips.services.extract_trips_per_line_per_vehicle_pandas import (
    extract_trips_per_line_per_vehicle_pandas,
)

BASE_TS = datetime(2026, 4, 14, 10, 0, 0, tzinfo=timezone.utc)


def _pos(sentido, offset_seconds=0, is_circular=False):
    return {
        "linha_sentido": sentido,
        "linha_lt": "1234-10",
        "veiculo_id": 100,
        "veiculo_ts": BASE_TS + timedelta(seconds=offset_seconds),
        "is_circular": is_circular,
    }


# --- get_trip_id ---


def test_get_trip_id_sentido_1():
    assert get_trip_id("1234-10", 1) == "1234-10-0"


def test_get_trip_id_sentido_2():
    assert get_trip_id("1234-10", 2) == "1234-10-1"


def test_get_trip_id_unknown_sentido():
    assert get_trip_id("1234-10", 9) == "1234-10-999"


# --- extract_raw_trips_metadata ---


def test_extract_raw_trips_metadata_empty_list():
    assert extract_raw_trips_metadata([]) == []


def test_extract_raw_trips_metadata_single_record():
    assert extract_raw_trips_metadata([_pos(1)]) == []


def test_extract_raw_trips_metadata_no_direction_change():
    assert extract_raw_trips_metadata([_pos(1), _pos(1), _pos(1)]) == []


def test_extract_raw_trips_metadata_one_direction_change_returns_empty():
    # First change is always discarded; the last trip is also always discarded
    records = [_pos(1), _pos(2), _pos(2)]
    assert extract_raw_trips_metadata(records) == []


def test_extract_raw_trips_metadata_two_direction_changes_returns_one_trip():
    # [1,1,2,2,1] → change at i=2 (discarded), change at i=4 (kept: {start:2, end:3})
    records = [_pos(1), _pos(1), _pos(2), _pos(2), _pos(1)]
    result = extract_raw_trips_metadata(records)
    assert len(result) == 1
    assert result[0]["start_position_index"] == 2
    assert result[0]["end_position_index"] == 3
    assert result[0]["sentido"] == 2


def test_extract_raw_trips_metadata_three_direction_changes_returns_two_trips():
    # [1,2,2,1,1,2] → change at i=1 (discarded), i=3 (kept), i=5 (kept)
    records = [_pos(1), _pos(2), _pos(2), _pos(1), _pos(1), _pos(2)]
    result = extract_raw_trips_metadata(records)
    assert len(result) == 2


# --- filter_healthy_trips ---


def test_filter_healthy_trips_empty_input():
    assert filter_healthy_trips([], []) == []


def test_filter_healthy_trips_non_circular_within_range_kept():
    records = [_pos(1, 0), _pos(1, 3600)]
    trips = [{"start_position_index": 0, "end_position_index": 1, "sentido": 1}]
    assert len(filter_healthy_trips(trips, records)) == 1


def test_filter_healthy_trips_non_circular_too_short_filtered():
    records = [_pos(1, 0), _pos(1, 1000)]
    trips = [{"start_position_index": 0, "end_position_index": 1, "sentido": 1}]
    assert filter_healthy_trips(trips, records) == []


def test_filter_healthy_trips_non_circular_too_long_filtered():
    records = [_pos(1, 0), _pos(1, 12000)]
    trips = [{"start_position_index": 0, "end_position_index": 1, "sentido": 1}]
    assert filter_healthy_trips(trips, records) == []


def test_filter_healthy_trips_circular_above_circular_min_kept():
    # 1500s > 1200 (circular min) but < 1800 (non-circular min) → kept for circular
    records = [_pos(1, 0, is_circular=True), _pos(1, 1500, is_circular=True)]
    trips = [{"start_position_index": 0, "end_position_index": 1, "sentido": 1}]
    assert len(filter_healthy_trips(trips, records)) == 1


def test_filter_healthy_trips_circular_too_short_filtered():
    records = [_pos(1, 0, is_circular=True), _pos(1, 800, is_circular=True)]
    trips = [{"start_position_index": 0, "end_position_index": 1, "sentido": 1}]
    assert filter_healthy_trips(trips, records) == []


# --- generate_trips_table ---


def test_generate_trips_table_tuple_has_seven_fields():
    start_ts = BASE_TS
    end_ts = BASE_TS + timedelta(seconds=3600)
    records = [
        {"veiculo_ts": start_ts, "is_circular": False},
        {"veiculo_ts": end_ts, "is_circular": False},
    ]
    trips_metadata = [{"start_position_index": 0, "end_position_index": 1, "sentido": 1}]
    result = generate_trips_table(records, trips_metadata, "1234-10", 100)
    assert len(result) == 1
    assert len(result[0]) == 7


def test_generate_trips_table_trip_id_from_linha_sentido():
    records = [
        {"veiculo_ts": BASE_TS, "is_circular": False},
        {"veiculo_ts": BASE_TS + timedelta(seconds=3600), "is_circular": False},
    ]
    trips_metadata = [{"start_position_index": 0, "end_position_index": 1, "sentido": 2}]
    result = generate_trips_table(records, trips_metadata, "1234-10", 100)
    assert result[0][0] == "1234-10-1"


def test_generate_trips_table_duration_equals_end_minus_start():
    start_ts = BASE_TS
    end_ts = BASE_TS + timedelta(seconds=3600)
    records = [
        {"veiculo_ts": start_ts, "is_circular": False},
        {"veiculo_ts": end_ts, "is_circular": False},
    ]
    trips_metadata = [{"start_position_index": 0, "end_position_index": 1, "sentido": 1}]
    result = generate_trips_table(records, trips_metadata, "1234-10", 100)
    assert result[0][4] == end_ts - start_ts


def test_generate_trips_table_average_speed_always_zero():
    records = [
        {"veiculo_ts": BASE_TS, "is_circular": False},
        {"veiculo_ts": BASE_TS + timedelta(seconds=3600), "is_circular": False},
    ]
    trips_metadata = [{"start_position_index": 0, "end_position_index": 1, "sentido": 1}]
    result = generate_trips_table(records, trips_metadata, "1234-10", 100)
    assert result[0][6] == 0.0


# --- extract_trips_per_line_per_vehicle_pandas ---


def test_extract_trips_per_vehicle_empty_positions_returns_none():
    assert extract_trips_per_line_per_vehicle_pandas([], 0, 0, "1234-10", 100) is None


def test_extract_trips_per_vehicle_invalid_indices_returns_none():
    positions = [_pos(1)]
    assert extract_trips_per_line_per_vehicle_pandas(positions, 5, 2, "1234-10", 100) is None


def test_extract_trips_per_vehicle_no_direction_change_returns_none():
    positions = [_pos(1, i * 60) for i in range(5)]
    assert extract_trips_per_line_per_vehicle_pandas(positions, 0, 4, "1234-10", 100) is None


def test_extract_trips_per_vehicle_two_direction_changes_returns_one_trip():
    # [1@0s, 2@0s, 2@3600s, 1@3600s, 1@7200s]
    # extract_raw_trips: change at i=1 (discarded), change at i=3 (kept: {start:1,end:2,sentido:2})
    # filter: duration = positions[2].ts - positions[1].ts = 3600s > 1800 → kept
    positions = [
        _pos(1, 0), _pos(2, 0), _pos(2, 3600), _pos(1, 3600), _pos(1, 7200),
    ]
    result = extract_trips_per_line_per_vehicle_pandas(positions, 0, 4, "1234-10", 100)
    assert result is not None
    assert len(result) == 1

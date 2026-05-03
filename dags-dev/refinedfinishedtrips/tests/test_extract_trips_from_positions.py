from datetime import datetime, timedelta, timezone

from refinedfinishedtrips.services.extract_trips_from_positions import (
    extract_raw_trips_metadata,
    generate_trips_table,
    get_trip_id,
)
from refinedfinishedtrips.services.extract_trips_per_line_per_vehicle import (
    extract_trips_per_line_per_vehicle,
)

BASE_TS = datetime(2026, 4, 14, 10, 0, 0, tzinfo=timezone.utc)
THRESHOLD = 100
LINHA_LT = "1234-10"
VEICULO_ID = 100


def _pos(
    distance_to_first_stop: float,
    distance_to_last_stop: float,
    offset_seconds: int = 0,
    linha_sentido: int = 1,
    is_circular: bool = False,
    veiculo_lat: float = -23.5,
    veiculo_long: float = -46.6,
):
    return {
        "distance_to_first_stop": distance_to_first_stop,
        "distance_to_last_stop": distance_to_last_stop,
        "linha_sentido": linha_sentido,
        "linha_lt": LINHA_LT,
        "veiculo_id": VEICULO_ID,
        "veiculo_ts": BASE_TS + timedelta(seconds=offset_seconds),
        "is_circular": is_circular,
        "veiculo_lat": veiculo_lat,
        "veiculo_long": veiculo_long,
    }


def _geo_pos(
    veiculo_lat: float,
    veiculo_long: float,
    offset_seconds: int,
    linha_sentido: int = 1,
):
    return _pos(
        1000,
        1000,
        offset_seconds=offset_seconds,
        linha_sentido=linha_sentido,
        veiculo_lat=veiculo_lat,
        veiculo_long=veiculo_long,
    )


# --- get_trip_id ---


def test_get_trip_id_sentido_1():
    assert get_trip_id("1234-10", 1) == "1234-10-0"


def test_get_trip_id_sentido_2():
    assert get_trip_id("1234-10", 2) == "1234-10-1"


def test_get_trip_id_unknown_sentido():
    assert get_trip_id("1234-10", 9) == "1234-10-999"


# --- extract_raw_trips_metadata ---


def test_extract_raw_trips_metadata_empty_list():
    assert extract_raw_trips_metadata([], THRESHOLD) == []


def test_extract_raw_trips_metadata_single_record():
    assert extract_raw_trips_metadata([_pos(50, 3000)], THRESHOLD) == []


def test_extract_raw_trips_metadata_bus_never_exits_departure_zone():
    # Bus stays within threshold of first stop the entire window
    position_records = [_pos(50, 3000), _pos(80, 3100), _pos(60, 3200)]
    assert extract_raw_trips_metadata(position_records, THRESHOLD) == []


def test_extract_raw_trips_metadata_bus_departs_but_never_reaches_destination():
    # Bus leaves first stop zone but never gets close to last stop
    position_records = [
        _pos(50, 3000, offset_seconds=0),
        _pos(500, 2000, offset_seconds=60),
        _pos(1000, 1500, offset_seconds=120),
    ]
    assert extract_raw_trips_metadata(position_records, THRESHOLD) == []


def test_extract_raw_trips_metadata_bus_mid_trip_no_departure_event():
    # Bus is already mid-route — no departure from either stop visible
    position_records = [
        _pos(800, 1500, offset_seconds=0),
        _pos(900, 1200, offset_seconds=60),
        _pos(1100, 900, offset_seconds=120),
    ]
    assert extract_raw_trips_metadata(position_records, THRESHOLD) == []


def test_extract_raw_trips_metadata_one_complete_trip_first_to_last():
    position_records = [
        _pos(50, 3000, offset_seconds=0, linha_sentido=1),       # at first stop
        _pos(80, 2800, offset_seconds=60, linha_sentido=1),      # still at first stop (dwell)
        _pos(500, 2000, offset_seconds=120, linha_sentido=1),    # departed
        _pos(1000, 1200, offset_seconds=180, linha_sentido=1),   # mid-route
        _pos(2000, 60, offset_seconds=240, linha_sentido=1),     # arrived at last stop
    ]
    result = extract_raw_trips_metadata(position_records, THRESHOLD)
    assert len(result) == 1
    assert result[0]["sentido"] == 1
    assert result[0]["start_position_index"] == 1   # last record in the departure zone
    assert result[0]["end_position_index"] == 4


def test_extract_raw_trips_metadata_one_complete_trip_last_to_first():
    position_records = [
        _pos(3000, 50, offset_seconds=0, linha_sentido=2),       # at last stop
        _pos(2800, 80, offset_seconds=60, linha_sentido=2),      # still at last stop (dwell)
        _pos(2000, 500, offset_seconds=120, linha_sentido=2),    # departed
        _pos(1200, 1000, offset_seconds=180, linha_sentido=2),   # mid-route
        _pos(60, 2000, offset_seconds=240, linha_sentido=2),     # arrived at first stop
    ]
    result = extract_raw_trips_metadata(position_records, THRESHOLD)
    assert len(result) == 1
    assert result[0]["sentido"] == 2
    assert result[0]["start_position_index"] == 1
    assert result[0]["end_position_index"] == 4


def test_extract_raw_trips_metadata_two_consecutive_trips():
    position_records = [
        _pos(50, 3000, offset_seconds=0, linha_sentido=1),       # at first stop
        _pos(500, 2000, offset_seconds=60, linha_sentido=1),     # departed → trip 1 start at index 0
        _pos(2000, 60, offset_seconds=120, linha_sentido=1),     # arrived at last stop → trip 1 end
        _pos(2800, 80, offset_seconds=180, linha_sentido=2),     # still at last stop (dwell)
        _pos(2000, 500, offset_seconds=240, linha_sentido=2),    # departed → trip 2 start at index 3
        _pos(60, 2000, offset_seconds=300, linha_sentido=2),     # arrived at first stop → trip 2 end
    ]
    result = extract_raw_trips_metadata(position_records, THRESHOLD)
    assert len(result) == 2
    assert result[0]["sentido"] == 1
    assert result[1]["sentido"] == 2


def test_extract_raw_trips_metadata_dwell_time_excluded_from_trip_start():
    # Three records within the departure zone — trip_start_record_index must be the last one
    position_records = [
        _pos(30, 3000, offset_seconds=0, linha_sentido=1),      # dwell record 1
        _pos(60, 3100, offset_seconds=60, linha_sentido=1),     # dwell record 2
        _pos(80, 3200, offset_seconds=120, linha_sentido=1),    # dwell record 3 — last in zone
        _pos(500, 2500, offset_seconds=180, linha_sentido=1),   # departed
        _pos(2500, 60, offset_seconds=240, linha_sentido=1),    # arrived at last stop
    ]
    result = extract_raw_trips_metadata(position_records, THRESHOLD)
    assert len(result) == 1
    assert result[0]["start_position_index"] == 2


def test_extract_raw_trips_metadata_circular_route_closes_on_terminal_return():
    # Circular routes may use the same anchor for start and end; after real departure,
    # returning to the anchor closes the trip.
    position_records = [
        _pos(50, 50, offset_seconds=0, is_circular=True, linha_sentido=1),
        _pos(500, 2000, offset_seconds=60, is_circular=True, linha_sentido=1),
        _pos(2000, 60, offset_seconds=120, is_circular=True, linha_sentido=1),
    ]
    result = extract_raw_trips_metadata(position_records, THRESHOLD)
    assert len(result) == 1
    assert result[0]["start_position_index"] == 0
    assert result[0]["end_position_index"] == 2
    assert result[0]["sentido"] == 1


def test_extract_raw_trips_metadata_divergent_linha_sentido_sets_source_sentido_discrepancy():
    position_records = [
        _pos(50, 3000, offset_seconds=0, linha_sentido=2),      # at first stop, wrong sentido
        _pos(500, 2000, offset_seconds=60, linha_sentido=2),    # departed
        _pos(2000, 60, offset_seconds=120, linha_sentido=2),    # arrived — last record sentido=2, derived=1
    ]
    result = extract_raw_trips_metadata(position_records, THRESHOLD)
    assert len(result) == 1
    assert result[0]["sentido"] == 1
    assert result[0]["source_sentido_discrepancy"] is True


def test_extract_raw_trips_metadata_boundary_sentido_flip_does_not_warn():
    position_records = [
        _pos(50, 3000, offset_seconds=0, linha_sentido=2),      # boundary at departure
        _pos(500, 2000, offset_seconds=60, linha_sentido=1),    # in motion
        _pos(2000, 60, offset_seconds=120, linha_sentido=2),    # boundary at arrival
    ]
    result = extract_raw_trips_metadata(position_records, THRESHOLD)
    assert len(result) == 1
    assert result[0]["sentido"] == 1
    assert result[0]["source_sentido_discrepancy"] is False


def test_extract_raw_trips_metadata_ambiguous_in_trip_sentido_does_not_warn():
    position_records = [
        _pos(50, 3000, offset_seconds=0, linha_sentido=1),
        _pos(500, 2000, offset_seconds=60, linha_sentido=1),
        _pos(800, 1500, offset_seconds=120, linha_sentido=2),
        _pos(2000, 60, offset_seconds=180, linha_sentido=1),
    ]
    result = extract_raw_trips_metadata(position_records, THRESHOLD)
    assert len(result) == 1
    assert result[0]["source_sentido_discrepancy"] is False


def test_extract_raw_trips_metadata_circular_syncs_only_after_first_anchor_occurrence():
    position_records = [
        _pos(600, 700, offset_seconds=0, is_circular=True, linha_sentido=1),
        _pos(700, 800, offset_seconds=60, is_circular=True, linha_sentido=1),
        _pos(50, 400, offset_seconds=120, is_circular=True, linha_sentido=1, veiculo_lat=-23.5000, veiculo_long=-46.6000),
        _pos(60, 420, offset_seconds=180, is_circular=True, linha_sentido=1, veiculo_lat=-23.5000, veiculo_long=-46.6000),
        _pos(500, 1200, offset_seconds=240, is_circular=True, linha_sentido=1, veiculo_lat=-23.5050, veiculo_long=-46.6050),
        _pos(700, 800, offset_seconds=300, is_circular=True, linha_sentido=1, veiculo_lat=-23.5100, veiculo_long=-46.6100),
        _pos(60, 430, offset_seconds=360, is_circular=True, linha_sentido=2, veiculo_lat=-23.5001, veiculo_long=-46.6001),
    ]

    result = extract_raw_trips_metadata(position_records, THRESHOLD)

    assert len(result) == 1
    assert result[0]["start_position_index"] == 3
    assert result[0]["end_position_index"] == 5
    assert result[0]["sentido"] == 1


def test_extract_raw_trips_metadata_circular_removes_waiting_time_from_trip_start():
    position_records = [
        _pos(50, 400, offset_seconds=0, is_circular=True, linha_sentido=1, veiculo_lat=-23.5000, veiculo_long=-46.6000),
        _pos(60, 420, offset_seconds=60, is_circular=True, linha_sentido=1, veiculo_lat=-23.5000, veiculo_long=-46.6000),
        _pos(55, 410, offset_seconds=120, is_circular=True, linha_sentido=1, veiculo_lat=-23.5000, veiculo_long=-46.6000),
        _pos(500, 1200, offset_seconds=180, is_circular=True, linha_sentido=1, veiculo_lat=-23.5050, veiculo_long=-46.6050),
        _pos(900, 900, offset_seconds=240, is_circular=True, linha_sentido=1, veiculo_lat=-23.5100, veiculo_long=-46.6100),
        _pos(60, 430, offset_seconds=300, is_circular=True, linha_sentido=2, veiculo_lat=-23.5001, veiculo_long=-46.6001),
    ]

    result = extract_raw_trips_metadata(position_records, THRESHOLD)

    assert len(result) == 1
    assert result[0]["start_position_index"] == 2
    assert result[0]["end_position_index"] == 4


def test_extract_raw_trips_metadata_circular_direction_change_after_movement_creates_trip():
    position_records = [
        _pos(50, 400, offset_seconds=0, is_circular=True, linha_sentido=1, veiculo_lat=-23.5000, veiculo_long=-46.6000),
        _pos(500, 1200, offset_seconds=60, is_circular=True, linha_sentido=1, veiculo_lat=-23.5050, veiculo_long=-46.6050),
        _pos(800, 1000, offset_seconds=120, is_circular=True, linha_sentido=1, veiculo_lat=-23.5100, veiculo_long=-46.6100),
        _pos(70, 450, offset_seconds=180, is_circular=True, linha_sentido=2, veiculo_lat=-23.5001, veiculo_long=-46.6001),
    ]

    result = extract_raw_trips_metadata(position_records, THRESHOLD)

    assert len(result) == 1
    assert result[0]["start_position_index"] == 0
    assert result[0]["end_position_index"] == 2
    assert result[0]["sentido"] == 1


def test_extract_raw_trips_metadata_circular_movement_inside_anchor_does_not_create_false_trip():
    position_records = [
        _pos(5, 5, offset_seconds=0, is_circular=True, linha_sentido=2, veiculo_lat=-23.469887, veiculo_long=-46.721775),
        _pos(5, 5, offset_seconds=120, is_circular=True, linha_sentido=2, veiculo_lat=-23.469887, veiculo_long=-46.721775),
        _pos(64, 64, offset_seconds=240, is_circular=True, linha_sentido=2, veiculo_lat=-23.470407, veiculo_long=-46.721549),
        _pos(24, 24, offset_seconds=360, is_circular=True, linha_sentido=1, veiculo_lat=-23.469688, veiculo_long=-46.721875),
        _pos(231, 231, offset_seconds=480, is_circular=True, linha_sentido=1, veiculo_lat=-23.471774, veiculo_long=-46.720854),
        _pos(900, 900, offset_seconds=600, is_circular=True, linha_sentido=1, veiculo_lat=-23.477580, veiculo_long=-46.719051),
        _pos(1411, 1411, offset_seconds=720, is_circular=True, linha_sentido=1, veiculo_lat=-23.482070, veiculo_long=-46.717945),
        _pos(1610, 1610, offset_seconds=840, is_circular=True, linha_sentido=1, veiculo_lat=-23.484344, veiculo_long=-46.722791),
        _pos(1895, 1895, offset_seconds=960, is_circular=True, linha_sentido=2, veiculo_lat=-23.486371, veiculo_long=-46.726582),
    ]

    result = extract_raw_trips_metadata(position_records, THRESHOLD)

    assert len(result) == 1
    assert result[0]["start_position_index"] == 3
    assert result[0]["end_position_index"] == 7
    assert result[0]["sentido"] == 1


def test_extract_raw_trips_metadata_circular_terminal_return_closes_trip_after_off_terminal_sentido_change():
    position_records = [
        _pos(5, 5, offset_seconds=0, is_circular=True, linha_sentido=2, veiculo_lat=-23.469887, veiculo_long=-46.721775),
        _pos(5, 5, offset_seconds=120, is_circular=True, linha_sentido=2, veiculo_lat=-23.469887, veiculo_long=-46.721775),
        _pos(24, 24, offset_seconds=240, is_circular=True, linha_sentido=1, veiculo_lat=-23.469688, veiculo_long=-46.721875),
        _pos(231, 231, offset_seconds=360, is_circular=True, linha_sentido=1, veiculo_lat=-23.471774, veiculo_long=-46.720854),
        _pos(900, 900, offset_seconds=480, is_circular=True, linha_sentido=1, veiculo_lat=-23.477580, veiculo_long=-46.719051),
        _pos(1411, 1411, offset_seconds=600, is_circular=True, linha_sentido=1, veiculo_lat=-23.482070, veiculo_long=-46.717945),
        _pos(1610, 1610, offset_seconds=720, is_circular=True, linha_sentido=1, veiculo_lat=-23.484344, veiculo_long=-46.722791),
        _pos(1895, 1895, offset_seconds=840, is_circular=True, linha_sentido=2, veiculo_lat=-23.486371, veiculo_long=-46.726582),
        _pos(1661, 1661, offset_seconds=960, is_circular=True, linha_sentido=2, veiculo_lat=-23.484783, veiculo_long=-46.723133),
        _pos(1406, 1406, offset_seconds=1080, is_circular=True, linha_sentido=2, veiculo_lat=-23.482116, veiculo_long=-46.718284),
        _pos(529, 529, offset_seconds=1200, is_circular=True, linha_sentido=2, veiculo_lat=-23.474263, veiculo_long=-46.719772),
        _pos(5, 5, offset_seconds=1320, is_circular=True, linha_sentido=2, veiculo_lat=-23.469887, veiculo_long=-46.721775),
    ]

    result = extract_raw_trips_metadata(position_records, THRESHOLD)

    assert len(result) == 2
    assert result[0]["start_position_index"] == 2
    assert result[0]["end_position_index"] == 6
    assert result[0]["sentido"] == 1
    assert result[1]["start_position_index"] == 7
    assert result[1]["end_position_index"] == 11
    assert result[1]["sentido"] == 2


def test_extract_raw_trips_metadata_circular_noisy_direction_flip_without_movement_does_not_create_trip():
    position_records = [
        _pos(50, 400, offset_seconds=0, is_circular=True, linha_sentido=1, veiculo_lat=-23.5000, veiculo_long=-46.6000),
        _pos(55, 410, offset_seconds=60, is_circular=True, linha_sentido=2, veiculo_lat=-23.5000, veiculo_long=-46.6000),
        _pos(60, 420, offset_seconds=120, is_circular=True, linha_sentido=2, veiculo_lat=-23.5000, veiculo_long=-46.6000),
        _pos(500, 1200, offset_seconds=180, is_circular=True, linha_sentido=2, veiculo_lat=-23.5050, veiculo_long=-46.6050),
    ]

    result = extract_raw_trips_metadata(position_records, THRESHOLD)

    assert result == []


# --- generate_trips_table ---


def test_generate_trips_table_tuple_has_seven_fields():
    start_ts = BASE_TS
    end_ts = BASE_TS + timedelta(seconds=3600)
    position_records = [
        {"veiculo_ts": start_ts, "is_circular": False},
        {"veiculo_ts": end_ts, "is_circular": False},
    ]
    trips_metadata = [{"start_position_index": 0, "end_position_index": 1, "sentido": 1}]
    result = generate_trips_table(position_records, trips_metadata, LINHA_LT, VEICULO_ID)
    assert len(result) == 1
    assert len(result[0]) == 7


def test_generate_trips_table_trip_id_from_linha_and_derived_sentido():
    position_records = [
        {"veiculo_ts": BASE_TS, "is_circular": False},
        {"veiculo_ts": BASE_TS + timedelta(seconds=3600), "is_circular": False},
    ]
    trips_metadata = [{"start_position_index": 0, "end_position_index": 1, "sentido": 2}]
    result = generate_trips_table(position_records, trips_metadata, LINHA_LT, VEICULO_ID)
    assert result[0][0] == "1234-10-1"


def test_generate_trips_table_duration_equals_end_minus_start():
    start_ts = BASE_TS
    end_ts = BASE_TS + timedelta(seconds=3600)
    position_records = [
        {"veiculo_ts": start_ts, "is_circular": False},
        {"veiculo_ts": end_ts, "is_circular": False},
    ]
    trips_metadata = [{"start_position_index": 0, "end_position_index": 1, "sentido": 1}]
    result = generate_trips_table(position_records, trips_metadata, LINHA_LT, VEICULO_ID)
    assert result[0][4] == end_ts - start_ts


def test_generate_trips_table_average_speed_always_zero():
    position_records = [
        {"veiculo_ts": BASE_TS, "is_circular": False},
        {"veiculo_ts": BASE_TS + timedelta(seconds=3600), "is_circular": False},
    ]
    trips_metadata = [{"start_position_index": 0, "end_position_index": 1, "sentido": 1}]
    result = generate_trips_table(position_records, trips_metadata, LINHA_LT, VEICULO_ID)
    assert result[0][6] == 0.0


# --- extract_trips_per_line_per_vehicle ---


def test_extract_trips_per_vehicle_empty_positions_returns_empty():
    trips, mismatches, dropped_points = extract_trips_per_line_per_vehicle([], 0, 0, LINHA_LT, VEICULO_ID, THRESHOLD)
    assert trips == []
    assert mismatches == 0
    assert dropped_points == 0


def test_extract_trips_per_vehicle_invalid_indices_returns_empty():
    position_records = [_pos(50, 3000)]
    trips, mismatches, dropped_points = extract_trips_per_line_per_vehicle(position_records, 5, 2, LINHA_LT, VEICULO_ID, THRESHOLD)
    assert trips == []
    assert mismatches == 0
    assert dropped_points == 0


def test_extract_trips_per_vehicle_no_departure_event_returns_empty():
    # Bus mid-route the entire window — no complete trip
    position_records = [_pos(800, 1500, offset_seconds=i * 60) for i in range(5)]
    trips, mismatches, dropped_points = extract_trips_per_line_per_vehicle(position_records, 0, 4, LINHA_LT, VEICULO_ID, THRESHOLD)
    assert trips == []
    assert mismatches == 0
    assert dropped_points == 0


def test_extract_trips_per_vehicle_one_complete_trip_detected():
    position_records = [
        _pos(50, 3000, offset_seconds=0, linha_sentido=1),
        _pos(500, 2000, offset_seconds=60, linha_sentido=1),
        _pos(2000, 60, offset_seconds=120, linha_sentido=1),
    ]
    trips, mismatches, dropped_points = extract_trips_per_line_per_vehicle(
        position_records, 0, 2, LINHA_LT, VEICULO_ID, THRESHOLD
    )
    assert len(trips) == 1
    assert trips[0][0] == "1234-10-0"
    assert mismatches == 0
    assert dropped_points == 0


def test_extract_trips_per_vehicle_returns_drop_count_when_sanitization_drops_point():
    position_records = [
        _pos(50, 3000, offset_seconds=0, linha_sentido=1, veiculo_lat=-23.460811, veiculo_long=-46.687363),
        _pos(3000, 50, offset_seconds=60, linha_sentido=1, veiculo_lat=-23.526252, veiculo_long=-46.667517),
        _pos(500, 2000, offset_seconds=120, linha_sentido=1, veiculo_lat=-23.461000, veiculo_long=-46.688000),
        _pos(1200, 1000, offset_seconds=180, linha_sentido=1, veiculo_lat=-23.480000, veiculo_long=-46.690000),
        _pos(2000, 60, offset_seconds=240, linha_sentido=1, veiculo_lat=-23.526252, veiculo_long=-46.667517),
    ]

    trips, mismatches, dropped_points = extract_trips_per_line_per_vehicle(
        position_records, 0, len(position_records) - 1, LINHA_LT, VEICULO_ID, THRESHOLD
    )

    assert len(trips) == 1
    assert mismatches == 0
    assert dropped_points == 1

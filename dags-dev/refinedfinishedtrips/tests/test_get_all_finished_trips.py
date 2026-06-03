from datetime import datetime, timezone

import pandas as pd

from refinedfinishedtrips.services.get_all_finished_trips import get_all_finished_trips

_BASE_TS = datetime(2026, 4, 14, 10, 0, 0, tzinfo=timezone.utc)

_CONFIG = {
    "general": {
        "trip_detection": {"stop_proximity_threshold_meters": 100},
    }
}

_FAKE_TRIP = ("trip_id", 1, _BASE_TS, _BASE_TS, 60, False, 10000.0, 60.0)


def _make_df(*vehicle_ids: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "linha_lt": "1234-10",
                "veiculo_id": vid,
                "veiculo_ts": _BASE_TS,
                "extracao_ts": _BASE_TS,
                "linha_sentido": 1,
                "is_circular": False,
            }
            for vid in vehicle_ids
        ]
    )


def _always_succeeds(positions_list, start_idx, end_idx, linha_lt, veiculo_id, threshold):
    return [_FAKE_TRIP], 0, 0


def _always_fails(positions_list, start_idx, end_idx, linha_lt, veiculo_id, threshold):
    raise ValueError(f"trip_linear_distance is null, missing, or zero for non-circular trip (linha_lt={linha_lt}, vehicle={veiculo_id})")


def _fail_for(failing_vehicle_id: str):
    def _fn(positions_list, start_idx, end_idx, linha_lt, veiculo_id, threshold):
        if veiculo_id == failing_vehicle_id:
            raise ValueError(f"extraction failed for vehicle {veiculo_id}")
        return [_FAKE_TRIP], 0, 0
    return _fn


def test_all_succeed_vehicle_line_groups_failed_is_zero():
    df = _make_df("1", "2")
    _, metrics = get_all_finished_trips(_CONFIG, df, _extract_trips_fn=_always_succeeds)
    assert metrics["vehicle_line_groups_failed"] == 0


def test_all_succeed_trips_are_returned():
    df = _make_df("1", "2")
    trips, _ = get_all_finished_trips(_CONFIG, df, _extract_trips_fn=_always_succeeds)
    assert len(trips) == 2


def test_single_vehicle_failure_increments_failed_counter():
    df = _make_df("1", "2")
    _, metrics = get_all_finished_trips(_CONFIG, df, _extract_trips_fn=_fail_for("1"))
    assert metrics["vehicle_line_groups_failed"] == 1


def test_single_vehicle_failure_does_not_prevent_other_vehicles_from_being_processed():
    df = _make_df("1", "2")
    trips, metrics = get_all_finished_trips(_CONFIG, df, _extract_trips_fn=_fail_for("1"))
    assert metrics["vehicle_line_groups_processed"] == 1
    assert len(trips) == 1


def test_all_fail_no_trips_returned():
    df = _make_df("1", "2")
    trips, _ = get_all_finished_trips(_CONFIG, df, _extract_trips_fn=_always_fails)
    assert trips == []


def test_all_fail_failed_counter_equals_vehicle_count():
    df = _make_df("1", "2")
    _, metrics = get_all_finished_trips(_CONFIG, df, _extract_trips_fn=_always_fails)
    assert metrics["vehicle_line_groups_failed"] == 2


def test_multiple_failures_accumulate_in_counter():
    df = _make_df("1", "2", "3")
    call_count = {"n": 0}

    def _fail_first_two(positions_list, start_idx, end_idx, linha_lt, veiculo_id, threshold):
        call_count["n"] += 1
        if call_count["n"] <= 2:
            raise ValueError("forced failure")
        return [_FAKE_TRIP], 0, 0

    _, metrics = get_all_finished_trips(_CONFIG, df, _extract_trips_fn=_fail_first_two)
    assert metrics["vehicle_line_groups_failed"] == 2
    assert metrics["vehicle_line_groups_processed"] == 1

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

from refinedfinishedtrips.services.validate_trips_quality import (
    evaluate_low_trip_count,
    evaluate_zero_trips,
    validate_trips_quality,
)

SP_TZ = ZoneInfo("America/Sao_Paulo")
BASE_EXT_UTC = datetime(2026, 4, 27, 11, 0, 0, tzinfo=timezone.utc)
BASE_EXT_SP_NAIVE = datetime(2026, 4, 27, 8, 0, 0)  # same instant, SP local, tz-naive


def make_config(window_threshold=60, min_trips=5):
    return {
        "general": {
            "quality": {
                "trips_effective_window_threshold_minutes": window_threshold,
                "trips_min_trips_threshold": min_trips,
            }
        }
    }


def make_df(window_minutes=180):
    """DataFrame with tz-aware UTC extracao_ts spanning window_minutes."""
    end = BASE_EXT_UTC + timedelta(minutes=window_minutes)
    return pd.DataFrame({"extracao_ts": [BASE_EXT_UTC, end]})


def make_df_tz_naive(window_minutes=180):
    """DataFrame with tz-naive SP-local extracao_ts (as returned by DuckDB)."""
    end = BASE_EXT_SP_NAIVE + timedelta(minutes=window_minutes)
    return pd.DataFrame({"extracao_ts": [BASE_EXT_SP_NAIVE, end]})


# ---------------------------------------------------------------------------
# evaluate_zero_trips
# ---------------------------------------------------------------------------


def test_evaluate_zero_trips_returns_raw_metrics():
    result = evaluate_zero_trips(
        config=make_config(window_threshold=60), effective_window_minutes=180.0, trips_count=0
    )
    assert result["effective_window_minutes"] == 180.0
    assert result["window_threshold_minutes"] == 60
    assert result["trips_count"] == 0


def test_evaluate_zero_trips_returns_threshold_from_config():
    result = evaluate_zero_trips(
        config=make_config(window_threshold=90), effective_window_minutes=30.0, trips_count=5
    )
    assert result["window_threshold_minutes"] == 90
    assert result["trips_count"] == 5


def test_evaluate_zero_trips_no_status_in_result():
    result = evaluate_zero_trips(
        config=make_config(window_threshold=60), effective_window_minutes=180.0, trips_count=0
    )
    assert "status" not in result
    assert "reason" not in result


# ---------------------------------------------------------------------------
# evaluate_low_trip_count
# ---------------------------------------------------------------------------


def test_evaluate_low_trip_count_returns_raw_metrics():
    result = evaluate_low_trip_count(
        config=make_config(window_threshold=60, min_trips=5), effective_window_minutes=180.0, trips_count=2
    )
    assert result["effective_window_minutes"] == 180.0
    assert result["window_threshold_minutes"] == 60
    assert result["min_trips_threshold"] == 5
    assert result["trips_count"] == 2


def test_evaluate_low_trip_count_returns_thresholds_from_config():
    result = evaluate_low_trip_count(
        config=make_config(window_threshold=90, min_trips=10), effective_window_minutes=180.0, trips_count=5
    )
    assert result["window_threshold_minutes"] == 90
    assert result["min_trips_threshold"] == 10


def test_evaluate_low_trip_count_no_status_in_result():
    result = evaluate_low_trip_count(
        config=make_config(window_threshold=60, min_trips=5), effective_window_minutes=180.0, trips_count=2
    )
    assert "status" not in result
    assert "reason" not in result


# ---------------------------------------------------------------------------
# validate_trips_quality — overall status
# ---------------------------------------------------------------------------


def test_validate_trips_quality_overall_warn_when_zero_trips():
    df = make_df(window_minutes=180)
    result = validate_trips_quality(make_config(window_threshold=60), df, [], {})
    assert result["status"] == "WARN"


def test_validate_trips_quality_overall_warn_when_low_count():
    df = make_df(window_minutes=180)
    result = validate_trips_quality(make_config(window_threshold=60, min_trips=5), df, ["t1", "t2"], {})
    assert result["status"] == "WARN"


def test_validate_trips_quality_overall_pass_when_adequate():
    df = make_df(window_minutes=180)
    trips = [f"trip_{i}" for i in range(10)]
    result = validate_trips_quality(make_config(window_threshold=60, min_trips=5), df, trips, {})
    assert result["status"] == "PASS"


def test_validate_trips_quality_overall_pass_when_window_insufficient():
    df = make_df(window_minutes=30)
    result = validate_trips_quality(make_config(window_threshold=60), df, [], {})
    assert result["status"] == "PASS"


def test_validate_trips_quality_returns_effective_window_and_trip_count():
    df = make_df(window_minutes=180)
    trips = [f"trip_{i}" for i in range(10)]
    result = validate_trips_quality(make_config(), df, trips, {})
    assert result["effective_window_minutes"] == 180.0
    assert result["trips_extracted"] == 10


def test_validate_trips_quality_includes_extraction_metrics_when_provided():
    df = make_df(window_minutes=180)
    trips = [f"trip_{i}" for i in range(10)]
    extraction_metrics = {
        "total_source_sentido_discrepancies": 13,
        "total_input_position_sanitization_drops": 879,
        "total_input_position_records": 525603,
        "vehicle_line_groups_processed": 8577,
    }
    result = validate_trips_quality(make_config(), df, trips, extraction_metrics)
    assert result["source_sentido_discrepancies"] == 13
    assert result["sanitization_dropped_points"] == 879
    assert result["input_position_records"] == 525603
    assert result["vehicle_line_groups_processed"] == 8577


def test_validate_trips_quality_result_contains_two_checks():
    df = make_df(window_minutes=180)
    result = validate_trips_quality(make_config(), df, [], {})
    assert len(result["checks"]) == 2
    check_names = {c["check"] for c in result["checks"]}
    assert check_names == {"zero_trips", "low_trip_count"}


def test_validate_trips_quality_warn_check_contains_reason():
    df = make_df(window_minutes=180)
    result = validate_trips_quality(make_config(window_threshold=60), df, [], {})
    warn_check = next(c for c in result["checks"] if c["status"] == "WARN")
    assert "reason" in warn_check


def test_validate_trips_quality_pass_when_trips_at_exact_min_threshold():
    df = make_df(window_minutes=180)
    trips = [f"trip_{i}" for i in range(5)]
    result = validate_trips_quality(make_config(window_threshold=60, min_trips=5), df, trips, {})
    assert result["status"] == "PASS"


def test_validate_trips_quality_pass_check_has_no_reason():
    df = make_df(window_minutes=30)
    result = validate_trips_quality(make_config(window_threshold=60), df, [], {})
    for check in result["checks"]:
        assert "reason" not in check


# ---------------------------------------------------------------------------
# extracao_ts: tz-naive SP-local (DuckDB behavior)
# ---------------------------------------------------------------------------


def test_validate_trips_quality_handles_tz_naive_extracao_ts():
    df = make_df_tz_naive(window_minutes=180)
    trips = [f"trip_{i}" for i in range(10)]
    result = validate_trips_quality(make_config(window_threshold=60, min_trips=5), df, trips, {})
    assert result["status"] == "PASS"
    assert result["effective_window_minutes"] == 180.0


def test_validate_trips_quality_tz_naive_warns_on_zero_trips():
    df = make_df_tz_naive(window_minutes=180)
    result = validate_trips_quality(make_config(window_threshold=60), df, [], {})
    assert result["status"] == "WARN"


def test_validate_trips_quality_tz_naive_passes_when_window_insufficient():
    """Simulates cold-start: only 11 min of extraction data → no warning."""
    df = make_df_tz_naive(window_minutes=11)
    result = validate_trips_quality(make_config(window_threshold=60), df, [], {})
    assert result["status"] == "PASS"


def test_validate_trips_quality_tz_aware_and_tz_naive_equivalent():
    df_aware = make_df(window_minutes=120)
    df_naive = make_df_tz_naive(window_minutes=120)
    trips = [f"trip_{i}" for i in range(10)]
    result_aware = validate_trips_quality(make_config(), df_aware, trips, {})
    result_naive = validate_trips_quality(make_config(), df_naive, trips, {})
    assert result_aware["effective_window_minutes"] == result_naive["effective_window_minutes"]
    assert result_aware["status"] == result_naive["status"]

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from refinedfinishedtrips.services.validate_positions_quality import (
    evaluate_freshness,
    evaluate_recent_gaps,
    validate_positions_quality,
)

# Brazil has been UTC-3 (no DST) since 2020.
# NOW_UTC = 2026-04-27 14:00:00 UTC → SP = 2026-04-27 11:00:00 (tz-naive)
NOW_UTC = datetime(2026, 4, 27, 14, 0, 0, tzinfo=timezone.utc)
NOW_FN = lambda: NOW_UTC  # noqa: E731
SP_NOON = datetime(2026, 4, 27, 11, 0, 0)  # tz-naive SP equivalent of NOW_UTC


def make_config(
    freshness_warn=10,
    freshness_fail=30,
    gaps_warn=5,
    gaps_fail=15,
    gaps_window=60,
):
    return {
        "general": {
            "quality": {
                "freshness_warn_staleness_minutes": freshness_warn,
                "freshness_fail_staleness_minutes": freshness_fail,
                "gaps_warn_gap_minutes": gaps_warn,
                "gaps_fail_gap_minutes": gaps_fail,
                "gaps_recent_window_minutes": gaps_window,
            }
        }
    }


def make_df(veiculo_ts_values, extracao_ts_values=None):
    """Build a minimal positions DataFrame."""
    n = len(veiculo_ts_values)
    if extracao_ts_values is None:
        extracao_ts_values = [NOW_UTC - timedelta(minutes=3)] * n
    return pd.DataFrame(
        {
            "veiculo_ts": veiculo_ts_values,
            "extracao_ts": extracao_ts_values,
        }
    )


# ---------------------------------------------------------------------------
# evaluate_freshness
# ---------------------------------------------------------------------------


def test_evaluate_freshness_below_warn_threshold():
    # lag = 5 min, warn=10 → below warn threshold
    df = make_df([SP_NOON - timedelta(minutes=5)])
    result = evaluate_freshness(make_config(), df, now_fn=NOW_FN)
    assert abs(result["observed_lag_minutes"] - 5.0) < 0.1
    assert result["observed_lag_minutes"] < result["warn_threshold_minutes"]


def test_evaluate_freshness_between_warn_and_fail_thresholds():
    # lag = 15 min, warn=10, fail=30
    df = make_df([SP_NOON - timedelta(minutes=15)])
    result = evaluate_freshness(make_config(), df, now_fn=NOW_FN)
    assert abs(result["observed_lag_minutes"] - 15.0) < 0.1
    assert result["warn_threshold_minutes"] < result["observed_lag_minutes"] < result["fail_threshold_minutes"]


def test_evaluate_freshness_above_fail_threshold():
    # lag = 35 min, fail=30
    df = make_df([SP_NOON - timedelta(minutes=35)])
    result = evaluate_freshness(make_config(), df, now_fn=NOW_FN)
    assert abs(result["observed_lag_minutes"] - 35.0) < 0.1
    assert result["observed_lag_minutes"] > result["fail_threshold_minutes"]


def test_evaluate_freshness_empty_df_returns_none_lag():
    result = evaluate_freshness(make_config(), pd.DataFrame(), now_fn=NOW_FN)
    assert result["observed_lag_minutes"] is None


def test_evaluate_freshness_result_contains_thresholds():
    df = make_df([SP_NOON - timedelta(minutes=5)])
    result = evaluate_freshness(make_config(freshness_warn=10, freshness_fail=30), df, now_fn=NOW_FN)
    assert result["warn_threshold_minutes"] == 10
    assert result["fail_threshold_minutes"] == 30


def test_evaluate_freshness_zero_lag():
    # SP_NOON is the naive equivalent of NOW_UTC in SP → lag ≈ 0
    df = make_df([SP_NOON])
    result = evaluate_freshness(make_config(), df, now_fn=NOW_FN)
    assert result["observed_lag_minutes"] < 0.1


# ---------------------------------------------------------------------------
# evaluate_freshness — tz-aware veiculo_ts (real parquet data scenario)
# The SPTrans API returns SP local time with UTC offset; pd.to_datetime
# preserves that, so veiculo_ts in parquet is tz-aware.
# ---------------------------------------------------------------------------

SP_TZ = ZoneInfo("America/Sao_Paulo")

# SP_NOON expressed as tz-aware (SP offset -03:00) — same instant as SP_NOON naive
SP_NOON_AWARE = datetime(2026, 4, 27, 11, 0, 0, tzinfo=SP_TZ)


def make_df_aware(veiculo_ts_values, extracao_ts_values=None):
    """Build a positions DataFrame with tz-aware veiculo_ts (real parquet format)."""
    n = len(veiculo_ts_values)
    if extracao_ts_values is None:
        extracao_ts_values = [NOW_UTC - timedelta(minutes=3)] * n
    return pd.DataFrame(
        {
            "veiculo_ts": pd.to_datetime(veiculo_ts_values, utc=False),
            "extracao_ts": extracao_ts_values,
        }
    )


def test_evaluate_freshness_tz_aware_below_warn_threshold():
    df = make_df_aware([SP_NOON_AWARE - timedelta(minutes=5)])
    result = evaluate_freshness(make_config(), df, now_fn=NOW_FN)
    assert abs(result["observed_lag_minutes"] - 5.0) < 0.1
    assert result["observed_lag_minutes"] < result["warn_threshold_minutes"]


def test_evaluate_freshness_tz_aware_between_thresholds():
    df = make_df_aware([SP_NOON_AWARE - timedelta(minutes=15)])
    result = evaluate_freshness(make_config(), df, now_fn=NOW_FN)
    assert result["warn_threshold_minutes"] < result["observed_lag_minutes"] < result["fail_threshold_minutes"]


def test_evaluate_freshness_tz_aware_above_fail_threshold():
    df = make_df_aware([SP_NOON_AWARE - timedelta(minutes=35)])
    result = evaluate_freshness(make_config(), df, now_fn=NOW_FN)
    assert result["observed_lag_minutes"] > result["fail_threshold_minutes"]


def test_evaluate_freshness_tz_aware_matches_tz_naive_lag():
    # Same instant expressed tz-aware vs tz-naive must produce the same lag
    df_naive = make_df([SP_NOON - timedelta(minutes=12)])
    df_aware = make_df_aware([SP_NOON_AWARE - timedelta(minutes=12)])
    result_naive = evaluate_freshness(make_config(), df_naive, now_fn=NOW_FN)
    result_aware = evaluate_freshness(make_config(), df_aware, now_fn=NOW_FN)
    assert abs(result_naive["observed_lag_minutes"] - result_aware["observed_lag_minutes"]) < 0.01


# ---------------------------------------------------------------------------
# evaluate_recent_gaps
# ---------------------------------------------------------------------------

# Cutoff = NOW_UTC - 60min = 2026-04-27 13:00:00 UTC


def _ext_ts(*offsets_from_cutoff_minutes):
    """Return UTC-aware extracao_ts values at given offsets past cutoff."""
    cutoff = NOW_UTC - timedelta(minutes=60)
    return [cutoff + timedelta(minutes=m) for m in offsets_from_cutoff_minutes]


def test_evaluate_recent_gaps_returns_max_gap():
    # Gaps of 3 min each
    ext_ts = _ext_ts(0, 3, 6, 9)
    df = make_df([SP_NOON] * len(ext_ts), extracao_ts_values=ext_ts)
    result = evaluate_recent_gaps(make_config(), df, now_fn=NOW_FN)
    assert result["max_gap_minutes"] == pytest.approx(3.0, abs=0.1)
    assert result["max_gap_minutes"] < result["warn_threshold_minutes"]


def test_evaluate_recent_gaps_returns_gap_above_warn_threshold():
    # Gap of 7 min, warn=5, fail=15
    ext_ts = _ext_ts(0, 7, 14)
    df = make_df([SP_NOON] * len(ext_ts), extracao_ts_values=ext_ts)
    result = evaluate_recent_gaps(make_config(), df, now_fn=NOW_FN)
    assert result["max_gap_minutes"] == pytest.approx(7.0, abs=0.1)
    assert result["warn_threshold_minutes"] < result["max_gap_minutes"] < result["fail_threshold_minutes"]


def test_evaluate_recent_gaps_returns_gap_above_fail_threshold():
    # Gap of 20 min, fail=15
    ext_ts = _ext_ts(0, 20)
    df = make_df([SP_NOON] * len(ext_ts), extracao_ts_values=ext_ts)
    result = evaluate_recent_gaps(make_config(), df, now_fn=NOW_FN)
    assert result["max_gap_minutes"] == pytest.approx(20.0, abs=0.1)
    assert result["max_gap_minutes"] > result["fail_threshold_minutes"]


def test_evaluate_recent_gaps_empty_df_returns_none_max_gap():
    result = evaluate_recent_gaps(make_config(), pd.DataFrame(), now_fn=NOW_FN)
    assert result["max_gap_minutes"] is None
    assert result["distinct_extracao_ts_in_window"] == 0


def test_evaluate_recent_gaps_fewer_than_2_distinct_timestamps_returns_none_max_gap():
    # Only one distinct extracao_ts in the recent window
    ext_ts = _ext_ts(30)  # one timestamp inside the 60-min window
    df = make_df([SP_NOON, SP_NOON - timedelta(minutes=1)], extracao_ts_values=ext_ts * 2)
    result = evaluate_recent_gaps(make_config(), df, now_fn=NOW_FN)
    assert result["max_gap_minutes"] is None
    assert result["distinct_extracao_ts_in_window"] == 1


def test_evaluate_recent_gaps_timestamps_outside_window_excluded():
    # Only timestamps before cutoff → fewer than 2 in window → None max_gap
    before_cutoff = [NOW_UTC - timedelta(minutes=90), NOW_UTC - timedelta(minutes=75)]
    df = make_df([SP_NOON, SP_NOON], extracao_ts_values=before_cutoff)
    result = evaluate_recent_gaps(make_config(), df, now_fn=NOW_FN)
    assert result["max_gap_minutes"] is None


def test_evaluate_recent_gaps_result_contains_window():
    ext_ts = _ext_ts(0, 3)
    df = make_df([SP_NOON] * 2, extracao_ts_values=ext_ts)
    result = evaluate_recent_gaps(make_config(gaps_window=60), df, now_fn=NOW_FN)
    assert result["recent_window_minutes"] == 60


# ---------------------------------------------------------------------------
# evaluate_recent_gaps — tz-naive extracao_ts (real parquet data scenario)
# DuckDB strips tz when returning tz-aware SP timestamps; values are SP-local.
# SP_NOON = 2026-04-27 11:00:00 (naive SP equivalent of NOW_UTC).
# SP cutoff (60 min before SP_NOON) = 2026-04-27 10:00:00.
# ---------------------------------------------------------------------------


def _ext_ts_naive(*offsets_from_cutoff_minutes):
    """Return tz-naive SP extracao_ts values at given offsets past the SP cutoff."""
    sp_cutoff = SP_NOON - timedelta(minutes=60)
    return [sp_cutoff + timedelta(minutes=m) for m in offsets_from_cutoff_minutes]


def test_evaluate_recent_gaps_tz_naive_returns_max_gap():
    ext_ts = _ext_ts_naive(0, 3, 6, 9)
    df = make_df([SP_NOON] * len(ext_ts), extracao_ts_values=ext_ts)
    result = evaluate_recent_gaps(make_config(), df, now_fn=NOW_FN)
    assert result["max_gap_minutes"] == pytest.approx(3.0, abs=0.1)


def test_evaluate_recent_gaps_tz_naive_between_thresholds():
    ext_ts = _ext_ts_naive(0, 7, 14)
    df = make_df([SP_NOON] * len(ext_ts), extracao_ts_values=ext_ts)
    result = evaluate_recent_gaps(make_config(), df, now_fn=NOW_FN)
    assert result["warn_threshold_minutes"] < result["max_gap_minutes"] < result["fail_threshold_minutes"]


def test_evaluate_recent_gaps_tz_naive_above_fail_threshold():
    ext_ts = _ext_ts_naive(0, 20)
    df = make_df([SP_NOON] * len(ext_ts), extracao_ts_values=ext_ts)
    result = evaluate_recent_gaps(make_config(), df, now_fn=NOW_FN)
    assert result["max_gap_minutes"] > result["fail_threshold_minutes"]


def test_evaluate_recent_gaps_tz_naive_outside_window_excluded():
    before_cutoff = [SP_NOON - timedelta(minutes=90), SP_NOON - timedelta(minutes=75)]
    df = make_df([SP_NOON, SP_NOON], extracao_ts_values=before_cutoff)
    result = evaluate_recent_gaps(make_config(), df, now_fn=NOW_FN)
    assert result["max_gap_minutes"] is None


def test_evaluate_recent_gaps_tz_naive_matches_tz_aware_max_gap():
    ext_aware = _ext_ts(0, 7)
    ext_naive = _ext_ts_naive(0, 7)
    df_aware = make_df([SP_NOON] * 2, extracao_ts_values=ext_aware)
    df_naive = make_df([SP_NOON] * 2, extracao_ts_values=ext_naive)
    result_aware = evaluate_recent_gaps(make_config(), df_aware, now_fn=NOW_FN)
    result_naive = evaluate_recent_gaps(make_config(), df_naive, now_fn=NOW_FN)
    assert abs(result_aware["max_gap_minutes"] - result_naive["max_gap_minutes"]) < 0.01


# ---------------------------------------------------------------------------
# validate_positions_quality — judgement layer
# ---------------------------------------------------------------------------


def test_validate_positions_quality_pass_when_both_checks_pass():
    ext_ts = _ext_ts(27, 30)
    df = make_df([SP_NOON - timedelta(minutes=5)] * 2, extracao_ts_values=ext_ts)
    result = validate_positions_quality(make_config(), df, now_fn=NOW_FN)
    assert result["status"] == "PASS"
    assert result["positions_in_time_window_count"] == 2
    assert len(result["checks"]) == 2


def test_validate_positions_quality_warn_when_freshness_warns():
    # Freshness lag=15 (WARN), gaps fine
    ext_ts = _ext_ts(27, 30)
    df = make_df([SP_NOON - timedelta(minutes=15)] * 2, extracao_ts_values=ext_ts)
    result = validate_positions_quality(make_config(), df, now_fn=NOW_FN)
    assert result["status"] == "WARN"


def test_validate_positions_quality_fail_when_freshness_fails():
    ext_ts = _ext_ts(27, 30)
    df = make_df([SP_NOON - timedelta(minutes=35)] * 2, extracao_ts_values=ext_ts)
    result = validate_positions_quality(make_config(), df, now_fn=NOW_FN)
    assert result["status"] == "FAIL"


def test_validate_positions_quality_fail_beats_warn():
    # Freshness FAIL, gaps WARN → overall FAIL
    ext_ts = _ext_ts(0, 7)  # gap=7 → WARN
    df = make_df(
        [SP_NOON - timedelta(minutes=35)] * 2,  # lag=35 → FAIL
        extracao_ts_values=ext_ts,
    )
    result = validate_positions_quality(make_config(), df, now_fn=NOW_FN)
    assert result["status"] == "FAIL"

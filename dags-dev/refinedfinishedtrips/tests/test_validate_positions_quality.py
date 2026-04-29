from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from refinedfinishedtrips.services.validate_positions_quality import (
    check_freshness,
    check_recent_gaps,
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
# check_freshness
# ---------------------------------------------------------------------------


def test_freshness_pass():
    # lag = 5 min (SP_NOON - 5min = 10:55), warn=10 → PASS
    df = make_df([SP_NOON - timedelta(minutes=5)])
    result = check_freshness(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "PASS"
    assert abs(result["observed_lag_minutes"] - 5.0) < 0.1


def test_freshness_warn():
    # lag = 15 min, warn=10, fail=30 → WARN
    df = make_df([SP_NOON - timedelta(minutes=15)])
    result = check_freshness(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "WARN"
    assert abs(result["observed_lag_minutes"] - 15.0) < 0.1


def test_freshness_fail():
    # lag = 35 min, fail=30 → FAIL
    df = make_df([SP_NOON - timedelta(minutes=35)])
    result = check_freshness(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "FAIL"
    assert abs(result["observed_lag_minutes"] - 35.0) < 0.1


def test_freshness_empty_df_returns_fail_with_note():
    result = check_freshness(pd.DataFrame(), make_config(), now_fn=NOW_FN)
    assert result["status"] == "FAIL"
    assert result["observed_lag_minutes"] is None
    assert "no positions" in result["note"].lower()


def test_freshness_result_contains_thresholds():
    df = make_df([SP_NOON - timedelta(minutes=5)])
    result = check_freshness(df, make_config(freshness_warn=10, freshness_fail=30), now_fn=NOW_FN)
    assert result["warn_threshold_minutes"] == 10
    assert result["fail_threshold_minutes"] == 30


def test_freshness_timezone_normalization():
    # Verify tz-naive veiculo_ts (SP values) is correctly compared against
    # UTC now_fn that gets converted to SP before the subtraction.
    # SP_NOON is the naive equivalent of NOW_UTC in SP.
    # lag = 0 minutes → PASS
    df = make_df([SP_NOON])
    result = check_freshness(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "PASS"
    assert result["observed_lag_minutes"] < 0.1


# ---------------------------------------------------------------------------
# check_freshness — tz-aware veiculo_ts (real parquet data scenario)
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


def test_freshness_pass_with_tz_aware_veiculo_ts():
    # lag = 5 min, warn=10 → PASS
    df = make_df_aware([SP_NOON_AWARE - timedelta(minutes=5)])
    result = check_freshness(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "PASS"
    assert abs(result["observed_lag_minutes"] - 5.0) < 0.1


def test_freshness_warn_with_tz_aware_veiculo_ts():
    # lag = 15 min, warn=10, fail=30 → WARN
    df = make_df_aware([SP_NOON_AWARE - timedelta(minutes=15)])
    result = check_freshness(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "WARN"


def test_freshness_fail_with_tz_aware_veiculo_ts():
    # lag = 35 min, fail=30 → FAIL
    df = make_df_aware([SP_NOON_AWARE - timedelta(minutes=35)])
    result = check_freshness(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "FAIL"


def test_freshness_tz_aware_equivalent_to_tz_naive():
    # Same instant expressed tz-aware vs tz-naive must produce the same lag
    df_naive = make_df([SP_NOON - timedelta(minutes=12)])
    df_aware = make_df_aware([SP_NOON_AWARE - timedelta(minutes=12)])
    result_naive = check_freshness(df_naive, make_config(), now_fn=NOW_FN)
    result_aware = check_freshness(df_aware, make_config(), now_fn=NOW_FN)
    assert result_naive["status"] == result_aware["status"]
    assert abs(result_naive["observed_lag_minutes"] - result_aware["observed_lag_minutes"]) < 0.01


# ---------------------------------------------------------------------------
# check_recent_gaps
# ---------------------------------------------------------------------------

# Cutoff = NOW_UTC - 60min = 2026-04-27 13:00:00 UTC


def _ext_ts(*offsets_from_cutoff_minutes):
    """Return UTC-aware extracao_ts values at given offsets past cutoff."""
    cutoff = NOW_UTC - timedelta(minutes=60)
    return [cutoff + timedelta(minutes=m) for m in offsets_from_cutoff_minutes]


def test_recent_gaps_pass():
    # Gaps of 3 min each, warn=5 → PASS
    ext_ts = _ext_ts(0, 3, 6, 9)
    df = make_df([SP_NOON] * len(ext_ts), extracao_ts_values=ext_ts)
    result = check_recent_gaps(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "PASS"
    assert result["max_gap_minutes"] == pytest.approx(3.0, abs=0.1)


def test_recent_gaps_warn():
    # Gap of 7 min, warn=5, fail=15 → WARN
    ext_ts = _ext_ts(0, 7, 14)
    df = make_df([SP_NOON] * len(ext_ts), extracao_ts_values=ext_ts)
    result = check_recent_gaps(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "WARN"
    assert result["max_gap_minutes"] == pytest.approx(7.0, abs=0.1)


def test_recent_gaps_fail():
    # Gap of 20 min, fail=15 → FAIL
    ext_ts = _ext_ts(0, 20)
    df = make_df([SP_NOON] * len(ext_ts), extracao_ts_values=ext_ts)
    result = check_recent_gaps(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "FAIL"
    assert result["max_gap_minutes"] == pytest.approx(20.0, abs=0.1)


def test_recent_gaps_empty_df_returns_fail_with_note():
    result = check_recent_gaps(pd.DataFrame(), make_config(), now_fn=NOW_FN)
    assert result["status"] == "FAIL"
    assert result["max_gap_minutes"] is None
    assert "no positions" in result["note"].lower()


def test_recent_gaps_fewer_than_2_distinct_timestamps_returns_fail_with_note():
    # Only one distinct extracao_ts in the recent window
    ext_ts = _ext_ts(30)  # one timestamp inside the 60-min window
    df = make_df([SP_NOON, SP_NOON - timedelta(minutes=1)], extracao_ts_values=ext_ts * 2)
    result = check_recent_gaps(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "FAIL"
    assert result["max_gap_minutes"] is None
    assert "fewer than 2" in result["note"].lower()


def test_recent_gaps_timestamps_outside_window_excluded():
    # Only timestamps before cutoff → fewer than 2 in window → FAIL
    before_cutoff = [NOW_UTC - timedelta(minutes=90), NOW_UTC - timedelta(minutes=75)]
    df = make_df([SP_NOON, SP_NOON], extracao_ts_values=before_cutoff)
    result = check_recent_gaps(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "FAIL"


def test_recent_gaps_result_contains_window():
    ext_ts = _ext_ts(0, 3)
    df = make_df([SP_NOON] * 2, extracao_ts_values=ext_ts)
    result = check_recent_gaps(df, make_config(gaps_window=60), now_fn=NOW_FN)
    assert result["recent_window_minutes"] == 60


# ---------------------------------------------------------------------------
# check_recent_gaps — tz-naive extracao_ts (real parquet data scenario)
# DuckDB strips tz when returning tz-aware SP timestamps; values are SP-local.
# SP_NOON = 2026-04-27 11:00:00 (naive SP equivalent of NOW_UTC).
# SP cutoff (60 min before SP_NOON) = 2026-04-27 10:00:00.
# ---------------------------------------------------------------------------


def _ext_ts_naive(*offsets_from_cutoff_minutes):
    """Return tz-naive SP extracao_ts values at given offsets past the SP cutoff."""
    sp_cutoff = SP_NOON - timedelta(minutes=60)
    return [sp_cutoff + timedelta(minutes=m) for m in offsets_from_cutoff_minutes]


def test_recent_gaps_pass_with_tz_naive_extracao_ts():
    ext_ts = _ext_ts_naive(0, 3, 6, 9)
    df = make_df([SP_NOON] * len(ext_ts), extracao_ts_values=ext_ts)
    result = check_recent_gaps(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "PASS"
    assert result["max_gap_minutes"] == pytest.approx(3.0, abs=0.1)


def test_recent_gaps_warn_with_tz_naive_extracao_ts():
    ext_ts = _ext_ts_naive(0, 7, 14)
    df = make_df([SP_NOON] * len(ext_ts), extracao_ts_values=ext_ts)
    result = check_recent_gaps(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "WARN"


def test_recent_gaps_fail_with_tz_naive_extracao_ts():
    ext_ts = _ext_ts_naive(0, 20)
    df = make_df([SP_NOON] * len(ext_ts), extracao_ts_values=ext_ts)
    result = check_recent_gaps(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "FAIL"


def test_recent_gaps_tz_naive_outside_window_excluded():
    # SP values from 90 and 75 min before SP_NOON → before the 60-min cutoff → FAIL
    before_cutoff = [SP_NOON - timedelta(minutes=90), SP_NOON - timedelta(minutes=75)]
    df = make_df([SP_NOON, SP_NOON], extracao_ts_values=before_cutoff)
    result = check_recent_gaps(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "FAIL"


def test_recent_gaps_tz_naive_equivalent_to_tz_aware():
    # Same gap expressed tz-naive (SP) vs tz-aware (UTC) must produce identical status
    ext_aware = _ext_ts(0, 7)       # UTC-aware, gap=7 → WARN
    ext_naive = _ext_ts_naive(0, 7)  # SP-naive, same gap → WARN
    df_aware = make_df([SP_NOON] * 2, extracao_ts_values=ext_aware)
    df_naive = make_df([SP_NOON] * 2, extracao_ts_values=ext_naive)
    result_aware = check_recent_gaps(df_aware, make_config(), now_fn=NOW_FN)
    result_naive = check_recent_gaps(df_naive, make_config(), now_fn=NOW_FN)
    assert result_aware["status"] == result_naive["status"]
    assert abs(result_aware["max_gap_minutes"] - result_naive["max_gap_minutes"]) < 0.01


# ---------------------------------------------------------------------------
# validate_positions_quality — overall status derivation
# ---------------------------------------------------------------------------


def test_overall_pass_when_both_checks_pass():
    df = make_df(
        [SP_NOON - timedelta(minutes=5)],
        extracao_ts_values=_ext_ts(30),
    )
    # Add a second distinct extracao_ts so gaps check has >= 2 values
    ext_ts = _ext_ts(27, 30)
    df = make_df([SP_NOON - timedelta(minutes=5)] * 2, extracao_ts_values=ext_ts)
    result = validate_positions_quality(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "PASS"
    assert result["positions_in_time_window_count"] == 2
    assert len(result["checks"]) == 2


def test_overall_warn_when_one_check_warns():
    # Freshness lag=15 (WARN), gaps fine
    ext_ts = _ext_ts(27, 30)
    df = make_df([SP_NOON - timedelta(minutes=15)] * 2, extracao_ts_values=ext_ts)
    result = validate_positions_quality(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "WARN"


def test_overall_fail_when_one_check_fails():
    # Freshness lag=35 (FAIL)
    ext_ts = _ext_ts(27, 30)
    df = make_df([SP_NOON - timedelta(minutes=35)] * 2, extracao_ts_values=ext_ts)
    result = validate_positions_quality(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "FAIL"


def test_overall_fail_beats_warn():
    # Freshness FAIL, gaps WARN → overall FAIL
    ext_ts = _ext_ts(0, 7)  # gap=7 → WARN
    df = make_df(
        [SP_NOON - timedelta(minutes=35)] * 2,  # lag=35 → FAIL
        extracao_ts_values=ext_ts,
    )
    result = validate_positions_quality(df, make_config(), now_fn=NOW_FN)
    assert result["status"] == "FAIL"

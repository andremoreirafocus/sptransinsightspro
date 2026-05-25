from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import logging
import pandas as pd
import pytest

from updatelatestpositions.services.create_latest_positions import _evaluate_freshness

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
NOW_SP_NAIVE = NOW.astimezone(ZoneInfo("America/Sao_Paulo")).replace(tzinfo=None)


def make_df(lag_minutes: float, tz_aware: bool = False) -> pd.DataFrame:
    if tz_aware:
        ts = NOW - timedelta(minutes=lag_minutes)
    else:
        ts = NOW_SP_NAIVE - timedelta(minutes=lag_minutes)
    return pd.DataFrame({"veiculo_ts": [ts]})


def make_config(warn: int = 10, fail: int = 30) -> dict:
    return {
        "general": {
            "quality": {
                "freshness_warn_staleness_minutes": warn,
                "freshness_fail_staleness_minutes": fail,
            }
        }
    }


def test_freshness_below_warn_threshold():
    result = _evaluate_freshness(make_config(), make_df(5), now_fn=lambda: NOW)
    assert abs(result["observed_lag_minutes"] - 5.0) < 0.1
    assert result["observed_lag_minutes"] < result["warn_threshold_minutes"]


def test_freshness_between_warn_and_fail():
    result = _evaluate_freshness(make_config(), make_df(15), now_fn=lambda: NOW)
    assert result["observed_lag_minutes"] > result["warn_threshold_minutes"]
    assert result["observed_lag_minutes"] < result["fail_threshold_minutes"]


def test_freshness_above_fail_threshold():
    result = _evaluate_freshness(make_config(), make_df(35), now_fn=lambda: NOW)
    assert result["observed_lag_minutes"] > result["fail_threshold_minutes"]


def test_freshness_empty_df_returns_none_lag():
    result = _evaluate_freshness(
        make_config(),
        pd.DataFrame({"veiculo_ts": []}),
        now_fn=lambda: NOW,
    )
    assert result["observed_lag_minutes"] is None


def test_freshness_result_contains_thresholds():
    result = _evaluate_freshness(make_config(warn=10, fail=30), make_df(5), now_fn=lambda: NOW)
    assert result["warn_threshold_minutes"] == 10
    assert result["fail_threshold_minutes"] == 30


def test_freshness_tz_aware_matches_tz_naive():
    naive = _evaluate_freshness(make_config(), make_df(10, tz_aware=False), now_fn=lambda: NOW)
    aware = _evaluate_freshness(make_config(), make_df(10, tz_aware=True), now_fn=lambda: NOW)
    assert abs(naive["observed_lag_minutes"] - aware["observed_lag_minutes"]) < 0.01


def test_freshness_emits_info_event_below_warn(caplog):
    with caplog.at_level(logging.INFO):
        _evaluate_freshness(make_config(), make_df(5), now_fn=lambda: NOW)
    assert "freshness_evaluation" in caplog.text


def test_freshness_empty_emits_warning_event(caplog):
    with caplog.at_level(logging.WARNING):
        _evaluate_freshness(
            make_config(),
            pd.DataFrame({"veiculo_ts": []}),
            now_fn=lambda: NOW,
        )
    assert "freshness_evaluation" in caplog.text

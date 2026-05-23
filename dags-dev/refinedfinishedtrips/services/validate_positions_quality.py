from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def evaluate_freshness(config: Dict[str, Any], df: pd.DataFrame, now_fn: Optional[Callable[[], datetime]] = None) -> Dict[str, Any]:
    def get_config(config):
        quality = config["general"]["quality"]
        return (
            quality["freshness_warn_staleness_minutes"],
            quality["freshness_fail_staleness_minutes"],
        )

    warn_threshold, fail_threshold = get_config(config)

    if df.empty:
        result = {
            "observed_lag_minutes": None,
            "warn_threshold_minutes": warn_threshold,
            "fail_threshold_minutes": fail_threshold,
        }
        structured_logger.warning(event="freshness_evaluation", message="Freshness evaluation", metadata=result)
        return result

    now_utc = (now_fn or _now_utc)()
    now_sp_naive = now_utc.astimezone(ZoneInfo("America/Sao_Paulo")).replace(tzinfo=None)
    latest_ts = df["veiculo_ts"].max()
    # Normalize to SP-naive regardless of whether the parquet column is tz-aware
    if latest_ts.tzinfo is not None:
        latest_ts = latest_ts.tz_convert(ZoneInfo("America/Sao_Paulo")).replace(tzinfo=None)
    lag_minutes = (now_sp_naive - latest_ts).total_seconds() / 60

    result = {
        "observed_lag_minutes": round(lag_minutes, 2),
        "warn_threshold_minutes": warn_threshold,
        "fail_threshold_minutes": fail_threshold,
    }
    structured_logger.info(event="freshness_evaluation", message="Freshness evaluation", metadata=result)
    return result


def evaluate_recent_gaps(config: Dict[str, Any], df: pd.DataFrame, now_fn: Optional[Callable[[], datetime]] = None) -> Dict[str, Any]:
    def get_config(config):
        quality = config["general"]["quality"]
        return (
            quality["gaps_warn_gap_minutes"],
            quality["gaps_fail_gap_minutes"],
            quality["gaps_recent_window_minutes"],
        )

    warn_threshold, fail_threshold, recent_window = get_config(config)

    if df.empty:
        result = {
            "max_gap_minutes": None,
            "distinct_extracao_ts_in_window": 0,
            "warn_threshold_minutes": warn_threshold,
            "fail_threshold_minutes": fail_threshold,
            "recent_window_minutes": recent_window,
        }
        structured_logger.warning(event="recent_gaps_evaluation", message="Recent gaps evaluation", metadata=result)
        return result

    now_utc = (now_fn or _now_utc)()
    # DuckDB strips tz when returning tz-aware timestamps into pandas, yielding
    # datetime64[us] (tz-naive) with SP-local values. Match the cutoff accordingly.
    if isinstance(df["extracao_ts"].dtype, pd.DatetimeTZDtype):
        cutoff = now_utc - timedelta(minutes=recent_window)
    else:
        now_sp_naive = now_utc.astimezone(ZoneInfo("America/Sao_Paulo")).replace(tzinfo=None)
        cutoff = now_sp_naive - timedelta(minutes=recent_window)
    recent_ts = (
        df[df["extracao_ts"] >= cutoff]["extracao_ts"]
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )
    recent_ts = pd.to_datetime(recent_ts)
    distinct_count = len(recent_ts)

    if distinct_count < 2:
        result = {
            "max_gap_minutes": None,
            "distinct_extracao_ts_in_window": distinct_count,
            "warn_threshold_minutes": warn_threshold,
            "fail_threshold_minutes": fail_threshold,
            "recent_window_minutes": recent_window,
        }
        structured_logger.warning(event="recent_gaps_evaluation", message="Recent gaps evaluation", metadata=result)
        return result

    diffs = recent_ts.diff().dropna().dt.total_seconds() / 60
    max_gap = round(float(diffs.max()), 2)

    result = {
        "max_gap_minutes": max_gap,
        "distinct_extracao_ts_in_window": distinct_count,
        "warn_threshold_minutes": warn_threshold,
        "fail_threshold_minutes": fail_threshold,
        "recent_window_minutes": recent_window,
    }
    structured_logger.info(event="recent_gaps_evaluation", message="Recent gaps evaluation", metadata=result)
    return result


def validate_positions_quality(config: Dict[str, Any], df: pd.DataFrame, now_fn: Optional[Callable[[], datetime]] = None) -> Dict[str, Any]:
    freshness = evaluate_freshness(config, df, now_fn)
    gaps = evaluate_recent_gaps(config, df, now_fn)

    checks = []

    lag = freshness["observed_lag_minutes"]
    if lag is None:
        checks.append({"check": "freshness", "status": "FAIL", "reason": "no positions available for the analysis time window", **freshness})
    elif lag > freshness["fail_threshold_minutes"]:
        checks.append({"check": "freshness", "status": "FAIL", **freshness})
    elif lag > freshness["warn_threshold_minutes"]:
        checks.append({"check": "freshness", "status": "WARN", **freshness})
    else:
        checks.append({"check": "freshness", "status": "PASS", **freshness})

    max_gap = gaps["max_gap_minutes"]
    distinct = gaps["distinct_extracao_ts_in_window"]
    if max_gap is None:
        reason = (
            "no positions available for the analysis time window"
            if distinct == 0
            else f"fewer than 2 distinct extraction timestamps in recent window (found: {distinct})"
        )
        checks.append({"check": "recent_gaps", "status": "FAIL", "reason": reason, **gaps})
    elif max_gap > gaps["fail_threshold_minutes"]:
        checks.append({"check": "recent_gaps", "status": "FAIL", **gaps})
    elif max_gap > gaps["warn_threshold_minutes"]:
        checks.append({"check": "recent_gaps", "status": "WARN", **gaps})
    else:
        checks.append({"check": "recent_gaps", "status": "PASS", **gaps})

    statuses = [c["status"] for c in checks]
    if "FAIL" in statuses:
        overall = "FAIL"
    elif "WARN" in statuses:
        overall = "WARN"
    else:
        overall = "PASS"

    result = {
        "status": overall,
        "positions_in_time_window_count": len(df),
        "checks": checks,
    }
    structured_logger.info(event="positions_quality_validated", message="Positions quality validation", metadata=result)
    return result

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def _now_utc():
    return datetime.now(timezone.utc)


def check_freshness(df, config, now_fn=None):
    def get_config(config):
        quality = config["general"]["quality"]
        return (
            quality["freshness_warn_staleness_minutes"],
            quality["freshness_fail_staleness_minutes"],
        )

    warn_threshold, fail_threshold = get_config(config)

    if df.empty:
        logger.warning("Freshness check: no positions available for the analysis time window.")
        return {
            "check": "freshness",
            "status": "FAIL",
            "observed_lag_minutes": None,
            "warn_threshold_minutes": warn_threshold,
            "fail_threshold_minutes": fail_threshold,
            "note": "no positions available for the analysis time window",
        }

    now_utc = (now_fn or _now_utc)()
    now_sp_naive = now_utc.astimezone(ZoneInfo("America/Sao_Paulo")).replace(tzinfo=None)
    latest_ts = df["veiculo_ts"].max()
    # Normalize to SP-naive regardless of whether the parquet column is tz-aware
    if latest_ts.tzinfo is not None:
        latest_ts = latest_ts.tz_convert(ZoneInfo("America/Sao_Paulo")).replace(tzinfo=None)
    lag_minutes = (now_sp_naive - latest_ts).total_seconds() / 60

    if lag_minutes > fail_threshold:
        status = "FAIL"
    elif lag_minutes > warn_threshold:
        status = "WARN"
    else:
        status = "PASS"

    logger.info(f"Freshness check: lag={lag_minutes:.2f} min (warn={warn_threshold}, fail={fail_threshold}) → {status}.")
    return {
        "check": "freshness",
        "status": status,
        "observed_lag_minutes": round(lag_minutes, 2),
        "warn_threshold_minutes": warn_threshold,
        "fail_threshold_minutes": fail_threshold,
    }


def check_recent_gaps(df, config, now_fn=None):
    def get_config(config):
        quality = config["general"]["quality"]
        return (
            quality["gaps_warn_gap_minutes"],
            quality["gaps_fail_gap_minutes"],
            quality["gaps_recent_window_minutes"],
        )

    warn_threshold, fail_threshold, recent_window = get_config(config)

    if df.empty:
        logger.warning("Recent gaps check: no positions available for the analysis time window.")
        return {
            "check": "recent_gaps",
            "status": "FAIL",
            "max_gap_minutes": None,
            "warn_threshold_minutes": warn_threshold,
            "fail_threshold_minutes": fail_threshold,
            "recent_window_minutes": recent_window,
            "note": "no positions available for the analysis time window",
        }

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
    distinct_count = len(recent_ts)
    logger.info(f"Recent gaps check: {distinct_count} distinct extraction timestamps in last {recent_window} min window.")

    if distinct_count < 2:
        logger.warning(f"Recent gaps check: insufficient extraction timestamps (found {distinct_count}, need ≥2).")
        return {
            "check": "recent_gaps",
            "status": "FAIL",
            "max_gap_minutes": None,
            "distinct_extracao_ts_in_window": distinct_count,
            "warn_threshold_minutes": warn_threshold,
            "fail_threshold_minutes": fail_threshold,
            "recent_window_minutes": recent_window,
            "note": f"fewer than 2 distinct extraction timestamps in recent window (found: {distinct_count})",
        }

    diffs = recent_ts.diff().dropna().dt.total_seconds() / 60
    max_gap = round(float(diffs.max()), 2)

    if max_gap > fail_threshold:
        status = "FAIL"
    elif max_gap > warn_threshold:
        status = "WARN"
    else:
        status = "PASS"

    logger.info(f"Recent gaps check: max gap={max_gap} min (warn={warn_threshold}, fail={fail_threshold}) → {status}.")
    return {
        "check": "recent_gaps",
        "status": status,
        "max_gap_minutes": max_gap,
        "distinct_extracao_ts_in_window": distinct_count,
        "warn_threshold_minutes": warn_threshold,
        "fail_threshold_minutes": fail_threshold,
        "recent_window_minutes": recent_window,
    }


def validate_positions_quality(df, config, now_fn=None):
    logger.info(f"Running positions quality checks on {len(df)} position records.")
    checks = [
        check_freshness(df, config, now_fn),
        check_recent_gaps(df, config, now_fn),
    ]
    statuses = [c["status"] for c in checks]
    if "FAIL" in statuses:
        overall = "FAIL"
    elif "WARN" in statuses:
        overall = "WARN"
    else:
        overall = "PASS"
    logger.info(f"Positions quality overall status: {overall}.")
    return {
        "status": overall,
        "positions_in_time_window_count": len(df),
        "checks": checks,
    }

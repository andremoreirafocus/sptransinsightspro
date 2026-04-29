import logging

logger = logging.getLogger(__name__)


def _effective_window_minutes(df):
    max_ts = df["extracao_ts"].max()
    min_ts = df["extracao_ts"].min()
    return round((max_ts - min_ts).total_seconds() / 60, 2)


def check_zero_trips(effective_window_minutes, trips_count, config):
    def get_config(config):
        return config["general"]["quality"]["trips_effective_window_threshold_minutes"]

    window_threshold = get_config(config)

    if effective_window_minutes > window_threshold and trips_count == 0:
        status = "WARN"
        note = f"no trips extracted despite effective window of {effective_window_minutes} min (threshold: {window_threshold} min)"
    else:
        status = "PASS"
        note = None

    logger.info(
        f"Zero trips check: effective_window={effective_window_minutes} min, trips={trips_count} → {status}."
    )
    result = {
        "check": "zero_trips",
        "status": status,
        "effective_window_minutes": effective_window_minutes,
        "window_threshold_minutes": window_threshold,
    }
    if note:
        result["note"] = note
    return result


def check_low_trip_count(effective_window_minutes, trips_count, config):
    def get_config(config):
        quality = config["general"]["quality"]
        return (
            quality["trips_effective_window_threshold_minutes"],
            quality["trips_min_trips_threshold"],
        )

    window_threshold, min_trips_threshold = get_config(config)

    if effective_window_minutes > window_threshold and trips_count < min_trips_threshold:
        status = "WARN"
        note = f"only {trips_count} trips extracted (min threshold: {min_trips_threshold}) in {effective_window_minutes} min window"
    else:
        status = "PASS"
        note = None

    logger.info(
        f"Low trip count check: trips={trips_count}, threshold={min_trips_threshold} → {status}."
    )
    result = {
        "check": "low_trip_count",
        "status": status,
        "trips_extracted": trips_count,
        "min_trips_threshold": min_trips_threshold,
    }
    if note:
        result["note"] = note
    return result


def validate_trips_quality(df, trips_extracted, config):
    trips_count = len(trips_extracted)
    effective_window = _effective_window_minutes(df)
    logger.info(
        f"Running trip extraction quality checks: {trips_count} trips, effective window {effective_window} min."
    )
    checks = [
        check_zero_trips(effective_window, trips_count, config),
        check_low_trip_count(effective_window, trips_count, config),
    ]
    overall = "WARN" if any(c["status"] == "WARN" for c in checks) else "PASS"
    logger.info(f"Trip extraction quality overall status: {overall}.")
    return {
        "status": overall,
        "effective_window_minutes": effective_window,
        "trips_extracted": trips_count,
        "checks": checks,
    }

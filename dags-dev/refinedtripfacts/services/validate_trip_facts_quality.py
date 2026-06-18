from typing import Any, Dict, Literal

_STATUS_PRIORITY: Dict[str, int] = {"FAIL": 2, "WARN": 1, "PASS": 0}


def _worst(*statuses: str) -> Literal["PASS", "WARN", "FAIL"]:
    return max(statuses, key=lambda s: _STATUS_PRIORITY.get(s, 0))  # type: ignore[return-value]


def validate_trip_facts_quality(
    config: Dict[str, Any],
    finished_trips_read: int,
    metrics: Dict[str, Any],
) -> Dict[str, Any]:
    quality = config["general"]["quality"]
    warn_threshold = quality["completeness_loss_rate_warn_threshold"]
    fail_threshold = quality["completeness_loss_rate_fail_threshold"]
    avg_speed_kmh_max = quality["avg_speed_kmh_max"]

    persisted_facts = metrics["persisted_facts"]
    uncovered_dim_keys = metrics["uncovered_dim_keys"]
    negative_duration = metrics["negative_duration"]
    negative_distance = metrics["negative_distance"]
    time_incoherent = metrics["time_incoherent"]
    implausible_speed = metrics["implausible_speed"]

    loss_rate = (
        0.0
        if finished_trips_read == 0
        else (finished_trips_read - persisted_facts) / finished_trips_read
    )
    if loss_rate > fail_threshold:
        completeness_status: Literal["PASS", "WARN", "FAIL"] = "FAIL"
    elif loss_rate > warn_threshold:
        completeness_status = "WARN"
    else:
        completeness_status = "PASS"

    coverage_status: Literal["PASS", "WARN", "FAIL"] = "FAIL" if uncovered_dim_keys > 0 else "PASS"

    domain_status: Literal["PASS", "WARN", "FAIL"] = (
        "FAIL"
        if any([negative_duration, negative_distance, time_incoherent, implausible_speed])
        else "PASS"
    )

    checks = [
        {
            "check": "completeness",
            "status": completeness_status,
            "finished_trips_read": finished_trips_read,
            "persisted_facts": persisted_facts,
            "loss_rate": loss_rate,
            "warn_threshold": warn_threshold,
            "fail_threshold": fail_threshold,
        },
        {
            "check": "dim_time_coverage",
            "status": coverage_status,
            "uncovered_dim_keys": uncovered_dim_keys,
        },
        {
            "check": "value_domain",
            "status": domain_status,
            "negative_duration": negative_duration,
            "negative_distance": negative_distance,
            "time_incoherent": time_incoherent,
            "implausible_speed": implausible_speed,
            "avg_speed_kmh_max": avg_speed_kmh_max,
        },
    ]

    return {
        "status": _worst(completeness_status, coverage_status, domain_status),
        "loss_rate": loss_rate,
        "persisted_facts": persisted_facts,
        "finished_trips_read": finished_trips_read,
        "checks": checks,
    }

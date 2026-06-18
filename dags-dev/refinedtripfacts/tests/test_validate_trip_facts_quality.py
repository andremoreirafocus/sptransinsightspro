import pytest

from refinedtripfacts.services.validate_trip_facts_quality import validate_trip_facts_quality


def make_config(warn=0.01, fail=0.05, max_speed=120.0):
    return {
        "general": {
            "quality": {
                "completeness_loss_rate_warn_threshold": warn,
                "completeness_loss_rate_fail_threshold": fail,
                "avg_speed_kmh_max": max_speed,
            }
        }
    }


def make_metrics(persisted=100, uncovered=0, neg_dur=0, neg_dist=0, time_inc=0, impl_speed=0):
    return {
        "persisted_facts": persisted,
        "uncovered_dim_keys": uncovered,
        "negative_duration": neg_dur,
        "negative_distance": neg_dist,
        "time_incoherent": time_inc,
        "implausible_speed": impl_speed,
    }


def test_completeness_pass_when_no_loss():
    result = validate_trip_facts_quality(make_config(), 100, make_metrics(persisted=100))
    completeness = next(c for c in result["checks"] if c["check"] == "completeness")
    assert completeness["status"] == "PASS"
    assert result["status"] == "PASS"


def test_completeness_warn_when_loss_between_thresholds():
    # loss_rate = 2/100 = 0.02; warn=0.01, fail=0.05 → WARN
    result = validate_trip_facts_quality(make_config(), 100, make_metrics(persisted=98))
    completeness = next(c for c in result["checks"] if c["check"] == "completeness")
    assert completeness["status"] == "WARN"


def test_completeness_fail_when_loss_above_fail_threshold():
    # loss_rate = 10/100 = 0.10; fail=0.05 → FAIL
    result = validate_trip_facts_quality(make_config(), 100, make_metrics(persisted=90))
    completeness = next(c for c in result["checks"] if c["check"] == "completeness")
    assert completeness["status"] == "FAIL"


def test_loss_rate_computed_correctly():
    result = validate_trip_facts_quality(make_config(), 100, make_metrics(persisted=98))
    assert result["loss_rate"] == pytest.approx(0.02)


def test_dim_time_coverage_fail_when_uncovered_keys():
    result = validate_trip_facts_quality(make_config(), 100, make_metrics(uncovered=3))
    coverage = next(c for c in result["checks"] if c["check"] == "dim_time_coverage")
    assert coverage["status"] == "FAIL"


def test_dim_time_coverage_pass_when_zero_uncovered():
    result = validate_trip_facts_quality(make_config(), 100, make_metrics(uncovered=0))
    coverage = next(c for c in result["checks"] if c["check"] == "dim_time_coverage")
    assert coverage["status"] == "PASS"


def test_value_domain_fail_on_negative_duration():
    result = validate_trip_facts_quality(make_config(), 100, make_metrics(neg_dur=1))
    domain = next(c for c in result["checks"] if c["check"] == "value_domain")
    assert domain["status"] == "FAIL"
    assert domain["negative_duration"] == 1


def test_value_domain_pass_when_all_zero():
    result = validate_trip_facts_quality(make_config(), 100, make_metrics())
    domain = next(c for c in result["checks"] if c["check"] == "value_domain")
    assert domain["status"] == "PASS"


def test_overall_status_is_worst_of_all_checks():
    # completeness PASS, coverage FAIL → overall FAIL
    result = validate_trip_facts_quality(make_config(), 100, make_metrics(persisted=100, uncovered=1))
    assert result["status"] == "FAIL"


def test_returns_dict_with_all_three_checks():
    result = validate_trip_facts_quality(make_config(), 100, make_metrics())
    assert "status" in result
    assert "loss_rate" in result
    assert "persisted_facts" in result
    assert "finished_trips_read" in result
    assert len(result["checks"]) == 3
    check_names = {c["check"] for c in result["checks"]}
    assert check_names == {"completeness", "dim_time_coverage", "value_domain"}


def test_zero_finished_trips_does_not_divide_by_zero():
    result = validate_trip_facts_quality(make_config(), 0, make_metrics(persisted=0))
    assert result["loss_rate"] == 0.0

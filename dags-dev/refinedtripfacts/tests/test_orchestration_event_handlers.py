import json
from datetime import datetime

import pytest

from refinedtripfacts.build_trip_facts import build_trip_facts
from refinedtripfacts.tests.fakes.fake_orchestration_dependencies import FakeOrchestrationDependencies

LOGIC_DATE_STR = "2026-06-08T15:00:00+00:00"
LOGIC_DATE = datetime.fromisoformat(LOGIC_DATE_STR)


def _parse_events(caplog, event_name: str) -> list[dict]:
    results = []
    for record in caplog.records:
        try:
            parsed = json.loads(record.getMessage())
        except Exception:
            continue
        if parsed.get("event") == event_name:
            results.append(parsed)
    return results


def test_logic_date_str_used_as_correlation_id(caplog):
    fake = FakeOrchestrationDependencies()
    caplog.set_level("INFO")
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    events = _parse_events(caplog, "execution_started")
    assert len(events) == 1
    assert events[0].get("correlation_id") == LOGIC_DATE_STR
    assert events[0].get("execution_id") != LOGIC_DATE_STR


def test_logic_date_parsed_correctly_from_logic_date_str():
    fake = FakeOrchestrationDependencies()
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    called_with = fake.call_kwargs["measure_input_trips"]["logic_date"]
    assert called_with == LOGIC_DATE
    assert isinstance(called_with, datetime)


def test_measure_called_with_correct_logic_date():
    fake = FakeOrchestrationDependencies()
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    assert fake.call_kwargs["measure_input_trips"]["logic_date"] == LOGIC_DATE


def test_no_upstream_data_aborts():
    fake = FakeOrchestrationDependencies(finished_trips_read=0)
    with pytest.raises(ValueError):
        build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    assert "provision_dim_time" not in fake.call_log
    assert "create_trip_facts" not in fake.call_log
    assert "measure_persisted_facts" not in fake.call_log
    assert "validate_trip_facts_quality" not in fake.call_log
    assert "create_failure_quality_report" in fake.call_log


def test_provision_called_with_logic_date():
    fake = FakeOrchestrationDependencies()
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    assert fake.call_kwargs["provision_dim_time"]["logic_date"] == LOGIC_DATE


def test_provision_called_before_creation():
    fake = FakeOrchestrationDependencies()
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    assert fake.call_log.index("provision_dim_time") < fake.call_log.index("create_trip_facts")


def test_create_called_with_logic_date():
    fake = FakeOrchestrationDependencies()
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    assert fake.call_kwargs["create_trip_facts"]["logic_date"] == LOGIC_DATE


def test_measurement_result_stored_on_state():
    fake = FakeOrchestrationDependencies(finished_trips_read=7)
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    assert fake.call_kwargs["validate_trip_facts_quality"]["finished_trips_read"] == 7


def test_creation_result_stored_on_state():
    fake = FakeOrchestrationDependencies()
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    creation = fake.call_kwargs["create_final_quality_report"]["creation_result"]
    assert "facts_derived" in creation
    assert "inserted_rows" in creation
    assert "skipped_rows" in creation


def test_persisted_facts_measured_after_creation():
    fake = FakeOrchestrationDependencies()
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    assert fake.call_log.index("measure_persisted_facts") > fake.call_log.index("create_trip_facts")
    persisted_metrics = fake.call_kwargs["validate_trip_facts_quality"]["persisted_metrics"]
    assert "persisted_facts" in persisted_metrics
    assert "uncovered_dim_keys" in persisted_metrics


def test_quality_called_with_finished_and_persisted_metrics():
    fake = FakeOrchestrationDependencies(finished_trips_read=5)
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    kwargs = fake.call_kwargs["validate_trip_facts_quality"]
    assert kwargs["finished_trips_read"] == 5
    assert "persisted_facts" in kwargs["persisted_metrics"]


def test_quality_result_stored_on_state():
    fake = FakeOrchestrationDependencies()
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    quality_result = fake.call_kwargs["create_final_quality_report"]["quality_result"]
    assert "status" in quality_result
    assert "loss_rate" in quality_result


def test_quality_warn_does_not_abort():
    fake = FakeOrchestrationDependencies(quality_status="WARN")
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    assert "create_final_quality_report" in fake.call_log


def test_quality_fail_does_not_abort():
    fake = FakeOrchestrationDependencies(quality_status="FAIL")
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    assert "create_final_quality_report" in fake.call_log
    assert fake.call_kwargs["create_final_quality_report"]["quality_result"]["status"] == "FAIL"


def test_dim_time_coverage_fail_does_not_abort():
    fake = FakeOrchestrationDependencies(quality_status="FAIL")
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    assert "create_final_quality_report" in fake.call_log


def test_column_lineage_built_from_live_table_columns():
    fake = FakeOrchestrationDependencies()
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    assert "get_trip_facts_table_columns" in fake.call_log
    assert "column_lineage" in fake.call_kwargs["create_final_quality_report"]


def test_lineage_failure_does_not_abort():
    fake = FakeOrchestrationDependencies(lineage_raises=RuntimeError("schema query failed"))
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    assert "create_final_quality_report" in fake.call_log
    lineage = fake.call_kwargs["create_final_quality_report"]["column_lineage"]
    assert lineage.get("drift_detected") is None
    assert "lineage validation unavailable" in lineage.get("warning", "")


def test_drift_detected_does_not_abort():
    fake = FakeOrchestrationDependencies()
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    assert "create_final_quality_report" in fake.call_log
    lineage = fake.call_kwargs["create_final_quality_report"]["column_lineage"]
    assert lineage.get("drift_detected") is True


def test_quality_report_called_with_explicit_phase_results():
    fake = FakeOrchestrationDependencies()
    build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    kwargs = fake.call_kwargs["create_final_quality_report"]
    assert "measurement_result" in kwargs
    assert "dim_time_result" in kwargs
    assert "creation_result" in kwargs
    assert "persisted_metrics" in kwargs
    assert "quality_result" in kwargs


def test_execution_aborted_on_creation_failure(caplog):
    fake = FakeOrchestrationDependencies(creation_raises=ValueError("insert failed"))
    caplog.set_level("ERROR")
    with pytest.raises(ValueError):
        build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    events = _parse_events(caplog, "execution_aborted")
    assert len(events) == 1
    assert events[0]["metadata"]["phase"] == "trip_facts_creation"


def test_execution_aborted_on_config_load_failure(caplog):
    fake = FakeOrchestrationDependencies(config_raises=RuntimeError("config unavailable"))
    caplog.set_level("ERROR")
    with pytest.raises(ValueError, match="Configuration load"):
        build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    events = _parse_events(caplog, "execution_aborted")
    assert len(events) == 1
    assert events[0]["metadata"]["phase"] == "config_load"
    assert "create_failure_quality_report" not in fake.call_log


def test_execution_aborted_on_provisioning_failure(caplog):
    fake = FakeOrchestrationDependencies(provision_raises=ValueError("provision failed"))
    caplog.set_level("ERROR")
    with pytest.raises(ValueError):
        build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    events = _parse_events(caplog, "execution_aborted")
    assert len(events) == 1
    assert events[0]["metadata"]["phase"] == "dim_time_provisioning"


def test_execution_aborted_on_verification_failure(caplog):
    fake = FakeOrchestrationDependencies(verification_raises=ValueError("read-back failed"))
    caplog.set_level("ERROR")
    with pytest.raises(ValueError):
        build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    events = _parse_events(caplog, "execution_aborted")
    assert len(events) == 1
    assert events[0]["metadata"]["phase"] == "trip_facts_verification"


def test_failure_report_called_on_creation_failure():
    fake = FakeOrchestrationDependencies(creation_raises=ValueError("insert failed"))
    with pytest.raises(ValueError):
        build_trip_facts(LOGIC_DATE_STR, fake.as_deps())
    assert "create_failure_quality_report" in fake.call_log
    kwargs = fake.call_kwargs["create_failure_quality_report"]
    assert kwargs["measurement_result"] is not None
    assert kwargs["quality_result"] is None

import json

import pytest

from refinedfinishedtrips.extract_trips import (
    extract_trips_for_all_Lines_and_vehicles,
)
from refinedfinishedtrips.tests.fakes import FakeRefinedFinishedTripsOrchestrationDependencies


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


def make_config():
    return {
        "general": {
            "tables": {"finished_trips_table_name": "finished_trips"},
            "quality": {
                "trips_effective_window_threshold_minutes": 60,
                "trips_min_trips_threshold": 5,
            },
            "trip_detection": {"stop_proximity_threshold_meters": 100},
        },
        "connections": {
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "testdb",
                "user": "user",
                "password": "pass",
            },
            "object_storage": {
                "endpoint": "localhost:9000",
                "access_key": "minioadmin",
                "secret_key": "minioadmin",
            },
        },
    }


# ---------------------------------------------------------------------------
# Positions FAIL → pipeline stops, phases 2 and 3 not executed
# ---------------------------------------------------------------------------


def test_positions_fail_raises_and_save_not_called():
    deps, recorder = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario(
        positions_status="FAIL"
    )
    with pytest.raises(ValueError, match="Positions quality check FAILED"):
        extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-04-14T00:00:00+00:00")
    assert recorder.save_calls == []


def test_positions_fail_failure_report_called():
    deps, recorder = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario(
        positions_status="FAIL"
    )
    with pytest.raises(ValueError):
        extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-04-14T00:00:00+00:00")
    assert len(recorder.failure_report_calls) == 1
    assert recorder.failure_report_calls[0]["failure_phase"] == "positions_quality"


def test_positions_fail_final_report_not_called():
    deps, recorder = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario(
        positions_status="FAIL"
    )
    with pytest.raises(ValueError):
        extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-04-14T00:00:00+00:00")
    assert recorder.final_report_calls == []


def test_trip_extraction_failure_calls_create_failure_report_with_positions_result():
    deps, recorder = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario(
        extract_trips_raises=RuntimeError("trip extraction exploded")
    )
    with pytest.raises(RuntimeError, match="trip extraction exploded"):
        extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-04-14T00:00:00+00:00")
    assert len(recorder.failure_report_calls) == 1
    call = recorder.failure_report_calls[0]
    assert call["failure_phase"] == "trip_extraction"
    assert call["failure_message"] == "trip extraction exploded"
    assert call["positions_result"]["status"] == "PASS"
    assert call["trips_result"] is None
    assert call["persistence_result"] is None


def test_persistence_failure_calls_create_failure_report_with_partial_results():
    deps, recorder = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario(
        save_trips_raises=RuntimeError("save failed")
    )
    with pytest.raises(RuntimeError, match="save failed"):
        extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-04-14T00:00:00+00:00")
    assert len(recorder.failure_report_calls) == 1
    call = recorder.failure_report_calls[0]
    assert call["failure_phase"] == "persistence"
    assert call["failure_message"] == "save failed"
    assert call["positions_result"]["status"] == "PASS"
    assert call["trips_result"]["status"] == "PASS"
    assert call["persistence_result"] is None
    assert call["column_lineage"]["table_name"] == "finished_trips"


# ---------------------------------------------------------------------------
# Positions WARN → early report generated, pipeline continues to Phase 3
# ---------------------------------------------------------------------------


def test_positions_warn_pipeline_continues_and_save_called():
    deps, recorder = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario(
        positions_status="WARN"
    )
    extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-04-14T00:00:00+00:00")
    assert len(recorder.save_calls) == 1


# ---------------------------------------------------------------------------
# All phases PASS → final report called once
# ---------------------------------------------------------------------------


def test_all_phases_pass_final_report_called():
    deps, recorder = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario()
    extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-04-14T00:00:00+00:00")
    assert len(recorder.final_report_calls) == 1


def test_all_phases_pass_final_report_status_pass():
    deps, recorder = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario()
    extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-04-14T00:00:00+00:00")
    assert len(recorder.final_report_calls) == 1
    call = recorder.final_report_calls[0]
    assert call["positions_result"]["status"] == "PASS"
    assert call["trips_result"]["status"] == "PASS"
    assert call["persistence_result"]["status"] == "PASS"
    assert call["column_lineage"]["table_name"] == "finished_trips"


def test_trip_extraction_metrics_reach_final_report():
    extraction_metrics = {
        "total_finished_trips": 1,
        "total_source_sentido_discrepancies": 3,
        "total_input_position_sanitization_drops": 7,
        "total_input_position_records": 42,
        "vehicle_line_processing_succeeded": 1,
    }
    deps, recorder = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario(
        extract_trips_output=([], extraction_metrics)
    )
    extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-04-14T00:00:00+00:00")
    assert len(recorder.final_report_calls) == 1
    trips_result = recorder.final_report_calls[0]["trips_result"]
    assert trips_result["source_sentido_discrepancies"] == 3
    assert trips_result["sanitization_dropped_points"] == 7
    assert trips_result["input_position_records"] == 42
    assert trips_result["vehicle_line_processing_succeeded"] == 1
    assert recorder.final_report_calls[0]["column_lineage"]["table_name"] == "finished_trips"


# ---------------------------------------------------------------------------
# Positions PASS → extraction and save proceed (existing behaviour)
# ---------------------------------------------------------------------------


def test_no_trips_extracted_save_called_with_empty_list():
    deps, recorder = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario(
        extract_trips_output=([], {})
    )
    extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-04-14T00:00:00+00:00")
    assert len(recorder.save_calls) == 1
    assert recorder.save_calls[0]["trips"] == []


def test_two_vehicles_save_called_once_with_combined_result():
    trips = [{"trip_id": "1234-10"}, {"trip_id": "5678-20"}]
    deps, recorder = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario(
        extract_trips_output=(trips, {})
    )
    extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-04-14T00:00:00+00:00")
    assert len(recorder.save_calls) == 1


# ---------------------------------------------------------------------------
# execution_aborted (Step 2)
# ---------------------------------------------------------------------------


def test_execution_aborted_event_emitted_on_pipeline_failure(caplog):
    deps, _ = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario(
        positions_status="FAIL"
    )
    caplog.set_level("ERROR")

    with pytest.raises(ValueError):
        extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-04-14T00:00:00+00:00")

    events = _parse_events(caplog, "execution_aborted")
    assert len(events) == 1
    event = events[0]
    assert event["status"] == "FAILED"
    assert "phase" in event["metadata"]
    assert event["metadata"]["phase"] == "positions_quality"
    assert "execution_id" in event
    assert "correlation_id" in event


# ---------------------------------------------------------------------------
# quality_report_metrics (Step 3)
# ---------------------------------------------------------------------------


def test_quality_report_metrics_emitted_on_success(caplog):
    deps, _ = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario()
    caplog.set_level("INFO")

    extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-04-14T00:00:00+00:00")

    events = _parse_events(caplog, "quality_report_metrics")
    assert len(events) == 1
    event = events[0]
    assert event["status"] == "SUCCEEDED"


def test_quality_report_metrics_emitted_on_failure(caplog):
    deps, _ = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario(
        positions_status="FAIL"
    )
    caplog.set_level("INFO")

    with pytest.raises(ValueError):
        extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-04-14T00:00:00+00:00")

    events = _parse_events(caplog, "quality_report_metrics")
    assert len(events) == 1
    event = events[0]
    assert event["status"] == "FAILED"


# ---------------------------------------------------------------------------
# logic_date propagation (Step 9)
# ---------------------------------------------------------------------------


def test_logic_date_parsed_from_logic_date_str():
    from datetime import datetime, timezone
    deps, recorder = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario()
    extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-06-01T00:00:00+00:00")
    assert recorder.save_calls[0]["logic_date"] == datetime(2026, 6, 1, tzinfo=timezone.utc)


def test_logic_date_passed_to_save_finished_trips_to_db():
    deps, recorder = FakeRefinedFinishedTripsOrchestrationDependencies.create_scenario()
    extract_trips_for_all_Lines_and_vehicles(make_config(), deps, correlation_id="test-correlation-id", logic_date_str="2026-06-01")
    assert len(recorder.save_calls) == 1
    assert "logic_date" in recorder.save_calls[0]
    assert recorder.save_calls[0]["logic_date"] is not None



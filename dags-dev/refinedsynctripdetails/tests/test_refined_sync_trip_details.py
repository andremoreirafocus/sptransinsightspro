import importlib.util
import json
import logging
import pathlib

import pytest

from refinedsynctripdetails.tests.fakes.fake_refinedsynctripdetails_orchestration_dependencies import (
    FakeRefinedSyncTripDetailsOrchestrationDependencies,
)

_mod_path = pathlib.Path(__file__).parents[2] / "refinedsynctripdetails-v3.py"
_spec = importlib.util.spec_from_file_location("refinedsynctripdetails_v3", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
refined_sync_trip_details = _mod.refined_sync_trip_details


# --- behavioral tests ---

def test_all_services_called_on_success():
    deps, recorder = FakeRefinedSyncTripDetailsOrchestrationDependencies.create_scenario()
    refined_sync_trip_details(deps=deps)
    assert len(recorder.load_calls) == 1
    assert len(recorder.transform_calls) == 1
    assert len(recorder.save_calls) == 1


def test_config_load_failure_raises():
    deps, _ = FakeRefinedSyncTripDetailsOrchestrationDependencies.create_scenario(
        config_raises=RuntimeError("config boom")
    )
    with pytest.raises(Exception):
        refined_sync_trip_details(deps=deps)


def test_load_failure_raises():
    deps, _ = FakeRefinedSyncTripDetailsOrchestrationDependencies.create_scenario(
        load_raises=RuntimeError("load boom")
    )
    with pytest.raises(Exception):
        refined_sync_trip_details(deps=deps)


def test_transform_failure_raises():
    deps, _ = FakeRefinedSyncTripDetailsOrchestrationDependencies.create_scenario(
        transform_raises=RuntimeError("transform boom")
    )
    with pytest.raises(Exception):
        refined_sync_trip_details(deps=deps)


def test_save_failure_raises():
    deps, _ = FakeRefinedSyncTripDetailsOrchestrationDependencies.create_scenario(
        save_raises=RuntimeError("save boom")
    )
    with pytest.raises(Exception):
        refined_sync_trip_details(deps=deps)


def test_load_failure_transform_not_called():
    deps, recorder = FakeRefinedSyncTripDetailsOrchestrationDependencies.create_scenario(
        load_raises=RuntimeError("load boom")
    )
    with pytest.raises(Exception):
        refined_sync_trip_details(deps=deps)
    assert len(recorder.transform_calls) == 0


def test_transform_failure_save_not_called():
    deps, recorder = FakeRefinedSyncTripDetailsOrchestrationDependencies.create_scenario(
        transform_raises=RuntimeError("transform boom")
    )
    with pytest.raises(Exception):
        refined_sync_trip_details(deps=deps)
    assert len(recorder.save_calls) == 0


def test_save_failure_save_was_attempted():
    deps, recorder = FakeRefinedSyncTripDetailsOrchestrationDependencies.create_scenario(
        save_raises=RuntimeError("save boom")
    )
    with pytest.raises(Exception):
        refined_sync_trip_details(deps=deps)
    assert len(recorder.save_calls) == 1


# --- event-emission tests ---

def _log_records(caplog):
    records = []
    for r in caplog.records:
        try:
            records.append(json.loads(r.getMessage()))
        except (ValueError, TypeError):
            pass
    return records


def test_success_emits_execution_finished(caplog):
    deps, _ = FakeRefinedSyncTripDetailsOrchestrationDependencies.create_scenario()
    with caplog.at_level(logging.INFO):
        refined_sync_trip_details(deps=deps)
    assert any(r.get("event") == "execution_finished" for r in _log_records(caplog))


def test_success_emits_execution_phase_metrics(caplog):
    deps, _ = FakeRefinedSyncTripDetailsOrchestrationDependencies.create_scenario()
    with caplog.at_level(logging.INFO):
        refined_sync_trip_details(deps=deps)
    matched = [
        r for r in _log_records(caplog)
        if r.get("event") == "execution_phase_metrics" and r.get("status") == "SUCCEEDED"
    ]
    assert matched


def test_config_failure_emits_execution_aborted(caplog):
    deps, _ = FakeRefinedSyncTripDetailsOrchestrationDependencies.create_scenario(
        config_raises=RuntimeError("config boom")
    )
    with caplog.at_level(logging.ERROR):
        with pytest.raises(Exception):
            refined_sync_trip_details(deps=deps)
    matched = [
        r for r in _log_records(caplog)
        if r.get("event") == "execution_aborted"
        and r.get("metadata", {}).get("phase") == "config_load"
    ]
    assert matched


def test_load_failure_emits_execution_aborted(caplog):
    deps, _ = FakeRefinedSyncTripDetailsOrchestrationDependencies.create_scenario(
        load_raises=RuntimeError("load boom")
    )
    with caplog.at_level(logging.ERROR):
        with pytest.raises(Exception):
            refined_sync_trip_details(deps=deps)
    matched = [
        r for r in _log_records(caplog)
        if r.get("event") == "execution_aborted"
        and r.get("metadata", {}).get("phase") == "load_trip_details"
    ]
    assert matched


def test_transform_failure_emits_execution_aborted(caplog):
    deps, _ = FakeRefinedSyncTripDetailsOrchestrationDependencies.create_scenario(
        transform_raises=RuntimeError("transform boom")
    )
    with caplog.at_level(logging.ERROR):
        with pytest.raises(Exception):
            refined_sync_trip_details(deps=deps)
    matched = [
        r for r in _log_records(caplog)
        if r.get("event") == "execution_aborted"
        and r.get("metadata", {}).get("phase") == "transform_trip_details"
    ]
    assert matched


def test_save_failure_emits_execution_aborted(caplog):
    deps, _ = FakeRefinedSyncTripDetailsOrchestrationDependencies.create_scenario(
        save_raises=RuntimeError("save boom")
    )
    with caplog.at_level(logging.ERROR):
        with pytest.raises(Exception):
            refined_sync_trip_details(deps=deps)
    matched = [
        r for r in _log_records(caplog)
        if r.get("event") == "execution_aborted"
        and r.get("metadata", {}).get("phase") == "save_trip_details"
    ]
    assert matched


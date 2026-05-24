import importlib.util
import json
import logging
import pathlib

import pytest

from updatelatestpositions.tests.fakes.fake_updatelatestpositions_orchestration_dependencies import (
    FakeUpdateLatestPositionsOrchestrationDependencies,
)

_mod_path = pathlib.Path(__file__).parents[2] / "updatelatestpositions-v4.py"
_spec = importlib.util.spec_from_file_location("updatelatestpositions_v4", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
update_latest_positions_table = _mod.update_latest_positions_table


def test_create_latest_positions_called_on_success():
    deps, recorder = FakeUpdateLatestPositionsOrchestrationDependencies.create_scenario()
    update_latest_positions_table(deps=deps)
    assert len(recorder.create_calls) == 1


def test_config_load_failure_raises():
    deps, _ = FakeUpdateLatestPositionsOrchestrationDependencies.create_scenario(
        config_raises=RuntimeError("config boom")
    )
    with pytest.raises(Exception):
        update_latest_positions_table(deps=deps)


def test_update_failure_raises():
    deps, _ = FakeUpdateLatestPositionsOrchestrationDependencies.create_scenario(
        create_raises=RuntimeError("update boom")
    )
    with pytest.raises(Exception):
        update_latest_positions_table(deps=deps)


def test_update_failure_create_was_attempted():
    deps, recorder = FakeUpdateLatestPositionsOrchestrationDependencies.create_scenario(
        create_raises=RuntimeError("update boom")
    )
    with pytest.raises(Exception):
        update_latest_positions_table(deps=deps)
    assert len(recorder.create_calls) == 1


def _log_records(caplog):
    records = []
    for r in caplog.records:
        try:
            records.append(json.loads(r.getMessage()))
        except (ValueError, TypeError):
            pass
    return records


def test_success_emits_execution_finished(caplog):
    deps, _ = FakeUpdateLatestPositionsOrchestrationDependencies.create_scenario()
    with caplog.at_level(logging.INFO):
        update_latest_positions_table(deps=deps)
    assert any(r.get("event") == "execution_finished" for r in _log_records(caplog))


def test_success_emits_execution_phase_metrics(caplog):
    deps, _ = FakeUpdateLatestPositionsOrchestrationDependencies.create_scenario()
    with caplog.at_level(logging.INFO):
        update_latest_positions_table(deps=deps)
    matched = [
        r for r in _log_records(caplog)
        if r.get("event") == "execution_phase_metrics" and r.get("status") == "SUCCEEDED"
    ]
    assert matched


def test_config_failure_emits_execution_aborted(caplog):
    deps, _ = FakeUpdateLatestPositionsOrchestrationDependencies.create_scenario(
        config_raises=RuntimeError("config boom")
    )
    with caplog.at_level(logging.ERROR):
        with pytest.raises(Exception):
            update_latest_positions_table(deps=deps)
    matched = [
        r for r in _log_records(caplog)
        if r.get("event") == "execution_aborted"
        and r.get("metadata", {}).get("phase") == "config_load"
    ]
    assert matched


def test_update_failure_emits_execution_aborted(caplog):
    deps, _ = FakeUpdateLatestPositionsOrchestrationDependencies.create_scenario(
        create_raises=RuntimeError("update boom")
    )
    with caplog.at_level(logging.ERROR):
        with pytest.raises(Exception):
            update_latest_positions_table(deps=deps)
    matched = [
        r for r in _log_records(caplog)
        if r.get("event") == "execution_aborted"
        and r.get("metadata", {}).get("phase") == "update_latest_positions"
    ]
    assert matched

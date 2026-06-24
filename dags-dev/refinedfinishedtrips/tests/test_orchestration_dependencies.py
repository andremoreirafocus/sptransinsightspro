import pytest

from refinedfinishedtrips.orchestration_dependencies import (
    RefinedFinishedTripsOrchestrationDependencies,
    get_refinedfinishedtrips_orchestration_dependencies,
)

_NOOP = lambda *a, **k: None  # noqa: E731

_ALL_FIELDS = [
    "get_config",
    "get_recent_positions",
    "get_all_finished_trips",
    "validate_positions_quality",
    "validate_trips_quality",
    "save_finished_trips_to_db",
    "create_failure_quality_report",
    "create_final_quality_report",
]


def _make_deps(**overrides) -> RefinedFinishedTripsOrchestrationDependencies:
    defaults = {field: _NOOP for field in _ALL_FIELDS}
    defaults.update(overrides)
    return RefinedFinishedTripsOrchestrationDependencies(**defaults)


def test_deps_is_frozen():
    deps = _make_deps()
    with pytest.raises((AttributeError, TypeError)):
        deps.get_recent_positions = _NOOP  # type: ignore[misc]


def test_deps_has_all_expected_fields():
    deps = _make_deps()
    for field_name in _ALL_FIELDS:
        assert hasattr(deps, field_name), f"Missing field: {field_name}"


def test_deps_stores_callables_correctly():
    sentinel = object()
    deps = _make_deps(get_recent_positions=lambda c, logic_date_str: sentinel)
    assert deps.get_recent_positions(None, "2026-04-14T10:00:00+00:00") is sentinel


def test_get_refinedfinishedtrips_orchestration_dependencies_returns_instance():
    deps = get_refinedfinishedtrips_orchestration_dependencies()
    assert isinstance(deps, RefinedFinishedTripsOrchestrationDependencies)
    for field_name in _ALL_FIELDS:
        assert callable(getattr(deps, field_name))

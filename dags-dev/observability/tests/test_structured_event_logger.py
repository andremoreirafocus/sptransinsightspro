import pytest

from observability.structured_event_logger import (
    StructuredEventLogger,
    clear_execution_context,
    get_structured_logger,
    set_execution_context,
)


@pytest.fixture(autouse=True)
def reset_execution_context():
    clear_execution_context()
    yield
    clear_execution_context()


def _make_logger(component: str = "test") -> StructuredEventLogger:
    return get_structured_logger(
        service="test_service",
        component=component,
        logger_name=f"test.{component}",
    )


# --- set_execution_context ---


def test_execution_id_from_context_appears_in_payload():
    set_execution_context("exec-123", "corr-456")
    logger = _make_logger()
    payload = logger.info(event="test_event", message="hello")
    assert payload["execution_id"] == "exec-123"


def test_correlation_id_from_context_appears_in_payload():
    set_execution_context("exec-123", "corr-456")
    logger = _make_logger()
    payload = logger.info(event="test_event", message="hello")
    assert payload["correlation_id"] == "corr-456"


def test_explicit_execution_id_overrides_context():
    set_execution_context("exec-from-context", "corr-from-context")
    logger = _make_logger()
    payload = logger.info(event="test_event", message="hello", execution_id="exec-explicit")
    assert payload["execution_id"] == "exec-explicit"


def test_explicit_correlation_id_overrides_context():
    set_execution_context("exec-from-context", "corr-from-context")
    logger = _make_logger()
    payload = logger.info(event="test_event", message="hello", correlation_id="corr-explicit")
    assert payload["correlation_id"] == "corr-explicit"


def test_no_execution_id_when_context_not_set():
    logger = _make_logger()
    payload = logger.info(event="test_event", message="hello")
    assert "execution_id" not in payload


def test_no_correlation_id_when_context_not_set():
    logger = _make_logger()
    payload = logger.info(event="test_event", message="hello")
    assert "correlation_id" not in payload


# --- clear_execution_context ---


def test_clear_execution_context_removes_ids_from_payload():
    set_execution_context("exec-123", "corr-456")
    clear_execution_context()
    logger = _make_logger()
    payload = logger.info(event="test_event", message="hello")
    assert "execution_id" not in payload
    assert "correlation_id" not in payload


# --- context isolation ---


def test_context_is_independent_per_logger_instance():
    set_execution_context("exec-abc", "corr-abc")
    logger_a = _make_logger("component_a")
    logger_b = _make_logger("component_b")
    payload_a = logger_a.info(event="event_a", message="from a")
    payload_b = logger_b.info(event="event_b", message="from b")
    assert payload_a["execution_id"] == "exec-abc"
    assert payload_b["execution_id"] == "exec-abc"


def test_set_execution_context_overwrites_previous_value():
    set_execution_context("exec-first", "corr-first")
    set_execution_context("exec-second", "corr-second")
    logger = _make_logger()
    payload = logger.info(event="test_event", message="hello")
    assert payload["execution_id"] == "exec-second"
    assert payload["correlation_id"] == "corr-second"


# --- existing behaviour preserved ---


def test_explicit_values_still_work_without_context():
    logger = _make_logger()
    payload = logger.info(event="test_event", message="hello", execution_id="e", correlation_id="c")
    assert payload["execution_id"] == "e"
    assert payload["correlation_id"] == "c"


def test_payload_structure_unchanged_with_context():
    set_execution_context("exec-xyz", "corr-xyz")
    logger = _make_logger()
    payload = logger.info(event="some_event", message="msg", status="SUCCEEDED", metadata={"k": "v"})
    assert payload["service"] == "test_service"
    assert payload["component"] == "test"
    assert payload["event"] == "some_event"
    assert payload["message"] == "msg"
    assert payload["status"] == "SUCCEEDED"
    assert payload["metadata"] == {"k": "v"}
    assert "timestamp" in payload


# --- get_structured_logger auto-derivation ---


def test_service_and_component_derived_from_logger_name():
    logger = get_structured_logger(logger_name="mypipeline.services.myservice")
    payload = logger.info(event="e", message="m")
    assert payload["service"] == "mypipeline"
    assert payload["component"] == "myservice"


def test_single_segment_logger_name_uses_it_for_both():
    logger = get_structured_logger(logger_name="mypipeline")
    payload = logger.info(event="e", message="m")
    assert payload["service"] == "mypipeline"
    assert payload["component"] == "mypipeline"


def test_explicit_service_overrides_derived():
    logger = get_structured_logger(service="override_svc", logger_name="mypipeline.services.myservice")
    payload = logger.info(event="e", message="m")
    assert payload["service"] == "override_svc"
    assert payload["component"] == "myservice"


def test_explicit_component_overrides_derived():
    logger = get_structured_logger(component="orchestrator", logger_name="mypipeline.mypipeline")
    payload = logger.info(event="e", message="m")
    assert payload["service"] == "mypipeline"
    assert payload["component"] == "orchestrator"


def test_raises_when_no_logger_name_and_no_service():
    with pytest.raises(ValueError, match="service"):
        get_structured_logger(component="myservice")


def test_raises_when_no_logger_name_and_no_component():
    with pytest.raises(ValueError, match="component"):
        get_structured_logger(service="mypipeline")

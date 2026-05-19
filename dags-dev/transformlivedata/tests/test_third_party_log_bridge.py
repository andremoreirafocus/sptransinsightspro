import logging

from observability.third_party_log_bridge import configure_third_party_log_bridge


class _FakeStructuredLogger:
    def __init__(self):
        self.calls = []

    def debug(self, **kwargs):
        self.calls.append(("DEBUG", kwargs))

    def info(self, **kwargs):
        self.calls.append(("INFO", kwargs))

    def warning(self, **kwargs):
        self.calls.append(("WARNING", kwargs))

    def error(self, **kwargs):
        self.calls.append(("ERROR", kwargs))

    def critical(self, **kwargs):
        self.calls.append(("CRITICAL", kwargs))


def test_bridge_emits_structured_warning_and_suppresses_propagation():
    namespace = "test.thirdparty.bridge.warning"
    test_logger = logging.getLogger(namespace)
    test_logger.handlers = []
    test_logger.propagate = True

    fake_structured_logger = _FakeStructuredLogger()
    configure_third_party_log_bridge(
        structured_logger=fake_structured_logger,
        execution_id="exec-1",
        correlation_id="corr-1",
        namespaces=[namespace],
    )

    assert test_logger.propagate is False
    assert len(test_logger.handlers) == 1

    test_logger.warning("warning message from third-party")

    assert len(fake_structured_logger.calls) == 1
    level, payload = fake_structured_logger.calls[0]
    assert level == "WARNING"
    assert payload["event"] == "third_party_log"
    assert payload["message"] == "warning message from third-party"
    assert payload["execution_id"] == "exec-1"
    assert payload["correlation_id"] == "corr-1"
    assert payload["metadata"]["origin_logger"] == namespace


def test_bridge_reconfiguration_does_not_duplicate_handlers():
    namespace = "test.thirdparty.bridge.idempotency"
    test_logger = logging.getLogger(namespace)
    test_logger.handlers = []
    test_logger.propagate = True

    fake_structured_logger = _FakeStructuredLogger()
    configure_third_party_log_bridge(
        structured_logger=fake_structured_logger,
        execution_id="exec-1",
        correlation_id="corr-1",
        namespaces=[namespace],
    )
    configure_third_party_log_bridge(
        structured_logger=fake_structured_logger,
        execution_id="exec-2",
        correlation_id="corr-2",
        namespaces=[namespace],
    )

    assert len(test_logger.handlers) == 1

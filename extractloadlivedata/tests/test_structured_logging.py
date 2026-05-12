import json
import logging

import pytest

from src.infra.structured_logging import get_structured_logger


def test_get_structured_logger_requires_non_empty_service_and_component() -> None:
    with pytest.raises(ValueError, match="service"):
        get_structured_logger(service="", component="worker")

    with pytest.raises(ValueError, match="component"):
        get_structured_logger(service="extractloadlivedata", component="")


def test_emit_requires_supported_level() -> None:
    logger = get_structured_logger(
        service="extractloadlivedata",
        component="scheduler",
        logger_name="tests.structured.invalid_level",
    )
    with pytest.raises(ValueError, match="Unsupported level"):
        logger.emit(level="TRACE", event="tick", message="scheduled run")


def test_emit_requires_metadata_dict() -> None:
    logger = get_structured_logger(
        service="extractloadlivedata",
        component="scheduler",
        logger_name="tests.structured.invalid_metadata",
    )
    with pytest.raises(ValueError, match="metadata"):
        logger.info(event="tick", message="scheduled run", metadata="wrong-type")


def test_emit_outputs_json_with_canonical_required_fields(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger_name = "tests.structured.payload"
    logger = get_structured_logger(
        service="extractloadlivedata",
        component="scheduler",
        logger_name=logger_name,
        base_metadata={"env": "dev"},
    )

    with caplog.at_level(logging.INFO, logger=logger_name):
        payload = logger.info(
            event="extract_cycle_completed",
            message="cycle completed",
            execution_id="exec-1",
            correlation_id="corr-1",
            status="PASS",
            metadata={"records": 5},
        )

    assert len(caplog.records) == 1
    serialized = caplog.records[0].message
    decoded = json.loads(serialized)

    for field in ("timestamp", "level", "service", "component", "event", "message"):
        assert field in decoded

    assert decoded["level"] == "INFO"
    assert decoded["service"] == "extractloadlivedata"
    assert decoded["component"] == "scheduler"
    assert decoded["event"] == "extract_cycle_completed"
    assert decoded["message"] == "cycle completed"
    assert decoded["execution_id"] == "exec-1"
    assert decoded["correlation_id"] == "corr-1"
    assert decoded["status"] == "PASS"
    assert decoded["metadata"] == {"env": "dev", "records": 5}

    assert payload == decoded


import json
import logging

import pytest

from observability.process_structured_logger import JsonLineFormatter


def _make_record(message: str, level: int = logging.INFO, name: str = "my.component") -> logging.LogRecord:
    return logging.LogRecord(
        name=name,
        level=level,
        pathname="",
        lineno=0,
        msg=message,
        args=(),
        exc_info=None,
    )


# --- pass-through ---


def test_valid_json_dict_message_is_returned_unchanged():
    formatter = JsonLineFormatter(service="svc")
    payload = json.dumps({"event": "something", "message": "ok"})
    result = formatter.format(_make_record(payload))
    assert result == payload


def test_valid_json_dict_result_is_not_re_encoded():
    formatter = JsonLineFormatter(service="svc")
    payload = json.dumps({"event": "e", "message": "m", "level": "INFO"})
    result = formatter.format(_make_record(payload))
    assert json.loads(result) == json.loads(payload)


# --- python_log fallback ---


def test_plain_text_produces_python_log_event():
    formatter = JsonLineFormatter(service="svc")
    result = formatter.format(_make_record("hello world"))
    parsed = json.loads(result)
    assert parsed["event"] == "python_log"


def test_plain_text_message_preserved_in_python_log():
    formatter = JsonLineFormatter(service="svc")
    result = formatter.format(_make_record("some plain text"))
    assert json.loads(result)["message"] == "some plain text"


def test_python_log_level_comes_from_record():
    formatter = JsonLineFormatter(service="svc")
    result = formatter.format(_make_record("msg", level=logging.WARNING))
    assert json.loads(result)["level"] == "WARNING"


def test_python_log_service_comes_from_constructor():
    formatter = JsonLineFormatter(service="my-service")
    result = formatter.format(_make_record("msg"))
    assert json.loads(result)["service"] == "my-service"


def test_python_log_component_comes_from_record_name():
    formatter = JsonLineFormatter(service="svc")
    result = formatter.format(_make_record("msg", name="pipeline.worker"))
    assert json.loads(result)["component"] == "pipeline.worker"


def test_non_dict_json_falls_through_to_python_log():
    formatter = JsonLineFormatter(service="svc")
    result = formatter.format(_make_record(json.dumps([1, 2, 3])))
    assert json.loads(result)["event"] == "python_log"


def test_json_string_scalar_falls_through_to_python_log():
    formatter = JsonLineFormatter(service="svc")
    result = formatter.format(_make_record('"just a string"'))
    assert json.loads(result)["event"] == "python_log"


# --- log_formatter_error ---


def test_unexpected_exception_produces_formatter_error_event():
    def boom(_message):
        raise RuntimeError("unexpected failure")

    formatter = JsonLineFormatter(service="svc", _json_loads=boom)
    result = formatter.format(_make_record("msg"))
    parsed = json.loads(result)
    assert parsed["event"] == "log_formatter_error"


def test_formatter_error_level_is_error():
    def boom(_message):
        raise RuntimeError("oops")

    formatter = JsonLineFormatter(service="svc", _json_loads=boom)
    result = formatter.format(_make_record("msg"))
    assert json.loads(result)["level"] == "ERROR"


def test_formatter_error_message_contains_exception_description():
    def boom(_message):
        raise RuntimeError("something went wrong")

    formatter = JsonLineFormatter(service="svc", _json_loads=boom)
    result = formatter.format(_make_record("msg"))
    assert "something went wrong" in json.loads(result)["message"]


def test_formatter_error_service_comes_from_constructor():
    def boom(_message):
        raise ValueError("bad")

    formatter = JsonLineFormatter(service="my-service", _json_loads=boom)
    result = formatter.format(_make_record("msg"))
    assert json.loads(result)["service"] == "my-service"


def test_formatter_error_component_comes_from_record_name():
    def boom(_message):
        raise ValueError("bad")

    formatter = JsonLineFormatter(service="svc", _json_loads=boom)
    result = formatter.format(_make_record("msg", name="pipeline.formatter"))
    assert json.loads(result)["component"] == "pipeline.formatter"


def test_formatter_error_result_is_valid_json():
    def boom(_message):
        raise TypeError("not a string")

    formatter = JsonLineFormatter(service="svc", _json_loads=boom)
    result = formatter.format(_make_record("msg"))
    parsed = json.loads(result)
    assert isinstance(parsed, dict)

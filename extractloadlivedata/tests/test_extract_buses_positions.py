import pytest

from src.services.extract_buses_positions import (
    buses_positions_response_is_valid,
    extract_buses_positions,
    extract_buses_positions_with_retries,
    get_buses_positions_summary,
)
from src.services.exceptions import PositionsDownloadError
from tests.fakes.http_session import FakeHttpSession
from tests.fakes.sleep import FakeSleep


def test_buses_positions_response_is_valid_ok():
    payload = {"hr": "09:00", "l": []}
    assert buses_positions_response_is_valid(payload) is True


def test_buses_positions_response_is_valid_missing_fields():
    payload = {"hr": "09:00"}
    assert buses_positions_response_is_valid(payload) is False


def test_get_buses_positions_summary_counts():
    payload = {
        "hr": "09:00",
        "l": [{"vs": [{}, {}]}, {"vs": [{}]}],
    }
    reference_time, total_vehicles = get_buses_positions_summary(payload)
    assert reference_time == "09:00"
    assert total_vehicles == 3


def test_extract_buses_positions_auth_failure():
    session = FakeHttpSession(auth_ok=False)
    result = extract_buses_positions("http://api", "token", session=session)
    assert result is None


def test_extract_buses_positions_success():
    payload = {"hr": "09:00", "l": []}
    session = FakeHttpSession(auth_ok=True, pos_ok=True, payload=payload)
    result = extract_buses_positions("http://api", "token", session=session)
    assert result == payload


def test_extract_buses_positions_with_retries_success():
    payload = {"hr": "09:00", "l": []}
    session = FakeHttpSession(auth_ok=True, pos_ok=True, payload=payload)
    fake_sleep = FakeSleep()
    config = {
        "TOKEN": "token",
        "API_BASE_URL": "http://api",
        "API_MAX_RETRIES": 3,
    }
    result = extract_buses_positions_with_retries(
        config, session=session, sleep_fn=fake_sleep
    )
    assert result == payload
    assert fake_sleep.calls == []


def test_extract_buses_positions_with_retries_success_with_metrics():
    payload = {"hr": "09:00", "l": []}
    session = FakeHttpSession(auth_ok=True, pos_ok=True, payload=payload)
    fake_sleep = FakeSleep()
    config = {
        "TOKEN": "token",
        "API_BASE_URL": "http://api",
        "API_MAX_RETRIES": 3,
    }
    result = extract_buses_positions_with_retries(
        config, session=session, sleep_fn=fake_sleep, with_metrics=True
    )
    assert result["result"] == payload
    assert result["metrics"]["retries"] == 0


def test_extract_buses_positions_with_retries_max():
    session = FakeHttpSession(auth_ok=True, pos_ok=False)
    fake_sleep = FakeSleep()
    config = {
        "TOKEN": "token",
        "API_BASE_URL": "http://api",
        "API_MAX_RETRIES": 2,
    }
    with pytest.raises(PositionsDownloadError, match="max retries reached") as exc_info:
        extract_buses_positions_with_retries(
            config, session=session, sleep_fn=fake_sleep
        )
    assert fake_sleep.calls == [1, 2]
    assert getattr(exc_info.value, "retries", None) == 2


def test_extract_buses_positions_with_retries_max_on_connection_error():
    session = FakeHttpSession(raise_on_post=ConnectionError("network unreachable"))
    fake_sleep = FakeSleep()
    config = {
        "TOKEN": "token",
        "API_BASE_URL": "http://api",
        "API_MAX_RETRIES": 2,
    }
    with pytest.raises(PositionsDownloadError, match="max retries reached") as exc_info:
        extract_buses_positions_with_retries(
            config, session=session, sleep_fn=fake_sleep
        )
    assert fake_sleep.calls == [1, 2]
    assert getattr(exc_info.value, "retries", None) == 2

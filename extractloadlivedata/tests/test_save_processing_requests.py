from datetime import datetime

from src.services.save_processing_requests import (
    create_pending_processing_request,
    get_pending_processing_requests,
    get_utc_logical_date_from_file,
    remove_pending_processing_request,
    save_processing_request,
    trigger_pending_processing_requests,
)
from tests.fakes.cache import fake_cache_factory


def test_get_utc_logical_date_from_file():
    dt = get_utc_logical_date_from_file("posicoes_onibus-202604090910.json")
    assert isinstance(dt, datetime)
    assert dt.tzinfo is not None


def test_cache_create_get_remove_flow():
    config = {"PROCESSING_REQUESTS_CACHE_DIR": "/tmp/cache"}
    marker = "posicoes_onibus-202604090910.json"
    create_pending_processing_request(
        config, marker, cache_factory=fake_cache_factory
    )
    pending = get_pending_processing_requests(
        config, cache_factory=fake_cache_factory
    )
    assert len(pending) == 1
    remove_pending_processing_request(
        config, pending[0], cache_factory=fake_cache_factory
    )
    pending_after = get_pending_processing_requests(
        config, cache_factory=fake_cache_factory
    )
    assert pending_after == []


def test_save_processing_request_success():
    config = {
        "RAW_EVENTS_TABLE_NAME": "to_be_processed.raw",
        "DB_HOST": "localhost",
        "DB_PORT": 5432,
        "DB_DATABASE": "db",
        "DB_USER": "user",
        "DB_PASSWORD": "pass",
    }

    def fake_save_row(*_args, **_kwargs):
        return True

    def fake_engine_factory(_uri):
        return None

    result = save_processing_request(
        config,
        "posicoes_onibus-202604090910.json",
        save_row_fn=fake_save_row,
        engine_factory=fake_engine_factory,
    )
    assert result is True


def test_save_processing_request_failure():
    config = {
        "RAW_EVENTS_TABLE_NAME": "to_be_processed.raw",
        "DB_HOST": "localhost",
        "DB_PORT": 5432,
        "DB_DATABASE": "db",
        "DB_USER": "user",
        "DB_PASSWORD": "pass",
    }

    def fake_save_row(*_args, **_kwargs):
        return False

    def fake_engine_factory(_uri):
        return None

    result = save_processing_request(
        config,
        "posicoes_onibus-202604090910.json",
        save_row_fn=fake_save_row,
        engine_factory=fake_engine_factory,
    )
    assert result is False


def test_trigger_pending_processing_requests_removes_on_success():
    config = {"PROCESSING_REQUESTS_CACHE_DIR": "/tmp/cache"}
    marker = "posicoes_onibus-202604090910.json"

    create_pending_processing_request(
        config, marker, cache_factory=fake_cache_factory
    )

    def fake_save_fn(_config, _marker):
        return True

    trigger_pending_processing_requests(
        config, cache_factory=fake_cache_factory, save_fn=fake_save_fn
    )
    pending_after = get_pending_processing_requests(
        config, cache_factory=fake_cache_factory
    )
    assert pending_after == []


def test_trigger_pending_processing_requests_keeps_on_failure():
    config = {"PROCESSING_REQUESTS_CACHE_DIR": "/tmp/cache"}
    marker = "posicoes_onibus-202604090910.json"

    create_pending_processing_request(
        config, marker, cache_factory=fake_cache_factory
    )

    def fake_save_fn(_config, _marker):
        return False

    trigger_pending_processing_requests(
        config, cache_factory=fake_cache_factory, save_fn=fake_save_fn
    )
    pending_after = get_pending_processing_requests(
        config, cache_factory=fake_cache_factory
    )
    assert pending_after != []

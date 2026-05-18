import pytest

from transformlivedata.services.ingest_requests_processor import (
    get_unprocessed_requests,
    mark_request_as_processed,
    mark_request_as_processed_by_filename,
)


def make_config():
    return {
        "general": {
            "tables": {
                "raw_events_table_name": "to_be_processed.raw",
            },
        },
        "connections": {
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "testdb",
                "user": "user",
                "password": "pass",
            }
        },
    }


# --- get_unprocessed_requests ---


def test_get_unprocessed_returns_results():
    def fake_select(connection, query):
        return [{"id": 1, "filename": "f.json", "processed": False}]

    result = get_unprocessed_requests(make_config(), select_fn=fake_select)
    assert len(result) == 1
    assert result[0]["filename"] == "f.json"


def test_get_unprocessed_returns_empty_list_when_none():
    def fake_select(connection, query):
        return []

    result = get_unprocessed_requests(make_config(), select_fn=fake_select)
    assert result == []


def test_get_unprocessed_raises_on_exception():
    def fake_select(connection, query):
        raise RuntimeError("db down")

    with pytest.raises(RuntimeError, match="db down"):
        get_unprocessed_requests(make_config(), select_fn=fake_select)


def test_get_unprocessed_missing_table_config_raises_key_error():
    config = make_config()
    del config["general"]["tables"]["raw_events_table_name"]
    with pytest.raises(KeyError, match="raw_events_table_name"):
        get_unprocessed_requests(config)


def test_get_unprocessed_table_name_without_dot_raises_value_error():
    config = make_config()
    config["general"]["tables"]["raw_events_table_name"] = "nodot"
    with pytest.raises(ValueError, match="schema.table"):
        get_unprocessed_requests(config)


# --- mark_request_as_processed ---


def test_mark_as_processed_returns_true_on_success():
    def fake_update(connection, query, params):
        return True

    result = mark_request_as_processed(
        make_config(), "2026-02-15T10:00:00", update_fn=fake_update
    )
    assert result is True


def test_mark_as_processed_returns_false_on_failure():
    def fake_update(connection, query, params):
        return False

    result = mark_request_as_processed(
        make_config(), "2026-02-15T10:00:00", update_fn=fake_update
    )
    assert result is False


def test_mark_as_processed_raises_on_exception():
    def fake_update(connection, query, params):
        raise RuntimeError("db error")

    with pytest.raises(RuntimeError, match="db error"):
        mark_request_as_processed(
            make_config(), "2026-02-15T10:00:00", update_fn=fake_update
        )


def test_mark_as_processed_passes_logical_date_as_param():
    calls = []

    def fake_update(connection, query, params):
        calls.append(params)
        return True

    mark_request_as_processed(
        make_config(), "2026-02-15T10:00:00", update_fn=fake_update
    )
    assert calls[0]["logical_date"] == "2026-02-15T10:00:00"


# --- mark_request_as_processed_by_filename ---


def test_mark_by_filename_returns_true_on_success():
    def fake_update(connection, query, params):
        return True

    result = mark_request_as_processed_by_filename(
        make_config(), "file.json", update_fn=fake_update
    )
    assert result is True


def test_mark_by_filename_passes_filename_as_param():
    calls = []

    def fake_update(connection, query, params):
        calls.append(params)
        return True

    mark_request_as_processed_by_filename(
        make_config(), "file.json", update_fn=fake_update
    )
    assert calls[0]["filename"] == "file.json"


def test_mark_by_filename_raises_on_exception():
    def fake_update(connection, query, params):
        raise RuntimeError("db error")

    with pytest.raises(RuntimeError, match="db error"):
        mark_request_as_processed_by_filename(
            make_config(), "file.json", update_fn=fake_update
        )

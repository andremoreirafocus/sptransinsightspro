import pytest
from updatelatestpositions.services.get_latest_path_for_query import (
    get_latest_path_for_query,
)
from fakes.fake_object_storage import FakeObject


def make_config():
    return {
        "general": {
            "storage": {
                "trusted_bucket": "trusted",
                "app_folder": "sptrans",
            },
            "tables": {
                "positions_table_name": "positions",
                "latest_positions_table_name": "refined.latest_positions",
            },
        },
        "connections": {
            "object_storage": {
                "endpoint": "localhost",
                "access_key": "key",
                "secret_key": "secret",
            }
        },
    }


def test_returns_s3_path_when_parquet_found():
    def fake_list(connection, bucket, prefix):
        return [FakeObject(object_name=f"{prefix}data.parquet")]

    result = get_latest_path_for_query(make_config(), list_objects_fn=fake_list)
    assert result is not None
    assert result.startswith("s3://trusted/")
    assert result.endswith(".parquet")


def test_returns_none_when_no_parquet_found():
    def fake_list(connection, bucket, prefix):
        return []

    result = get_latest_path_for_query(make_config(), list_objects_fn=fake_list)
    assert result is None


def test_ignores_non_parquet_files():
    def fake_list(connection, bucket, prefix):
        return [FakeObject(object_name=f"{prefix}data.json")]

    result = get_latest_path_for_query(make_config(), list_objects_fn=fake_list)
    assert result is None


def test_returns_last_sorted_parquet_when_multiple_found():
    def fake_list(connection, bucket, prefix):
        return [
            FakeObject(object_name=f"{prefix}a.parquet"),
            FakeObject(object_name=f"{prefix}c.parquet"),
            FakeObject(object_name=f"{prefix}b.parquet"),
        ]

    result = get_latest_path_for_query(make_config(), list_objects_fn=fake_list)
    assert result is not None
    assert "c.parquet" in result


def test_stops_searching_after_first_hit():
    call_count = {"n": 0}

    def fake_list(connection, bucket, prefix):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return [FakeObject(object_name=f"{prefix}data.parquet")]
        return []

    get_latest_path_for_query(make_config(), list_objects_fn=fake_list)
    assert call_count["n"] == 1


def test_tries_second_hour_when_first_empty():
    call_count = {"n": 0}

    def fake_list(connection, bucket, prefix):
        call_count["n"] += 1
        if call_count["n"] == 2:
            return [FakeObject(object_name=f"{prefix}data.parquet")]
        return []

    result = get_latest_path_for_query(make_config(), list_objects_fn=fake_list)
    assert call_count["n"] == 2
    assert result is not None


def test_list_objects_failure_raises_value_error():
    def fake_list(connection, bucket, prefix):
        raise RuntimeError("connection refused")

    with pytest.raises(ValueError, match="Object storage scan failed for bucket 'trusted' prefix"):
        get_latest_path_for_query(make_config(), list_objects_fn=fake_list)


def test_path_discovery_empty_emits_warning(caplog):
    import logging

    def fake_list(connection, bucket, prefix):
        return []

    with caplog.at_level(logging.WARNING):
        get_latest_path_for_query(make_config(), list_objects_fn=fake_list)

    records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert any("path_discovery_empty" in r.getMessage() for r in records)



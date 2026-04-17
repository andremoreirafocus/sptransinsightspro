import io
import pytest
from gtfs.services.load_raw_csv_to_buffer_from_storage import (
    load_raw_csv_to_buffer_from_storage,
)


def make_config():
    return {
        "general": {
            "storage": {
                "raw_bucket": "raw",
                "gtfs_folder": "gtfs",
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


def test_returns_buffer_from_storage():
    expected = io.BytesIO(b"stop_id,stop_name\n1,Central")

    def fake_read(connection, bucket, object_name):
        return expected

    result = load_raw_csv_to_buffer_from_storage(
        make_config(), "stops", read_fn=fake_read
    )
    assert result is expected


def test_object_name_uses_correct_path():
    calls = []

    def fake_read(connection, bucket, object_name):
        calls.append((bucket, object_name))
        return io.BytesIO(b"")

    load_raw_csv_to_buffer_from_storage(make_config(), "stops", read_fn=fake_read)
    bucket, object_name = calls[0]
    assert bucket == "raw"
    assert object_name == "gtfs/stops.txt"


def test_missing_config_key_raises_value_error():
    config = make_config()
    del config["general"]["storage"]["raw_bucket"]
    with pytest.raises(ValueError, match="Missing required configuration key"):
        load_raw_csv_to_buffer_from_storage(config, "stops")

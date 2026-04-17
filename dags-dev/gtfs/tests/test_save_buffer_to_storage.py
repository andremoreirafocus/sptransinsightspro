import pytest
from gtfs.services.save_buffer_to_storage import save_buffer_to_storage


def make_config():
    return {
        "general": {
            "storage": {
                "trusted_bucket": "trusted",
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


def test_write_called_with_correct_object_name():
    calls = []

    def fake_write(connection, buffer, bucket_name, object_name):
        calls.append((bucket_name, object_name))

    save_buffer_to_storage(make_config(), "stops.parquet", b"data", write_fn=fake_write)
    bucket, object_name = calls[0]
    assert bucket == "trusted"
    assert object_name == "gtfs/stops/stops.parquet"


def test_write_called_with_correct_buffer():
    calls = []

    def fake_write(connection, buffer, bucket_name, object_name):
        calls.append(buffer)

    save_buffer_to_storage(
        make_config(), "stops.parquet", b"parquet-bytes", write_fn=fake_write
    )
    assert calls[0] == b"parquet-bytes"


def test_missing_config_key_raises_value_error():
    config = make_config()
    del config["general"]["storage"]["trusted_bucket"]
    with pytest.raises(ValueError, match="Missing required configuration key"):
        save_buffer_to_storage(config, "stops.parquet", b"data")


def test_write_error_raises_value_error():
    def fake_write(connection, buffer, bucket_name, object_name):
        raise RuntimeError("write failed")

    with pytest.raises(ValueError, match="Failed to save parquet buffer"):
        save_buffer_to_storage(
            make_config(), "stops.parquet", b"data", write_fn=fake_write
        )

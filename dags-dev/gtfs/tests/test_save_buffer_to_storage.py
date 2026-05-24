import io
import json

import pytest
from gtfs.services.save_buffer_to_storage import save_buffer_to_storage


def _parse_log_events(caplog, event_name: str) -> list[dict]:
    results = []
    for record in caplog.records:
        try:
            parsed = json.loads(record.getMessage())
        except Exception:
            continue
        if parsed.get("event") == event_name:
            results.append(parsed)
    return results


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


def test_missing_config_key_raises_key_error():
    config = make_config()
    del config["general"]["storage"]["trusted_bucket"]
    with pytest.raises(KeyError, match="trusted_bucket"):
        save_buffer_to_storage(config, "stops.parquet", b"data")


def test_write_error_raises_value_error():
    def fake_write(connection, buffer, bucket_name, object_name):
        raise RuntimeError("write failed")

    with pytest.raises(
        ValueError,
        match="Error writing data to object storage while saving parquet buffer",
    ):
        save_buffer_to_storage(
            make_config(), "stops.parquet", b"data", write_fn=fake_write
        )


def test_write_to_explicit_subfolder():
    calls = []

    def fake_write(connection, buffer, bucket_name, object_name):
        calls.append(object_name)

    save_buffer_to_storage(
        make_config(),
        "stops.parquet",
        b"data",
        subfolder="staging",
        write_fn=fake_write,
    )
    assert calls[0] == "gtfs/staging/stops.parquet"


def test_bytes_written_is_logged_for_bytesio_buffer(caplog):
    caplog.set_level("INFO")
    payload = b"parquet-data-here"
    buffer = io.BytesIO(payload)

    def fake_write(connection, buffer, bucket_name, object_name):
        pass

    save_buffer_to_storage(make_config(), "stops.parquet", buffer, write_fn=fake_write)

    events = _parse_log_events(caplog, "buffer_save_succeeded")
    assert len(events) == 1
    assert events[0]["metadata"]["bytes_written"] == len(payload)


def test_bytes_written_is_logged_for_bytes_buffer(caplog):
    caplog.set_level("INFO")
    payload = b"raw-bytes"

    def fake_write(connection, buffer, bucket_name, object_name):
        pass

    save_buffer_to_storage(make_config(), "stops.parquet", payload, write_fn=fake_write)

    events = _parse_log_events(caplog, "buffer_save_succeeded")
    assert len(events) == 1
    assert events[0]["metadata"]["bytes_written"] == len(payload)

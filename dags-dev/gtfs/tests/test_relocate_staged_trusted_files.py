import pytest

from gtfs.services.relocate_staged_trusted_files import (
    _move_object_with_minio,
    relocate_staged_trusted_files,
)
from gtfs.tests.fakes.fake_minio import FakeMinioClient


def make_config():
    return {
        "general": {
            "storage": {
                "trusted_bucket": "trusted",
                "gtfs_folder": "gtfs",
                "quarantined_subfolder": "quarantined",
            }
        },
        "connections": {
            "object_storage": {
                "endpoint": "localhost",
                "access_key": "key",
                "secret_key": "secret",
            }
        },
    }


def test_relocate_staged_files_to_quarantine():
    calls = []

    def fake_move(connection, bucket_name, source_object_name, destination_object_name):
        calls.append((bucket_name, source_object_name, destination_object_name))

    result = relocate_staged_trusted_files(
        make_config(),
        [
            {
                "table_name": "stops",
                "staging_object_name": "gtfs/staging/stops.parquet",
            }
        ],
        target="quarantine",
        move_fn=fake_move,
    )

    assert result["status"] == "SUCCESS"
    assert calls[0][0] == "trusted"
    assert calls[0][2] == "gtfs/quarantined/stops.parquet"


def test_relocate_staged_files_to_final():
    calls = []

    def fake_move(connection, bucket_name, source_object_name, destination_object_name):
        calls.append((bucket_name, source_object_name, destination_object_name))

    result = relocate_staged_trusted_files(
        make_config(),
        [
            {
                "table_name": "calendar",
                "staging_object_name": "gtfs/staging/calendar.parquet",
            }
        ],
        target="final",
        move_fn=fake_move,
    )

    assert result["status"] == "SUCCESS"
    assert calls[0][2] == "gtfs/calendar/calendar.parquet"


def test_relocate_staged_files_returns_failed_with_partial_errors():
    calls = []

    def fake_move(connection, bucket_name, source_object_name, destination_object_name):
        calls.append((bucket_name, source_object_name, destination_object_name))
        if source_object_name.endswith("stops.parquet"):
            raise RuntimeError("boom")

    result = relocate_staged_trusted_files(
        make_config(),
        [
            {
                "table_name": "stops",
                "staging_object_name": "gtfs/staging/stops.parquet",
            },
            {
                "table_name": "calendar",
                "staging_object_name": "gtfs/staging/calendar.parquet",
            },
        ],
        target="quarantine",
        move_fn=fake_move,
    )

    assert result["status"] == "FAILED"
    assert len(result["errors"]) == 1
    assert result["errors"][0]["table_name"] == "stops"
    assert len(result["moved"]) == 1
    assert result["moved"][0]["table_name"] == "calendar"


def test_relocate_staged_files_rejects_invalid_target():
    with pytest.raises(ValueError, match="Invalid relocation target"):
        relocate_staged_trusted_files(
            make_config(),
            [{"table_name": "stops", "staging_object_name": "gtfs/staging/stops.parquet"}],
            target="invalid",
        )


def test_relocate_staged_files_raises_for_missing_required_config():
    config = make_config()
    del config["general"]["storage"]["quarantined_subfolder"]
    with pytest.raises(ValueError, match="Missing required configuration key"):
        relocate_staged_trusted_files(
            config,
            [{"table_name": "stops", "staging_object_name": "gtfs/staging/stops.parquet"}],
            target="quarantine",
        )


def test_move_object_with_minio_reads_writes_and_removes():
    calls = {"response_closed": False, "response_released": False}
    FakeMinioClient.configure(calls, payload=b"abc123")

    _move_object_with_minio(
        {
            "endpoint": "localhost:9000",
            "access_key": "ak",
            "secret_key": "sk",
            "secure": False,
        },
        "trusted",
        "gtfs/staging/stops.parquet",
        "gtfs/quarantined/stops.parquet",
        client_factory=FakeMinioClient,
    )

    assert calls["init"]["endpoint"] == "localhost:9000"
    assert calls["get_object"] == ("trusted", "gtfs/staging/stops.parquet")
    assert calls["put_object"]["bucket_name"] == "trusted"
    assert calls["put_object"]["object_name"] == "gtfs/quarantined/stops.parquet"
    assert calls["put_object"]["length"] == 6
    assert calls["put_object"]["payload"] == b"abc123"
    assert calls["remove_object"] == ("trusted", "gtfs/staging/stops.parquet")
    assert calls["response_closed"] is True
    assert calls["response_released"] is True

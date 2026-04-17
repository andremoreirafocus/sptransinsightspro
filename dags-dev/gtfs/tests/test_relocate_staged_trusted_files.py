from gtfs.services.relocate_staged_trusted_files import relocate_staged_trusted_files


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

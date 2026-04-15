import pytest
from gtfs.services.save_files_to_raw_storage import save_files_to_raw_storage


def make_config():
    return {
        "general": {
            "extraction": {
                "local_downloads_folder": "/tmp/gtfs",
            },
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


def make_fake_open(file_data=b"content"):
    class FakeFile:
        def __init__(self):
            self.data = file_data

        def read(self):
            return self.data

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    def fake_open(path, mode):
        return FakeFile()

    return fake_open


def test_write_called_once_per_file():
    write_calls = []

    def fake_write(connection, buffer, bucket_name, object_name):
        write_calls.append(object_name)

    save_files_to_raw_storage(
        make_config(),
        ["stops.txt", "routes.txt"],
        read_file_fn=make_fake_open(),
        write_fn=fake_write,
    )
    assert len(write_calls) == 2


def test_object_name_formatted_correctly():
    write_calls = []

    def fake_write(connection, buffer, bucket_name, object_name):
        write_calls.append(object_name)

    save_files_to_raw_storage(
        make_config(),
        ["stops.txt"],
        read_file_fn=make_fake_open(),
        write_fn=fake_write,
    )
    assert write_calls[0] == "gtfs/stops/stops.txt"


def test_correct_bucket_used():
    write_calls = []

    def fake_write(connection, buffer, bucket_name, object_name):
        write_calls.append(bucket_name)

    save_files_to_raw_storage(
        make_config(),
        ["stops.txt"],
        read_file_fn=make_fake_open(),
        write_fn=fake_write,
    )
    assert write_calls[0] == "raw"


def test_file_content_passed_to_write():
    write_calls = []

    def fake_write(connection, buffer, bucket_name, object_name):
        write_calls.append(buffer)

    save_files_to_raw_storage(
        make_config(),
        ["stops.txt"],
        read_file_fn=make_fake_open(b"abc123"),
        write_fn=fake_write,
    )
    assert write_calls[0] == b"abc123"


def test_missing_config_key_raises_value_error():
    config = make_config()
    del config["general"]["storage"]["raw_bucket"]
    with pytest.raises(ValueError, match="Missing required configuration key"):
        save_files_to_raw_storage(config, ["stops.txt"])


def test_empty_files_list_does_nothing():
    write_calls = []

    def fake_write(connection, buffer, bucket_name, object_name):
        write_calls.append(True)

    save_files_to_raw_storage(
        make_config(),
        [],
        read_file_fn=make_fake_open(),
        write_fn=fake_write,
    )
    assert write_calls == []

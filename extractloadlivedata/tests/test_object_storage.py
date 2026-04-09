import io

from src.infra.object_storage import (
    list_objects_in_object_storage_bucket,
    read_file_from_object_storage,
    write_generic_bytes_to_object_storage,
)
from tests.fakes.minio_client import FakeMinioClient


def test_write_generic_bytes_to_object_storage_creates_bucket_and_puts_bytes():
    client = FakeMinioClient(bucket_exists=False)
    write_generic_bytes_to_object_storage(
        connection_data={"minio_endpoint": "", "access_key": "", "secret_key": "", "secure": False},
        buffer=b"data",
        bucket_name="raw",
        object_name="path/file.json",
        client=client,
    )
    assert client.created_bucket is True
    assert len(client.put_calls) == 1
    call = client.put_calls[0]
    assert call["bucket_name"] == "raw"
    assert call["object_name"] == "path/file.json"


def test_write_generic_bytes_to_object_storage_accepts_bytesio():
    client = FakeMinioClient()
    write_generic_bytes_to_object_storage(
        connection_data={"minio_endpoint": "", "access_key": "", "secret_key": "", "secure": False},
        buffer=io.BytesIO(b"data"),
        bucket_name="raw",
        object_name="path/file.json",
        client=client,
    )
    assert len(client.put_calls) == 1


def test_write_generic_bytes_to_object_storage_rejects_invalid_buffer():
    client = FakeMinioClient()
    try:
        write_generic_bytes_to_object_storage(
            connection_data={"minio_endpoint": "", "access_key": "", "secret_key": "", "secure": False},
            buffer="invalid",
            bucket_name="raw",
            object_name="path/file.json",
            client=client,
        )
    except ValueError:
        assert True
    else:
        assert False, "Expected ValueError for invalid buffer"


def test_list_objects_in_object_storage_bucket_returns_names():
    client = FakeMinioClient(list_objects=["a.txt", "b.txt"])
    result = list_objects_in_object_storage_bucket(
        connection_data={"minio_endpoint": "", "access_key": "", "secret_key": "", "secure": False},
        bucket_name="raw",
        prefix="",
        client=client,
    )
    assert result == ["a.txt", "b.txt"]


def test_list_objects_in_object_storage_bucket_returns_empty_on_error():
    client = FakeMinioClient(raise_on_list=True)
    result = list_objects_in_object_storage_bucket(
        connection_data={"minio_endpoint": "", "access_key": "", "secret_key": "", "secure": False},
        bucket_name="raw",
        prefix="",
        client=client,
    )
    assert result == []


def test_read_file_from_object_storage_returns_content():
    client = FakeMinioClient()
    result = read_file_from_object_storage(
        connection_data={"minio_endpoint": "", "access_key": "", "secret_key": "", "secure": False},
        bucket_name="raw",
        object_name="file.json",
        client=client,
    )
    assert result == "file-content"
    assert client.get_calls == ["file.json"]


def test_read_file_from_object_storage_returns_none_on_error():
    client = FakeMinioClient(raise_on_get=True)
    result = read_file_from_object_storage(
        connection_data={"minio_endpoint": "", "access_key": "", "secret_key": "", "secure": False},
        bucket_name="raw",
        object_name="file.json",
        client=client,
    )
    assert result is None

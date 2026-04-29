from minio import Minio
import io
import logging
from typing import Any, Dict, List, Optional, Union

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)

client_factory = Minio


def _get_object_storage_client(connection_data: Dict[str, Any], client: Optional[Any] = None) -> Any:
    return client or client_factory(
        connection_data["minio_endpoint"],
        access_key=connection_data["access_key"],
        secret_key=connection_data["secret_key"],
        secure=connection_data["secure"],
    )


def list_objects_in_object_storage_bucket(
    connection_data: Dict[str, Any],
    bucket_name: str,
    prefix: str,
    client: Optional[Any] = None,
) -> List[str]:
    """
    Lists files in a MinIO folder (prefix).
    :param bucket_name: MinIO bucket name
    :param prefix: Folder path (prefix) in the bucket
    :param connection_data: MinIO connection data
    :return: List of file object names
    """

    try:
        client = _get_object_storage_client(connection_data, client)
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
    except Exception as e:
        logger.error(f"Error listing files in MinIO folder: {e}")
        return []


def read_file_from_object_storage(
    connection_data: Dict[str, Any],
    bucket_name: str,
    object_name: str,
    client: Optional[Any] = None,
) -> Optional[str]:
    """
    Reads a file from MinIO and returns its contents as a string.
    :param connection data: MinIO connection data
    :param bucket_name: MinIO bucket name
    :param object_name: Object name for the JSON file in MinIO
    :return: file content as a string
    """
    try:
        client = _get_object_storage_client(connection_data, client)
        response = client.get_object(bucket_name, object_name)
        content = response.read().decode("utf-8")
        response.close()
        response.release_conn()
        return content
    except Exception as e:
        logger.error(f"Error reading JSON from MinIO: {e}")
        return None


def write_generic_bytes_to_object_storage(
    connection_data: Dict[str, Any],
    buffer: Union[bytes, io.BytesIO],
    bucket_name: str,
    object_name: str,
    client: Optional[Any] = None,
) -> None:
    """
    Writes a io bytes buffer (such as from Pandas) to MinIO at the specified bucket and object name.
    :param connection data: MinIO connection data
    :param bucket_name: MinIO bucket name
    :param object_name: Object name for the JSON file in MinIO
    :return: void
    """
    client = _get_object_storage_client(connection_data, client)

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    if isinstance(buffer, bytes):
        data_stream = io.BytesIO(buffer)
        data_length = len(buffer)
    elif isinstance(buffer, io.BytesIO):
        data_stream = buffer
        # Go to start of stream if it has been read before
        data_stream.seek(0)
        data_length = buffer.getbuffer().nbytes
    else:
        raise ValueError("Buffer must be either bytes or io.BytesIO")
    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=data_stream,
        length=data_length,
        content_type="application/octet-stream",
    )
    logger.info(
        f"Consolidated file uploaded to bucket '{bucket_name}' as '{object_name}'"
    )

from minio import Minio
import io
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)

client_factory = Minio


def _get_object_storage_client(connection, client=None):
    return client or client_factory(
        connection["endpoint"],
        access_key=connection["access_key"],
        secret_key=connection["secret_key"],
        secure=connection["secure"],
    )


def list_objects_in_object_storage_bucket(
    connection,
    bucket_name,
    prefix,
    client=None,
):
    """
    Lists files in an object storage bucket (prefix).
    :param bucket_name: Object storage bucket name
    :param prefix: Folder path (prefix) in the bucket
    :param connection: Object storage connection data
    :return: Iterable of file objects
    """

    try:
        client = _get_object_storage_client(connection, client)
        objects = client.list_objects(
            bucket_name=bucket_name, prefix=prefix, recursive=True
        )
        return objects
    except Exception as e:
        logger.error(
            f"Error listing files in bucket '{bucket_name}' with prefix '{prefix}': {e}"
        )
        raise



def read_file_from_object_storage_to_str(
    connection, bucket_name, object_name, client=None
):
    """
    Reads a file from object storage and returns its contents as a string.
    :param connection: Object storage connection data
    :param bucket_name: Object storage bucket name
    :param object_name: Object name for the file in object storage
    :return: file content as a string
    """
    try:
        client = _get_object_storage_client(connection, client)
        response = client.get_object(bucket_name, object_name)
        content = response.read().decode("utf-8")
        response.close()
        response.release_conn()
        return content
    except Exception as e:
        logger.error(f"Error reading file from {bucket_name}/{object_name}: {e}")
        raise


def read_file_from_object_storage_to_bytesio(
    connection, bucket_name, object_name, client=None
):
    """
    Reads a file from object storage and returns its contents as a BytesIO buffer.
    :param connection: Object storage connection data
    :param bucket_name: Object storage bucket name
    :param object_name: Object name for the file in object storage
    :return: file content as io.BytesIO
    """
    try:
        client = _get_object_storage_client(connection, client)
        response = client.get_object(bucket_name, object_name)
        content = io.BytesIO(response.read())
        response.close()
        response.release_conn()
        return content
    except Exception as e:
        logger.error(f"Error reading bytes from {bucket_name}/{object_name}: {e}")
        raise


def write_generic_bytes_to_object_storage(
    connection, buffer, bucket_name, object_name, client=None
):
    """
    Writes a io bytes buffer (such as from Pandas) to object storage at the specified bucket and object name.
    :param connection: Object storage connection data
    :param bucket_name: Object storage bucket name
    :param object_name: Object name for the JSON file in object storage
    :return: void
    """
    try:
        client = _get_object_storage_client(connection, client)

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
    except Exception as e:
        logger.error(
            "Error writing bytes to object storage "
            f"(bucket='{bucket_name}', object='{object_name}'): {e}"
        )
        raise

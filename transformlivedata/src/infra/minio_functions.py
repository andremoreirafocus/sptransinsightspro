from minio import Minio
import io
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def list_objects_in_minio_bucket(
    connection_data,
    bucket_name,
    prefix,
):
    """
    Lists files in a MinIO folder (prefix).
    :param bucket_name: MinIO bucket name
    :param prefix: Folder path (prefix) in the bucket
    :param connection_data: MinIO connection data
    :return: List of file object names
    """

    try:
        client = Minio(
            connection_data["minio_endpoint"],
            access_key=connection_data["access_key"],
            secret_key=connection_data["secret_key"],
            secure=connection_data["secure"],
        )
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]
    except Exception as e:
        print(f"Error listing files in MinIO folder: {e}")
        return []


def read_file_from_minio(connection_data, bucket_name, object_name):
    """
    Reads a file from MinIO and returns its contents as a string.
    :param connection data: MinIO connection data
    :param bucket_name: MinIO bucket name
    :param object_name: Object name for the JSON file in MinIO
    :return: file content as a string
    """
    try:
        client = Minio(
            connection_data["minio_endpoint"],
            access_key=connection_data["access_key"],
            secret_key=connection_data["secret_key"],
            secure=connection_data["secure"],
        )
        response = client.get_object(bucket_name, object_name)
        content = response.read().decode("utf-8")
        response.close()
        response.release_conn()
        return content
    except Exception as e:
        print(f"Error reading JSON from MinIO: {e}")
        return None


def write_generic_bytes_to_minio(connection_data, buffer, bucket_name, object_name):
    """
    Writes a io bytes buffer (such as from Pandas) to MinIO at the specified bucket and object name.
    :param connection data: MinIO connection data
    :param bucket_name: MinIO bucket name
    :param object_name: Object name for the JSON file in MinIO
    :return: void
    """
    client = Minio(
        connection_data["minio_endpoint"],
        access_key=connection_data["access_key"],
        secret_key=connection_data["secret_key"],
        secure=connection_data["secure"],
    )

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
    print(f"Consolidated file uploaded to bucket '{bucket_name}' as '{object_name}'")

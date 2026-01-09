import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def get_minio_connection_data():
    """
    Creates and returns a dictionary containing MinIO connection data.
    """
    minio_server = "localhost"
    # minio_server = "minio"
    connection_data = {
        "minio_endpoint": f"{minio_server}:9000",
        "access_key": "datalake",
        "secret_key": "datalake",
        "secure": False,
    }

    return connection_data

import os
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def get_config():
    # Check if we are running inside Airflow
    if os.getenv("AIRFLOW_HOME"):
        # Pulling from Airflow Variables
        config = get_airflow_config()
        logger.info("Configuration retrieved for Airflow!")
    else:
        # Pulling from local .env
        from dotenv import dotenv_values

        config = dotenv_values("gtfs/.env")
        logger.info("Configuration retrieved locally!")
    logger.info(f"Config: {config}")
    return config


def get_airflow_config():
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable

    gtfs_conn = BaseHook.get_connection("gtfs_conn")
    gtfs_url = f"{gtfs_conn.conn_type}://{gtfs_conn.host}{gtfs_conn.schema}"
    gtfs_login = gtfs_conn.login
    gtfs_password = gtfs_conn.password
    minio_conn = BaseHook.get_connection("minio_conn")
    minio_endpoint = f"{minio_conn.host}:{minio_conn.port}"
    minio_access_key = minio_conn.login
    minio_secret_key = minio_conn.password
    gtfs_vars = Variable.get("gtfs", deserialize_json=True)
    local_downloads_folder = gtfs_vars["local_downloads_folder"]
    app_folder = gtfs_vars["app_folder"]
    gtfs_folder = gtfs_vars["gtfs_folder"]
    raw_bucket = gtfs_vars["raw_bucket"]
    trusted_bucket = gtfs_vars["trusted_bucket"]
    trip_details_table_name = gtfs_vars["trip_details_table_name"]
    config = {
        "GTFS_URL": gtfs_url,
        "LOGIN": gtfs_login,
        "PASSWORD": gtfs_password,
        "LOCAL_DOWNLOADS_FOLDER": local_downloads_folder,
        "APP_FOLDER": app_folder,
        "GTFS_FOLDER": gtfs_folder,
        "RAW_BUCKET": raw_bucket,
        "TRUSTED_BUCKET": trusted_bucket,
        "TRIP_DETAILS_TABLE_NAME": trip_details_table_name,
        "MINIO_ENDPOINT": minio_endpoint,
        "ACCESS_KEY": minio_access_key,
        "SECRET_KEY": minio_secret_key,
    }
    return config

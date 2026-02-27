import os


def get_config():
    # Check if we are running inside Airflow
    if os.getenv("AIRFLOW_HOME"):
        # Pulling from Airflow Variables
        # from airflow.models import Variable
        config = get_airflow_config()
        return config
    else:
        # Pulling from local .env or hardcoded defaults for testing
        from dotenv import dotenv_values

        return dotenv_values("transformlivedata/.env")


def get_airflow_config():
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable

    minio_conn = BaseHook.get_connection("minio_conn")
    minio_endpoint = f"{minio_conn.host}:{minio_conn.port}"
    minio_access_key = minio_conn.login
    minio_secret_key = minio_conn.password
    transformlivedata_vars = Variable.get("transformlivedata", deserialize_json=True)
    app_folder = transformlivedata_vars["app_folder"]
    gtfs_folder = transformlivedata_vars["gtfs_folder"]
    raw_bucket = transformlivedata_vars["raw_bucket"]
    trusted_bucket = transformlivedata_vars["trusted_bucket"]
    positions_table_name = transformlivedata_vars["positions_table_name"]
    trip_details_table_name = transformlivedata_vars["trip_details_table_name"]
    raw_data_compression = transformlivedata_vars["raw_data_compression"]
    raw_data_compression_extension = transformlivedata_vars[
        "raw_data_compression_extension"
    ]
    raw_events_table_name = transformlivedata_vars["raw_events_table_name"]
    postgres_conn = BaseHook.get_connection("airflow_postgres_conn")
    postgres_host = postgres_conn.host
    postgres_port = postgres_conn.port
    postgres_database = postgres_conn.schema
    postgres_user = postgres_conn.login
    postgres_password = postgres_conn.password
    postgres_sslmode = postgres_conn.extra_dejson.get("sslmode", "prefer")
    config = {
        "RAW_BUCKET": raw_bucket,
        "TRUSTED_BUCKET": trusted_bucket,
        "APP_FOLDER": app_folder,
        "GTFS_FOLDER": gtfs_folder,
        "POSITIONS_TABLE_NAME": positions_table_name,
        "TRIP_DETAILS_TABLE_NAME": trip_details_table_name,
        "MINIO_ENDPOINT": minio_endpoint,
        "ACCESS_KEY": minio_access_key,
        "SECRET_KEY": minio_secret_key,
        "RAW_DATA_COMPRESSION": raw_data_compression,
        "RAW_DATA_COMPRESSION_EXTENSION": raw_data_compression_extension,
        "RAW_EVENTS_TABLE_NAME": raw_events_table_name,
        "DB_HOST": postgres_host,
        "DB_PORT": postgres_port,
        "DB_DATABASE": postgres_database,
        "DB_USER": postgres_user,
        "DB_PASSWORD": postgres_password,
        "DB_SSLMODE": postgres_sslmode,
    }
    return config

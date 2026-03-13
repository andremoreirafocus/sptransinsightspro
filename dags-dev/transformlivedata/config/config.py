import os
import json


def get_config():
    # Check if we are running inside Airflow
    if os.getenv("AIRFLOW_HOME"):
        # Pulling from Airflow Variables
        # from airflow.models import Variable
        return get_airflow_config()
    else:
        # Pulling from local .env or hardcoded defaults for testing
        from dotenv import dotenv_values

        return get_local_config(dotenv_values("transformlivedata/.env"))


def get_airflow_config():
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable

    minio_conn = BaseHook.get_connection("minio_conn")
    minio_endpoint = f"{minio_conn.host}:{minio_conn.port}"
    minio_access_key = minio_conn.login
    minio_secret_key = minio_conn.password
    general_vars = Variable.get("transformlivedata_general", deserialize_json=True)
    raw_data_json_schema = Variable.get(
        "transformlivedata_raw_data_json_schema", deserialize_json=True
    )
    data_expectations = Variable.get(
        "transformlivedata_data_expectations", deserialize_json=True
    )
    app_folder = general_vars["app_folder"]
    gtfs_folder = general_vars["gtfs_folder"]
    raw_bucket = general_vars["raw_bucket"]
    trusted_bucket = general_vars["trusted_bucket"]
    positions_table_name = general_vars["positions_table_name"]
    trip_details_table_name = general_vars["trip_details_table_name"]
    raw_data_compression = general_vars["raw_data_compression"]
    raw_data_compression_extension = general_vars["raw_data_compression_extension"]
    raw_events_table_name = general_vars["raw_events_table_name"]
    metadata_bucket = general_vars["metadata_bucket"]
    quality_report_folder = general_vars["quality_report_folder"]
    quarantined_bucket = general_vars["quarantined_bucket"]
    postgres_conn = BaseHook.get_connection("airflow_postgres_conn")
    postgres_host = postgres_conn.host
    postgres_port = postgres_conn.port
    postgres_database = postgres_conn.schema
    postgres_user = postgres_conn.login
    postgres_password = postgres_conn.password
    postgres_sslmode = postgres_conn.extra_dejson.get("sslmode", "prefer")
    general_config = {
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
        "METADATA_BUCKET": metadata_bucket,
        "QUALITY_REPORT_FOLDER": quality_report_folder,
        "QUARANTINED_BUCKET": quarantined_bucket,
        "DB_HOST": postgres_host,
        "DB_PORT": postgres_port,
        "DB_DATABASE": postgres_database,
        "DB_USER": postgres_user,
        "DB_PASSWORD": postgres_password,
        "DB_SSLMODE": postgres_sslmode,
    }
    return {
        "general": general_config,
        "raw_data_json_schema": raw_data_json_schema,
        "data_expectations": data_expectations,
    }


def get_local_config(env_values):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    raw_schema_path = os.path.join(base_dir, "raw_data_schema_config.json")
    expectations_path = os.path.join(base_dir, "transformed_data_expectations.json")
    with open(raw_schema_path, "r") as f:
        raw_data_json_schema = json.load(f)
    with open(expectations_path, "r") as f:
        data_expectations = json.load(f)
    return {
        "general": dict(env_values),
        "raw_data_json_schema": raw_data_json_schema,
        "data_expectations": data_expectations,
    }

import logging
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

        return dotenv_values("updatelatestpositions/.env")


def get_airflow_config():
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable

    minio_conn = BaseHook.get_connection("minio_conn")
    minio_endpoint = f"{minio_conn.host}:{minio_conn.port}"
    minio_access_key = minio_conn.login
    minio_secret_key = minio_conn.password
    updatelatestpositions_vars = Variable.get(
        "updatelatestpositions", deserialize_json=True
    )
    postgres_conn = BaseHook.get_connection("postgres_conn")
    postgres_host = postgres_conn.host
    postgres_port = postgres_conn.port
    postgres_database = postgres_conn.schema
    postgres_user = postgres_conn.login
    postgres_password = postgres_conn.password
    postgres_sslmode = postgres_conn.extra_dejson.get("sslmode", "prefer")

    trusted_bucket = updatelatestpositions_vars["trusted_bucket"]
    app_folder = updatelatestpositions_vars["app_folder"]
    positions_table_name = updatelatestpositions_vars["positions_table_name"]
    latest_positions_table_name = updatelatestpositions_vars[
        "latest_positions_table_name"
    ]
    config = {
        "TRUSTED_BUCKET": trusted_bucket,
        "APP_FOLDER": app_folder,
        "POSITIONS_TABLE_NAME": positions_table_name,
        "LATEST_POSITIONS_TABLE_NAME": latest_positions_table_name,
        "MINIO_ENDPOINT": minio_endpoint,
        "ACCESS_KEY": minio_access_key,
        "SECRET_KEY": minio_secret_key,
        "DB_HOST": postgres_host,
        "DB_PORT": postgres_port,
        "DB_DATABASE": postgres_database,
        "DB_USER": postgres_user,
        "DB_PASSWORD": postgres_password,
        "DB_SSLMODE": postgres_sslmode,
    }
    logging.info(f"Airflow configuration for Update Latest Positions: {config}")
    return config

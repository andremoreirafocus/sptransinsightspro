import os
import json

PIPELINE_NAME = "transformlivedata"


def get_config():
    # Check if we are running inside Airflow
    if os.getenv("AIRFLOW_HOME"):
        # Pulling from Airflow Variables
        # from airflow.models import Variable
        return get_airflow_config()
    else:
        # Pulling from local .env or hardcoded defaults for testing
        from dotenv import dotenv_values

        return get_local_config(dotenv_values(f"{PIPELINE_NAME}/.env"))


def get_airflow_config():
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable

    minio_conn = BaseHook.get_connection("minio_conn")
    minio_endpoint = f"{minio_conn.host}:{minio_conn.port}"
    minio_access_key = minio_conn.login
    minio_secret_key = minio_conn.password
    general_vars = Variable.get(f"{PIPELINE_NAME}_general", deserialize_json=True)
    raw_data_json_schema = Variable.get(
        f"{PIPELINE_NAME}_raw_data_json_schema", deserialize_json=True
    )
    data_expectations = Variable.get(
        f"{PIPELINE_NAME}_data_expectations", deserialize_json=True
    )
    storage = general_vars["storage"]
    tables = general_vars["tables"]
    compression = general_vars["compression"]
    quality = general_vars["quality"]
    postgres_conn = BaseHook.get_connection("airflow_postgres_conn")
    postgres_host = postgres_conn.host
    postgres_port = postgres_conn.port
    postgres_database = postgres_conn.schema
    postgres_user = postgres_conn.login
    postgres_password = postgres_conn.password
    postgres_sslmode = postgres_conn.extra_dejson.get("sslmode", "prefer")
    storage["minio_endpoint"] = minio_endpoint
    storage["access_key"] = minio_access_key
    storage["secret_key"] = minio_secret_key
    general_vars["database"] = {
        "host": postgres_host,
        "port": postgres_port,
        "database": postgres_database,
        "user": postgres_user,
        "password": postgres_password,
        "sslmode": postgres_sslmode,
    }
    general_config = {
        "storage": storage,
        "tables": tables,
        "compression": compression,
        "quality": quality,
        "database": general_vars["database"],
    }
    return {
        "general": general_config,
        "raw_data_json_schema": raw_data_json_schema,
        "data_expectations": data_expectations,
    }


def get_local_config(env_values):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    general_config_path = os.path.join(base_dir, f"{PIPELINE_NAME}_general.json")
    raw_schema_path = os.path.join(
        base_dir, f"{PIPELINE_NAME}_raw_data_json_schema.json"
    )
    expectations_path = os.path.join(
        base_dir, f"{PIPELINE_NAME}_data_expectations.json"
    )
    with open(general_config_path, "r") as f:
        general = json.load(f).get(f"{PIPELINE_NAME}_general")
    with open(raw_schema_path, "r") as f:
        raw_data_json_schema = json.load(f).get(
            f"{PIPELINE_NAME}_raw_data_json_schema", {}
        )
    with open(expectations_path, "r") as f:
        data_expectations = json.load(f).get(f"{PIPELINE_NAME}_data_expectations", {})
    storage = general.setdefault("storage", {})
    database = general.setdefault("database", {})
    if env_values.get("MINIO_ENDPOINT"):
        storage["minio_endpoint"] = env_values.get("MINIO_ENDPOINT")
    if env_values.get("ACCESS_KEY"):
        storage["access_key"] = env_values.get("ACCESS_KEY")
    if env_values.get("SECRET_KEY"):
        storage["secret_key"] = env_values.get("SECRET_KEY")
    if env_values.get("DB_HOST"):
        database["host"] = env_values.get("DB_HOST")
    if env_values.get("DB_PORT"):
        database["port"] = env_values.get("DB_PORT")
    if env_values.get("DB_DATABASE"):
        database["database"] = env_values.get("DB_DATABASE")
    if env_values.get("DB_USER"):
        database["user"] = env_values.get("DB_USER")
    if env_values.get("DB_PASSWORD"):
        database["password"] = env_values.get("DB_PASSWORD")
    if env_values.get("DB_SSLMODE"):
        database["sslmode"] = env_values.get("DB_SSLMODE")
    return {
        "general": general,
        "raw_data_json_schema": raw_data_json_schema,
        "data_expectations": data_expectations,
    }

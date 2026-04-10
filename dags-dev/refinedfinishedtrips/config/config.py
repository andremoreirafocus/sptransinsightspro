import logging
import os
import json


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

        return get_local_config(dotenv_values("refinedfinishedtrips/.env"))


def get_local_config(env_values):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "refinedfinishedtrips.json")
    with open(config_path, "r") as f:
        general = json.load(f)["refinedfinishedtrips_general"]

    analysis = general.setdefault("analysis", {})
    storage = general.setdefault("storage", {})
    tables = general.setdefault("tables", {})
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
    }


def get_airflow_config():
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable

    minio_conn = BaseHook.get_connection("minio_conn")
    minio_endpoint = f"{minio_conn.host}:{minio_conn.port}"
    minio_access_key = minio_conn.login
    minio_secret_key = minio_conn.password
    refinedfinishedtrips_vars = Variable.get(
        "refinedfinishedtrips_general", deserialize_json=True
    )
    postgres_conn = BaseHook.get_connection("postgres_conn")
    postgres_host = postgres_conn.host
    postgres_port = postgres_conn.port
    postgres_database = postgres_conn.schema
    postgres_user = postgres_conn.login
    postgres_password = postgres_conn.password
    try:
        postgres_sslmode = postgres_conn.extra_dejson["sslmode"]
    except Exception as e:
        logging.error("Missing required key in postgres_conn.extra_dejson: %s", e)
        raise ValueError(f"Missing required key in postgres_conn.extra_dejson: {e}")

    analysis = refinedfinishedtrips_vars["analysis"]
    storage = refinedfinishedtrips_vars["storage"]
    tables = refinedfinishedtrips_vars["tables"]
    database = refinedfinishedtrips_vars.get("database", {})
    storage["minio_endpoint"] = minio_endpoint
    storage["access_key"] = minio_access_key
    storage["secret_key"] = minio_secret_key
    database["host"] = postgres_host
    database["port"] = postgres_port
    database["database"] = postgres_database
    database["user"] = postgres_user
    database["password"] = postgres_password
    database["sslmode"] = postgres_sslmode
    config = {
        "general": {
            "analysis": analysis,
            "storage": storage,
            "tables": tables,
            "database": database,
        }
    }
    logging.info(f"Airflow configuration for finsihed trips calculation: {config}")
    return config

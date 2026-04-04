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

        return get_local_config(dotenv_values("orchestratetransform/.env"))


def get_local_config(env_values):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "orchestratetransform.json")
    with open(config_path, "r") as f:
        general = json.load(f)["orchestratetransform_general"]

    orchestration = general.setdefault("orchestration", {})
    tables = general.setdefault("tables", {})
    database = general.setdefault("database", {})
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

    orchestrate_transform_vars = Variable.get(
        "orchestratetransform_general", deserialize_json=True
    )
    postgres_conn = BaseHook.get_connection("airflow_postgres_conn")
    postgres_host = postgres_conn.host
    postgres_port = postgres_conn.port
    postgres_database = postgres_conn.schema
    postgres_user = postgres_conn.login
    postgres_password = postgres_conn.password
    try:
        postgres_sslmode = postgres_conn.extra_dejson["sslmode"]
    except Exception as e:
        raise RuntimeError(f"Missing required key in postgres_conn.extra_dejson: {e}")

    orchestration = orchestrate_transform_vars["orchestration"]
    tables = orchestrate_transform_vars["tables"]
    database = orchestrate_transform_vars.get("database", {})
    database["host"] = postgres_host
    database["port"] = postgres_port
    database["database"] = postgres_database
    database["user"] = postgres_user
    database["password"] = postgres_password
    database["sslmode"] = postgres_sslmode
    config = {
        "general": {
            "orchestration": orchestration,
            "tables": tables,
            "database": database,
        }
    }
    return config

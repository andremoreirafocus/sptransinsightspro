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

        return dotenv_values("orchestratetransform/.env")


def get_airflow_config():
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable

    orchestrate_transform_vars = Variable.get(
        "orchestratetransform", deserialize_json=True
    )
    raw_events_table_name = orchestrate_transform_vars["raw_events_table_name"]
    postgres_conn = BaseHook.get_connection("airflow_postgres_conn")
    postgres_host = postgres_conn.host
    postgres_port = postgres_conn.port
    postgres_database = postgres_conn.schema
    postgres_user = postgres_conn.login
    postgres_password = postgres_conn.password
    postgres_sslmode = postgres_conn.extra_dejson.get("sslmode", "prefer")
    config = {
        "RAW_EVENTS_TABLE_NAME": raw_events_table_name,
        "DB_HOST": postgres_host,
        "DB_PORT": postgres_port,
        "DB_DATABASE": postgres_database,
        "DB_USER": postgres_user,
        "DB_PASSWORD": postgres_password,
        "DB_SSLMODE": postgres_sslmode,
    }
    return config

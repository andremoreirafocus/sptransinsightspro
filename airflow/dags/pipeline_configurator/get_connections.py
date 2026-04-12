from typing import Any, Dict
from urllib.parse import urlparse


def get_object_storage_connection_from_airflow(
    connection_name: str,
) -> Dict[str, Any]:
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(connection_name)
    return {
        "endpoint": f"{conn.host}:{conn.port}",
        "access_key": conn.login,
        "secret_key": conn.password,
    }


def get_database_connection_from_airflow(
    connection_name: str,
) -> Dict[str, Any]:
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(connection_name)
    sslmode = conn.extra_dejson.get("sslmode", "prefer")
    return {
        "host": conn.host,
        "port": conn.port,
        "database": conn.schema,
        "user": conn.login,
        "password": conn.password,
        "sslmode": sslmode,
    }


def get_object_storage_connection_from_env(
    env_values: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "endpoint": env_values.get("MINIO_ENDPOINT", ""),
        "access_key": env_values.get("ACCESS_KEY", ""),
        "secret_key": env_values.get("SECRET_KEY", ""),
    }


def get_database_connection_from_env(
    env_values: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "host": env_values.get("DB_HOST", ""),
        "port": int(env_values.get("DB_PORT") or 0),
        "database": env_values.get("DB_DATABASE", ""),
        "user": env_values.get("DB_USER", ""),
        "password": env_values.get("DB_PASSWORD", ""),
        "sslmode": env_values.get("DB_SSLMODE", "prefer"),
    }


def get_http_connection_from_airflow(
    connection_name: str,
) -> Dict[str, Any]:
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(connection_name)
    return {
        "conn_type": conn.conn_type,
        "host": conn.host,
        "schema": conn.schema,
        "login": conn.login,
        "password": conn.password,
    }


def get_http_connection_from_env(
    env_values: Dict[str, Any],
) -> Dict[str, Any]:
    gtfs_url = env_values.get("GTFS_URL", "")
    parsed = urlparse(gtfs_url) if gtfs_url else None
    schema = ""
    if parsed and parsed.path:
        schema = parsed.path
        if parsed.query:
            schema = f"{schema}?{parsed.query}"
    return {
        "conn_type": parsed.scheme if parsed else "",
        "host": parsed.netloc if parsed else "",
        "schema": schema,
        "login": env_values.get("LOGIN", ""),
        "password": env_values.get("PASSWORD", ""),
    }

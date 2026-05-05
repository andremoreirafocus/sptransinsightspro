from pipeline_configurator.get_connections import (
    get_database_connection_from_env,
    get_http_connection_from_env,
    get_object_storage_connection_from_env,
)


def test_get_object_storage_connection_from_env(local_env_values):
    result = get_object_storage_connection_from_env(local_env_values)

    assert result == {
        "endpoint": "localhost:9000",
        "access_key": "minioadmin",
        "secret_key": "miniosecret",
    }


def test_get_database_connection_from_env(local_env_values):
    result = get_database_connection_from_env(local_env_values)

    assert result == {
        "host": "localhost",
        "port": 5432,
        "database": "sptrans",
        "user": "postgres",
        "password": "postgres",
        "sslmode": "require",
    }


def test_get_http_connection_from_env(local_env_values):
    result = get_http_connection_from_env(local_env_values)

    assert result == {
        "conn_type": "https",
        "host": "example.com",
        "schema": "/gtfs/download?format=zip",
        "login": "user@example.com",
        "password": "secret",
    }


def test_get_http_connection_from_env_with_missing_url():
    result = get_http_connection_from_env({})

    assert result == {
        "conn_type": "",
        "host": "",
        "schema": "",
        "login": "",
        "password": "",
    }


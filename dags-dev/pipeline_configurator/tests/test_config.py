import os

import pytest

import pipeline_configurator.config as config_module
from fakes.fake_airflow_connection import FakeAirflowConnection


def test_get_config_uses_local_mode_by_default(
    pipeline_config_tree, dummy_schema
):
    os.environ.pop("AIRFLOW_HOME", None)
    os.environ.pop("PIPELINE_ENV", None)

    result = config_module.get_config(
        pipeline_config_tree["pipeline"],
        None,
        dummy_schema,
        "http_conn",
        "minio_conn",
        "postgres_conn",
    )

    assert result["general"]["storage"]["trusted_bucket"] == "trusted"
    assert result["connections"]["object_storage"]["endpoint"] == "localhost:9000"


def test_get_config_uses_airflow_when_airflow_home_is_set(
    pipeline_config_tree, dummy_schema, airflow_installer
):
    pipeline = pipeline_config_tree["pipeline"]
    airflow_installer(
        variable_values={
            f"{pipeline}_general": {
                "storage": {"trusted_bucket": "trusted"},
                "tables": {"positions_table_name": "positions"},
            }
        },
        connections={
            "http_conn": FakeAirflowConnection(
                host="example.com",
                schema="/api",
                login="user",
                password="secret",
                conn_type="https",
            ),
            "minio_conn": FakeAirflowConnection(
                host="localhost",
                port=9000,
                login="minioadmin",
                password="miniosecret",
            ),
            "postgres_conn": FakeAirflowConnection(
                host="localhost",
                port=5432,
                schema="sptrans",
                login="postgres",
                password="postgres",
                extra_dejson={"sslmode": "require"},
            ),
        },
    )
    os.environ["AIRFLOW_HOME"] = "/tmp/airflow"
    os.environ.pop("PIPELINE_ENV", None)

    result = config_module.get_config(
        pipeline,
        None,
        dummy_schema,
        "http_conn",
        "minio_conn",
        "postgres_conn",
    )

    assert result["general"]["storage"]["trusted_bucket"] == "trusted"
    assert result["connections"]["database"]["database"] == "sptrans"


def test_get_config_uses_explicit_airflow_override(
    pipeline_config_tree, dummy_schema, airflow_installer
):
    pipeline = pipeline_config_tree["pipeline"]
    airflow_installer(
        variable_values={
            f"{pipeline}_general": {
                "storage": {"trusted_bucket": "trusted"},
                "tables": {"positions_table_name": "positions"},
            }
        },
        connections={
            "minio_conn": FakeAirflowConnection(
                host="localhost",
                port=9000,
                login="minioadmin",
                password="miniosecret",
            ),
            "postgres_conn": FakeAirflowConnection(
                host="localhost",
                port=5432,
                schema="sptrans",
                login="postgres",
                password="postgres",
                extra_dejson={"sslmode": "require"},
            ),
        },
    )

    result = config_module.get_config(
        pipeline,
        "airflow",
        dummy_schema,
        None,
        "minio_conn",
        "postgres_conn",
    )

    assert result["general"]["storage"]["trusted_bucket"] == "trusted"
    assert result["connections"]["object_storage"]["endpoint"] == "localhost:9000"


def test_get_config_rejects_invalid_pipeline_env(pipeline_config_tree, dummy_schema):
    with pytest.raises(ValueError, match="PIPELINE_ENV must be 'airflow' or 'local'"):
        config_module.get_config(
            pipeline_config_tree["pipeline"],
            "invalid",
            dummy_schema,
            None,
            "minio_conn",
            "postgres_conn",
        )


def test_get_local_config_loads_general_connections_and_validation_artifacts(
    pipeline_config_tree, local_env_values, dummy_schema
):
    result = config_module._get_local_config(
        pipeline_config_tree["pipeline"],
        local_env_values,
        http_conn_name="http_conn",
        object_storage_conn_name="minio_conn",
        database_conn_name="postgres_conn",
        general_schema=dummy_schema,
    )

    assert result["general"]["storage"]["trusted_bucket"] == "trusted"
    assert result["raw_data_json_schema"] == {"type": "object"}
    assert result["data_expectations"] == {"expectations": ["x"]}
    assert result["connections"]["object_storage"]["endpoint"] == "localhost:9000"
    assert result["connections"]["database"]["port"] == 5432
    assert result["connections"]["http"]["conn_type"] == "https"


def test_get_local_config_builds_only_requested_connections(
    pipeline_config_tree, local_env_values, dummy_schema
):
    result = config_module._get_local_config(
        pipeline_config_tree["pipeline"],
        local_env_values,
        http_conn_name=None,
        object_storage_conn_name="minio_conn",
        database_conn_name=None,
        general_schema=dummy_schema,
    )

    assert result["connections"] == {
        "object_storage": {
            "endpoint": "localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "miniosecret",
        }
    }


def test_get_local_config_raises_for_missing_general_file(
    pipeline_config_tree, local_env_values, dummy_schema
):
    missing_pipeline = "missingpipeline"

    with pytest.raises(ValueError, match="Failed to load config key missingpipeline_general"):
        config_module._get_local_config(
            missing_pipeline,
            local_env_values,
            object_storage_conn_name="minio_conn",
            general_schema=dummy_schema,
        )


def test_get_local_config_raises_for_invalid_general_schema(
    pipeline_config_tree, local_env_values, failing_schema
):
    with pytest.raises(ValueError, match="Invalid config:"):
        config_module._get_local_config(
            pipeline_config_tree["pipeline"],
            local_env_values,
            object_storage_conn_name="minio_conn",
            general_schema=failing_schema,
        )


def test_load_json_raises_for_missing_file():
    with pytest.raises(ValueError, match="Failed to load config key some_key"):
        config_module._load_json("/tmp/does-not-exist.json", "some_key")


def test_get_airflow_config_loads_general_artifacts_and_connections(
    pipeline_config_tree, dummy_schema, airflow_installer
):
    pipeline = pipeline_config_tree["pipeline"]
    variable_values = {
        f"{pipeline}_general": {
            "storage": {"trusted_bucket": "trusted"},
            "tables": {"positions_table_name": "positions"},
            "data_validations": {
                "json_validation": {"schemas": ["raw_data_json_schema"]},
                "expectations_validation": {
                    "expectations_suites": ["data_expectations"]
                },
            },
        },
        f"{pipeline}_raw_data_json_schema": {"type": "object"},
        f"{pipeline}_data_expectations": {"expectations": ["x"]},
    }
    airflow_installer(
        variable_values=variable_values,
        connections={
            "http_conn": FakeAirflowConnection(
                host="example.com",
                schema="/api",
                login="user",
                password="secret",
                conn_type="http",
            ),
            "minio_conn": FakeAirflowConnection(
                host="localhost",
                port=9000,
                login="minioadmin",
                password="miniosecret",
            ),
            "postgres_conn": FakeAirflowConnection(
                host="localhost",
                port=5432,
                schema="sptrans",
                login="postgres",
                password="postgres",
            ),
        },
    )

    result = config_module._get_airflow_config(
        pipeline,
        http_conn_name="http_conn",
        object_storage_conn_name="minio_conn",
        database_conn_name="postgres_conn",
        general_schema=dummy_schema,
    )

    assert result["general"]["storage"]["trusted_bucket"] == "trusted"
    assert result["raw_data_json_schema"] == {"type": "object"}
    assert result["data_expectations"] == {"expectations": ["x"]}
    assert result["connections"] == {
        "http": {
            "conn_type": "http",
            "host": "example.com",
            "schema": "/api",
            "login": "user",
            "password": "secret",
        },
        "object_storage": {
            "endpoint": "localhost:9000",
            "access_key": "minioadmin",
            "secret_key": "miniosecret",
        },
        "database": {
            "host": "localhost",
            "port": 5432,
            "database": "sptrans",
            "user": "postgres",
            "password": "postgres",
            "sslmode": "prefer",
        },
    }

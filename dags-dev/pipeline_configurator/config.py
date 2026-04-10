import json
import os
from typing import Any, Dict, Optional

from pipeline_configurator.get_connections import (
    get_database_connection_from_airflow,
    get_database_connection_from_env,
    get_object_storage_connection_from_airflow,
    get_object_storage_connection_from_env,
)


def get_config(
    pipeline: str,
    env: Optional[str],
    general_schema: Any,
    object_storage_conn_name: str,
    database_conn_name: str,
    load_raw_data_json_schema: bool = False,
    load_data_expectations: bool = False,
) -> Dict[str, Any]:
    """
    Load configuration for a pipeline.
    env override: 'airflow' or 'local'. If not set, auto-detect via AIRFLOW_HOME.
    """
    env_override = (env or os.getenv("PIPELINE_ENV") or "").strip().lower()
    if env_override not in ("", "airflow", "local"):
        raise ValueError("PIPELINE_ENV must be 'airflow' or 'local'")
    use_airflow = env_override == "airflow" or (
        env_override == "" and os.getenv("AIRFLOW_HOME")
    )
    if use_airflow:
        return _get_airflow_config(
            pipeline,
            load_raw_data_json_schema=load_raw_data_json_schema,
            load_data_expectations=load_data_expectations,
            object_storage_conn_name=object_storage_conn_name,
            database_conn_name=database_conn_name,
            general_schema=general_schema,
        )
    from dotenv import dotenv_values

    return _get_local_config(
        pipeline,
        dotenv_values(f"{pipeline}/.env"),
        load_raw_data_json_schema=load_raw_data_json_schema,
        load_data_expectations=load_data_expectations,
        general_schema=general_schema,
    )


def _get_airflow_config(
    pipeline: str,
    load_raw_data_json_schema: bool = False,
    load_data_expectations: bool = False,
    object_storage_conn_name: str = "minio_conn",
    database_conn_name: str = "airflow_postgres_conn",
    general_schema: Any = None,
) -> Dict[str, Any]:
    from airflow.models import Variable

    general_vars = Variable.get(f"{pipeline}_general", deserialize_json=True)
    raw_data_json_schema = (
        Variable.get(
            f"{pipeline}_raw_data_json_schema", deserialize_json=True, default_var={}
        )
        if load_raw_data_json_schema
        else {}
    )
    data_expectations = (
        Variable.get(
            f"{pipeline}_data_expectations", deserialize_json=True, default_var={}
        )
        if load_data_expectations
        else {}
    )
    _validate_general_input(general_vars, general_schema)
    connections = {
        "object_storage": get_object_storage_connection_from_airflow(
            object_storage_conn_name
        ),
        "database": get_database_connection_from_airflow(database_conn_name),
    }
    config = {
        "general": general_vars,
        "connections": connections,
    }
    if load_raw_data_json_schema:
        config["raw_data_json_schema"] = raw_data_json_schema
    if load_data_expectations:
        config["data_expectations"] = data_expectations
    return config


def _get_local_config(
    pipeline: str,
    env_values: Dict[str, Any],
    load_raw_data_json_schema: bool = False,
    load_data_expectations: bool = False,
    general_schema: Any = None,
) -> Dict[str, Any]:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    general_config_path = os.path.join(
        base_dir, "..", pipeline, "config", f"{pipeline}_general.json"
    )
    raw_schema_path = os.path.join(
        base_dir, "..", pipeline, "config", f"{pipeline}_raw_data_json_schema.json"
    )
    expectations_path = os.path.join(
        base_dir, "..", pipeline, "config", f"{pipeline}_data_expectations.json"
    )
    with open(general_config_path, "r") as f:
        general_config = json.load(f).get(f"{pipeline}_general")
    raw_data_json_schema = (
        _load_optional_json(raw_schema_path, f"{pipeline}_raw_data_json_schema")
        if load_raw_data_json_schema
        else {}
    )
    data_expectations = (
        _load_optional_json(expectations_path, f"{pipeline}_data_expectations")
        if load_data_expectations
        else {}
    )
    _validate_general_input(general_config, general_schema)
    connections = {
        "object_storage": get_object_storage_connection_from_env(env_values),
        "database": get_database_connection_from_env(env_values),
    }
    config = {
        "general": general_config,
        "connections": connections,
    }
    if load_raw_data_json_schema:
        config["raw_data_json_schema"] = raw_data_json_schema
    if load_data_expectations:
        config["data_expectations"] = data_expectations
    return config


def _load_optional_json(path: str, key: str) -> Dict[str, Any]:
    try:
        with open(path, "r") as f:
            return json.load(f).get(key, {})
    except FileNotFoundError:
        return {}


def _validate_general_input(
    general: Dict[str, Any],
    model: Any,
) -> Any:
    try:
        return model.model_validate(general)
    except Exception as e:
        raise ValueError(f"Invalid config: {e}")

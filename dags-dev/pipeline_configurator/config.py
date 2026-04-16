import json
import os
from typing import Any, Dict, Optional

from pipeline_configurator.get_connections import (
    get_database_connection_from_airflow,
    get_database_connection_from_env,
    get_http_connection_from_airflow,
    get_http_connection_from_env,
    get_object_storage_connection_from_airflow,
    get_object_storage_connection_from_env,
)


def get_config(
    pipeline: str,
    env: Optional[str],
    general_schema: Any,
    http_conn_name: str,
    object_storage_conn_name: str,
    database_conn_name: str,
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
            http_conn_name=http_conn_name,
            object_storage_conn_name=object_storage_conn_name,
            database_conn_name=database_conn_name,
            general_schema=general_schema,
        )
    from dotenv import dotenv_values

    return _get_local_config(
        pipeline,
        dotenv_values(f"{pipeline}/.env"),
        http_conn_name=http_conn_name,
        object_storage_conn_name=object_storage_conn_name,
        database_conn_name=database_conn_name,
        general_schema=general_schema,
    )


def _get_airflow_config(
    pipeline: str,
    http_conn_name: str = None,
    object_storage_conn_name: str = None,
    database_conn_name: str = None,
    general_schema: Any = None,
) -> Dict[str, Any]:
    from airflow.models import Variable

    general_vars = Variable.get(f"{pipeline}_general", deserialize_json=True)
    _validate_general_input(general_vars, general_schema)
    config = {
        "general": general_vars,
    }
    data_validations = general_vars.get("data_validations")
    if data_validations:
        schemas = data_validations.get("json_validation", {}).get("schemas", [])
        expectations_suites = data_validations.get("expectations_validation", {}).get("expectations_suites", [])
        for name in schemas + expectations_suites:
            config[name] = Variable.get(
                f"{pipeline}_{name}",
                deserialize_json=True,
                default_var={},
            )
    connections = {}
    if http_conn_name is not None:
        http_connection = get_http_connection_from_airflow(http_conn_name)
        connections["http"] = http_connection
    if object_storage_conn_name is not None:
        object_storage = get_object_storage_connection_from_airflow(
            object_storage_conn_name
        )
        connections["object_storage"] = object_storage
    if database_conn_name is not None:
        database = get_database_connection_from_airflow(database_conn_name)
        connections["database"] = database
    config["connections"] = connections
    return config


def _get_local_config(
    pipeline: str,
    env_values: Dict[str, Any],
    http_conn_name: str = None,
    object_storage_conn_name: str = None,
    database_conn_name: str = None,
    general_schema: Any = None,
) -> Dict[str, Any]:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    general_config_path = os.path.join(
        base_dir, "..", pipeline, "config", f"{pipeline}_general.json"
    )
    try:
        with open(general_config_path, "r") as f:
            general_config = json.load(f).get(f"{pipeline}_general")
    except FileNotFoundError as e:
        raise ValueError(
            f"Failed to load config key {pipeline}_general from file {general_config_path}: {e}"
        )
    _validate_general_input(general_config, general_schema)
    config = {
        "general": general_config,
    }
    data_validations = general_config.get("data_validations")
    if data_validations:
        schemas = data_validations.get("json_validation", {}).get("schemas", [])
        expectations_suites = data_validations.get("expectations_validation", {}).get("expectations_suites", [])
        for name in schemas + expectations_suites:
            artifact_path = os.path.join(
                base_dir, "..", pipeline, "config", f"{pipeline}_{name}.json"
            )
            config[name] = _load_json(artifact_path, f"{pipeline}_{name}")

    connections = {}
    if http_conn_name is not None:
        http_connection = get_http_connection_from_env(env_values)
        connections["http"] = http_connection
    if object_storage_conn_name is not None:
        object_storage = get_object_storage_connection_from_env(env_values)
        connections["object_storage"] = object_storage
    if database_conn_name is not None:
        database = get_database_connection_from_env(env_values)
        connections["database"] = database
    config["connections"] = connections
    return config


def _load_json(path: str, key: str) -> Dict[str, Any]:
    try:
        with open(path, "r") as f:
            return json.load(f).get(key, {})
    except FileNotFoundError as e:
        raise ValueError(f"Failed to load config key {key} from file {path}: {e}")


def _validate_general_input(
    general: Dict[str, Any],
    model: Any,
) -> Any:
    try:
        return model.model_validate(general)
    except Exception as e:
        raise ValueError(f"Invalid config: {e}")

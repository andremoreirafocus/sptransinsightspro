import json
import os
import shutil
import sys
import types
from pathlib import Path

import pytest
from fakes.fake_airflow_base_hook import FakeAirflowBaseHook
from fakes.fake_airflow_connection import FakeAirflowConnection
from fakes.fake_airflow_variable import FakeAirflowVariable


def pytest_configure():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    shared_fakes = os.path.join(project_root, "tests")
    for path in [project_root, shared_fakes]:
        if path not in sys.path:
            sys.path.insert(0, path)


class DummySchema:
    @classmethod
    def model_validate(cls, value):
        return value


class FailingSchema:
    @classmethod
    def model_validate(cls, value):
        raise ValueError("schema validation failed")


@pytest.fixture
def dummy_schema():
    return DummySchema


@pytest.fixture
def failing_schema():
    return FailingSchema


@pytest.fixture
def local_env_values():
    return {
        "MINIO_ENDPOINT": "localhost:9000",
        "ACCESS_KEY": "minioadmin",
        "SECRET_KEY": "miniosecret",
        "DB_HOST": "localhost",
        "DB_PORT": "5432",
        "DB_DATABASE": "sptrans",
        "DB_USER": "postgres",
        "DB_PASSWORD": "postgres",
        "DB_SSLMODE": "require",
        "GTFS_URL": "https://example.com/gtfs/download?format=zip",
        "LOGIN": "user@example.com",
        "PASSWORD": "secret",
    }


@pytest.fixture
def pipeline_config_tree():
    project_root = Path(__file__).resolve().parents[3]
    dags_dev_dir = project_root / "dags-dev"
    pipeline_name = "test_pipeline_configurator_fake"
    pipeline_dir = dags_dev_dir / pipeline_name
    if pipeline_dir.exists():
        shutil.rmtree(pipeline_dir)
    config_dir = pipeline_dir / "config"
    config_dir.mkdir(parents=True)
    original_cwd = Path.cwd()

    def write_json(name, payload):
        path = config_dir / name
        path.write_text(json.dumps(payload))
        return path

    write_json(
        f"{pipeline_name}_general.json",
        {
            f"{pipeline_name}_general": {
                "storage": {"trusted_bucket": "trusted"},
                "tables": {"positions_table_name": "positions"},
                "data_validations": {
                    "json_validation": {"schemas": ["raw_data_json_schema"]},
                    "expectations_validation": {
                        "expectations_suites": ["data_expectations"]
                    },
                },
            }
        },
    )
    write_json(
        f"{pipeline_name}_raw_data_json_schema.json",
        {f"{pipeline_name}_raw_data_json_schema": {"type": "object"}},
    )
    write_json(
        f"{pipeline_name}_data_expectations.json",
        {f"{pipeline_name}_data_expectations": {"expectations": ["x"]}},
    )
    (pipeline_dir / ".env").write_text(
        "\n".join(
            [
                "MINIO_ENDPOINT=localhost:9000",
                "ACCESS_KEY=minioadmin",
                "SECRET_KEY=miniosecret",
                "DB_HOST=localhost",
                "DB_PORT=5432",
                "DB_DATABASE=sptrans",
                "DB_USER=postgres",
                "DB_PASSWORD=postgres",
                "DB_SSLMODE=require",
                "GTFS_URL=https://example.com/gtfs/download?format=zip",
                "LOGIN=user@example.com",
                "PASSWORD=secret",
            ]
        )
    )
    os.chdir(dags_dev_dir)

    try:
        yield {
            "project_root": project_root,
            "dags_dev_dir": dags_dev_dir,
            "pipeline": pipeline_name,
            "pipeline_dir": pipeline_dir,
            "config_dir": config_dir,
        }
    finally:
        os.chdir(original_cwd)
        if pipeline_dir.exists():
            shutil.rmtree(pipeline_dir)


@pytest.fixture(autouse=True)
def restore_process_state():
    env_backup = {
        "AIRFLOW_HOME": os.environ.get("AIRFLOW_HOME"),
        "PIPELINE_ENV": os.environ.get("PIPELINE_ENV"),
    }
    cwd_backup = Path.cwd()
    module_names = ["airflow", "airflow.models", "airflow.hooks", "airflow.hooks.base"]
    module_backup = {name: sys.modules.get(name) for name in module_names}
    try:
        yield
    finally:
        for key, value in env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        os.chdir(cwd_backup)
        for name, module in module_backup.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module


@pytest.fixture
def airflow_installer():
    def install(variable_values=None, connections=None):
        FakeAirflowVariable.values = variable_values or {}
        FakeAirflowBaseHook.connections = connections or {}

        airflow_module = types.ModuleType("airflow")
        models_module = types.ModuleType("airflow.models")
        hooks_module = types.ModuleType("airflow.hooks")
        hooks_base_module = types.ModuleType("airflow.hooks.base")

        models_module.Variable = FakeAirflowVariable
        hooks_base_module.BaseHook = FakeAirflowBaseHook
        hooks_module.base = hooks_base_module
        airflow_module.models = models_module
        airflow_module.hooks = hooks_module

        sys.modules["airflow"] = airflow_module
        sys.modules["airflow.models"] = models_module
        sys.modules["airflow.hooks"] = hooks_module
        sys.modules["airflow.hooks.base"] = hooks_base_module

        return None

    install.Conn = FakeAirflowConnection
    return install

import os
import logging
import json

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def get_config():
    # Check if we are running inside Airflow
    if os.getenv("AIRFLOW_HOME"):
        # Pulling from Airflow Variables
        config = get_airflow_config()
        logger.info("Configuration retrieved for Airflow!")
    else:
        # Pulling from local .env
        from dotenv import dotenv_values

        config = get_local_config(dotenv_values("gtfs/.env"))
        logger.info("Configuration retrieved locally!")
    logger.info(f"Config: {config}")
    return config


def get_local_config(env_values):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "gtfs.json")
    with open(config_path, "r") as f:
        general = json.load(f)["gtfs_general"]

    storage = general.setdefault("storage", {})
    extraction = general.setdefault("extraction", {})
    if env_values.get("GTFS_URL"):
        extraction["gtfs_url"] = env_values.get("GTFS_URL")
    if env_values.get("LOGIN"):
        extraction["login"] = env_values.get("LOGIN")
    if env_values.get("PASSWORD"):
        extraction["password"] = env_values.get("PASSWORD")
    if env_values.get("MINIO_ENDPOINT"):
        storage["minio_endpoint"] = env_values.get("MINIO_ENDPOINT")
    if env_values.get("ACCESS_KEY"):
        storage["access_key"] = env_values.get("ACCESS_KEY")
    if env_values.get("SECRET_KEY"):
        storage["secret_key"] = env_values.get("SECRET_KEY")
    return {
        "general": general,
    }


def get_airflow_config():
    from airflow.hooks.base import BaseHook
    from airflow.models import Variable

    gtfs_conn = BaseHook.get_connection("gtfs_conn")
    gtfs_url = f"{gtfs_conn.conn_type}://{gtfs_conn.host}{gtfs_conn.schema}"
    gtfs_login = gtfs_conn.login
    gtfs_password = gtfs_conn.password
    minio_conn = BaseHook.get_connection("minio_conn")
    minio_endpoint = f"{minio_conn.host}:{minio_conn.port}"
    minio_access_key = minio_conn.login
    minio_secret_key = minio_conn.password
    gtfs_vars = Variable.get("gtfs_general", deserialize_json=True)

    extraction = gtfs_vars["extraction"]
    storage = gtfs_vars["storage"]
    tables = gtfs_vars["tables"]
    extraction["gtfs_url"] = gtfs_url
    extraction["login"] = gtfs_login
    extraction["password"] = gtfs_password
    storage["minio_endpoint"] = minio_endpoint
    storage["access_key"] = minio_access_key
    storage["secret_key"] = minio_secret_key
    config = {
        "general": {
            "extraction": extraction,
            "storage": storage,
            "tables": tables,
        }
    }
    return config

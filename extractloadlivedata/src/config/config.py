import os
from typing import Any, Dict

from dotenv import load_dotenv

_COMMON_REQUIRED_KEYS = [
    "EXTRACTION_INTERVAL_SECONDS",
    "API_BASE_URL",
    "TOKEN",
    "API_MAX_RETRIES",
    "INGEST_BUFFER_PATH",
    "DATA_COMPRESSION_ON_SAVE",
    "SOURCE_BUCKET",
    "APP_FOLDER",
    "MINIO_ENDPOINT",
    "ACCESS_KEY",
    "SECRET_KEY",
    "NOTIFICATION_ENGINE",
]

_AIRFLOW_REQUIRED_KEYS = [
    "INVOKATIONS_CACHE_DIR",
    "AIRFLOW_USER",
    "AIRFLOW_PASSWORD",
    "AIRFLOW_WEBSERVER",
    "AIRFLOW_DAG_NAME",
]

_PROCESSING_REQUESTS_REQUIRED_KEYS = [
    "PROCESSING_REQUESTS_CACHE_DIR",
    "RAW_EVENTS_TABLE_NAME",
    "DB_HOST",
    "DB_PORT",
    "DB_DATABASE",
    "DB_USER",
    "DB_PASSWORD",
]

_ENGINE_REQUIRED_KEYS = {
    "airflow": _AIRFLOW_REQUIRED_KEYS,
    "processing_requests": _PROCESSING_REQUESTS_REQUIRED_KEYS,
}


def get_config() -> Dict[str, Any]:
    load_dotenv(override=False)
    return dict(os.environ)


def validate_config(config: Dict[str, str]) -> None:
    missing = [k for k in _COMMON_REQUIRED_KEYS if not config.get(k)]
    if missing:
        raise ValueError(f"Missing required config keys: {missing}")
    engine = config.get("NOTIFICATION_ENGINE", "").strip()
    engine_keys = _ENGINE_REQUIRED_KEYS.get(engine)
    if engine_keys is None:
        raise ValueError(
            f"Unknown NOTIFICATION_ENGINE value '{engine}'. "
            f"Must be one of: {list(_ENGINE_REQUIRED_KEYS)}"
        )
    missing_engine = [k for k in engine_keys if not config.get(k)]
    if missing_engine:
        raise ValueError(
            f"Missing required config keys for engine '{engine}': {missing_engine}"
        )

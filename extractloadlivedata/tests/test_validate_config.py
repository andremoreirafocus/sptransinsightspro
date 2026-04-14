import pytest

from src.config import validate_config

_COMMON_BASE = {
    "EXTRACTION_INTERVAL_SECONDS": "120",
    "API_BASE_URL": "https://api.example.com",
    "TOKEN": "abc123",
    "API_MAX_RETRIES": "4",
    "INGEST_BUFFER_PATH": "/tmp/ingest",
    "DATA_COMPRESSION_ON_SAVE": "true",
    "SOURCE_BUCKET": "raw",
    "APP_FOLDER": "sptrans",
    "MINIO_ENDPOINT": "minio:9000",
    "ACCESS_KEY": "key",
    "SECRET_KEY": "secret",
}

_PROCESSING_REQUESTS_KEYS = {
    "NOTIFICATION_ENGINE": "processing_requests",
    "PROCESSING_REQUESTS_CACHE_DIR": "/tmp/pr_cache",
    "RAW_EVENTS_TABLE_NAME": "to_be_processed.raw",
    "DB_HOST": "localhost",
    "DB_PORT": "5432",
    "DB_DATABASE": "mydb",
    "DB_USER": "user",
    "DB_PASSWORD": "pass",
}

_AIRFLOW_KEYS = {
    "NOTIFICATION_ENGINE": "airflow",
    "INVOKATIONS_CACHE_DIR": "/tmp/inv_cache",
    "AIRFLOW_USER": "admin",
    "AIRFLOW_PASSWORD": "password",
    "AIRFLOW_WEBSERVER": "airflow_webserver",
    "AIRFLOW_DAG_NAME": "my_dag",
}


def _valid_processing_requests_config():
    return {**_COMMON_BASE, **_PROCESSING_REQUESTS_KEYS}


def _valid_airflow_config():
    return {**_COMMON_BASE, **_AIRFLOW_KEYS}


def test_valid_processing_requests_config_passes():
    validate_config(_valid_processing_requests_config())


def test_valid_airflow_config_passes():
    validate_config(_valid_airflow_config())


def test_missing_common_key_raises():
    config = _valid_processing_requests_config()
    del config["TOKEN"]
    with pytest.raises(ValueError, match="TOKEN"):
        validate_config(config)


def test_multiple_missing_common_keys_reported_together():
    config = _valid_processing_requests_config()
    del config["TOKEN"]
    del config["API_BASE_URL"]
    with pytest.raises(ValueError) as exc_info:
        validate_config(config)
    message = str(exc_info.value)
    assert "TOKEN" in message
    assert "API_BASE_URL" in message


def test_unknown_notification_engine_raises():
    config = {**_COMMON_BASE, "NOTIFICATION_ENGINE": "kafka"}
    with pytest.raises(ValueError, match="kafka"):
        validate_config(config)


def test_empty_notification_engine_raises():
    config = {**_COMMON_BASE, "NOTIFICATION_ENGINE": ""}
    with pytest.raises(ValueError):
        validate_config(config)


def test_missing_notification_engine_raises():
    config = {**_COMMON_BASE}
    with pytest.raises(ValueError, match="NOTIFICATION_ENGINE"):
        validate_config(config)


def test_missing_processing_requests_engine_key_raises():
    config = _valid_processing_requests_config()
    del config["DB_PASSWORD"]
    with pytest.raises(ValueError, match="DB_PASSWORD"):
        validate_config(config)


def test_missing_airflow_engine_key_raises():
    config = _valid_airflow_config()
    del config["AIRFLOW_USER"]
    with pytest.raises(ValueError, match="AIRFLOW_USER"):
        validate_config(config)


def test_airflow_keys_not_required_for_processing_requests():
    config = _valid_processing_requests_config()
    # Airflow keys deliberately absent — should not raise
    validate_config(config)


def test_processing_requests_keys_not_required_for_airflow():
    config = _valid_airflow_config()
    # DB keys deliberately absent — should not raise
    validate_config(config)

import logging
import os
from functools import lru_cache
from typing import Any, Dict, Optional

import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_SRC_DIR))
_DEFAULT_CONFIG_PATH = os.path.join(_PROJECT_ROOT, "config", "pipelines.yaml")
_DEFAULT_DB_PATH = os.path.join(os.path.dirname(_SRC_DIR), "storage", "alerts.db")
_DEFAULT_LOG_FILE = os.path.join(_PROJECT_ROOT, "alertservice.log")
_ENV_PATH = os.path.join(_PROJECT_ROOT, ".env")


class Settings(BaseSettings):
    smtp_host: str
    smtp_port: int = 25
    smtp_user: Optional[str] = None
    smtp_password: Optional[str] = None
    smtp_use_tls: bool = False
    email_from: str
    email_to: str
    email_subject_prefix: str = "[DQ]"
    pipeline_config_path: str = _DEFAULT_CONFIG_PATH
    alerts_db_path: str = _DEFAULT_DB_PATH
    log_file: str = _DEFAULT_LOG_FILE

    model_config = SettingsConfigDict(
        env_file=_ENV_PATH,
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def load_pipeline_config() -> Dict[str, Any]:
    path = get_settings().pipeline_config_path
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    for pipeline, cfg in data.items():
        if not isinstance(cfg, dict):
            raise ValueError(f"Invalid config for pipeline '{pipeline}'")
        window = cfg.get("warning_window", {})
        if window:
            window_type = window.get("type")
            window_value = window.get("value")
            if window_type not in {"time", "count"}:
                raise ValueError(
                    f"Invalid warning_window.type for pipeline '{pipeline}'"
                )
            if not isinstance(window_value, int) or window_value <= 0:
                raise ValueError(
                    f"Invalid warning_window.value for pipeline '{pipeline}'"
                )
        thresholds = cfg.get("warning_thresholds", {})
        for key in ["max_failed_rows", "max_failed_ratio", "max_consecutive_warn"]:
            if key in thresholds and not isinstance(thresholds.get(key), (int, float)):
                raise ValueError(
                    f"Invalid warning_thresholds.{key} for pipeline '{pipeline}'"
                )
        for key in ["notify_on_fail", "notify_on_warn"]:
            if key not in cfg:
                raise ValueError(f"Missing {key} for pipeline '{pipeline}'")
            if not isinstance(cfg.get(key), bool):
                raise ValueError(f"Invalid {key} for pipeline '{pipeline}'")
    return data

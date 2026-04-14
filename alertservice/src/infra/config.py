import logging
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict

import yaml
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class NotificationDeliveryError(Exception):
    """Raised when email delivery fails."""


_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_SRC_DIR))
_CONFIG_PATH = os.path.join(_PROJECT_ROOT, "config", "pipelines.yaml")
_DB_PATH = os.path.join(os.path.dirname(_SRC_DIR), "storage", "alerts.db")
_LOG_FILE = os.path.join(_PROJECT_ROOT, "alertservice.log")
_ENV_PATH = os.path.join(_PROJECT_ROOT, ".env")


class Settings(BaseSettings):
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    smtp_use_tls: bool
    email_from: str
    email_to: str
    email_subject_prefix: str
    pipeline_config_path: str = _CONFIG_PATH
    alerts_db_path: str = _DB_PATH
    log_file: str = _LOG_FILE

    model_config = SettingsConfigDict(
        env_file=_ENV_PATH,
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


@dataclass(frozen=True)
class EmailSendConfig:
    smtp_host: str
    smtp_port: int
    user: str
    password: str
    smtp_use_tls: bool
    email_from: str
    email_to: str


def get_email_send_config(settings: Settings) -> EmailSendConfig:
    return EmailSendConfig(
        smtp_host=settings.smtp_host,
        smtp_port=settings.smtp_port,
        user=settings.smtp_user,
        password=settings.smtp_password,
        smtp_use_tls=settings.smtp_use_tls,
        email_from=settings.email_from,
        email_to=settings.email_to,
    )


def load_pipeline_config() -> Dict[str, Any]:
    path = get_settings().pipeline_config_path
    if not os.path.exists(path):
        raise FileNotFoundError(f"Pipeline config not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    for pipeline, cfg in data.items():
        if not isinstance(cfg, dict):
            raise ValueError(f"Invalid config for pipeline '{pipeline}'")
        if "warning_window" not in cfg:
            raise ValueError(f"Missing warning_window for pipeline '{pipeline}'")
        if "warning_thresholds" not in cfg:
            raise ValueError(f"Missing warning_thresholds for pipeline '{pipeline}'")
        window = cfg["warning_window"]
        if not isinstance(window, dict):
            raise ValueError(f"Invalid warning_window for pipeline '{pipeline}'")
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
        thresholds = cfg["warning_thresholds"]
        if not isinstance(thresholds, dict):
            raise ValueError(f"Invalid warning_thresholds for pipeline '{pipeline}'")
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

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

SRC_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SRC_DIR))
CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "pipelines.yaml")
ENV_PATH = os.path.join(PROJECT_ROOT, ".env")


def load_env() -> None:
    if os.path.exists(ENV_PATH):
        load_dotenv(ENV_PATH)


def get_email_subject_prefix() -> str:
    return os.getenv("EMAIL_SUBJECT_PREFIX", "[DQ]")


@dataclass(frozen=True)
class EmailConfig:
    smtp_host: str
    smtp_port: int
    smtp_user: Optional[str]
    smtp_password: Optional[str]
    email_from: str
    email_to: str
    use_tls: bool


def get_email_config() -> EmailConfig:
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "25"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    email_from = os.getenv("EMAIL_FROM")
    email_to = os.getenv("EMAIL_TO", "")
    use_tls = os.getenv("SMTP_USE_TLS", "false").lower() == "true"

    required = {
        "SMTP_HOST": smtp_host,
        "EMAIL_FROM": email_from,
        "EMAIL_TO": email_to,
    }
    missing = [key for key, value in required.items() if not value]
    if missing:
        raise RuntimeError(f"Missing required env keys: {', '.join(missing)}")

    return EmailConfig(
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        email_from=email_from,
        email_to=email_to,
        use_tls=use_tls,
    )


def validate_required_env() -> None:
    get_email_config()


def load_pipeline_config() -> Dict[str, Any]:
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
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

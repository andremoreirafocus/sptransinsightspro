import os
from typing import Any, Dict

import yaml
from dotenv import load_dotenv

SRC_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SRC_DIR)
CONFIG_PATH = os.path.join(BASE_DIR, "config", "pipelines.yaml")
ENV_PATH = os.path.join(BASE_DIR, "..", ".env")


def load_env() -> None:
    if os.path.exists(ENV_PATH):
        load_dotenv(ENV_PATH)


def get_email_subject_prefix() -> str:
    return os.getenv("EMAIL_SUBJECT_PREFIX", "[DQ]")


def validate_required_env() -> None:
    required_env = ["SMTP_HOST", "EMAIL_FROM", "EMAIL_TO"]
    missing = [key for key in required_env if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Missing required env keys: {', '.join(missing)}")


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

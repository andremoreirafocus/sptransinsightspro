#!/usr/bin/env python3
"""Render an importable Airflow connections file from the tracked template."""

from __future__ import annotations

import json
import os
import sys
from typing import Dict, Any


REQUIRED_ENV_VARS = (
    "MINIO_PLATFORM_ACCESS_KEY",
    "MINIO_PLATFORM_SECRET_KEY",
    "POSTGRES_DB_USER",
    "POSTGRES_DB_PASSWORD",
    "AIRFLOW_DB_USER",
    "AIRFLOW_DB_PASSWORD",
)


def require_env(var_name: str) -> str:
    value = os.environ.get(var_name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {var_name}")
    return value


def render_connections(source_path: str, target_path: str) -> None:
    with open(source_path, "r", encoding="utf-8") as source_file:
        data: Dict[str, Any] = json.load(source_file)

    data["minio_conn"]["login"] = require_env("MINIO_PLATFORM_ACCESS_KEY")
    data["minio_conn"]["password"] = require_env("MINIO_PLATFORM_SECRET_KEY")
    data["postgres_conn"]["login"] = require_env("POSTGRES_DB_USER")
    data["postgres_conn"]["password"] = require_env("POSTGRES_DB_PASSWORD")
    data["airflow_postgres_conn"]["login"] = require_env("AIRFLOW_DB_USER")
    data["airflow_postgres_conn"]["password"] = require_env("AIRFLOW_DB_PASSWORD")

    with open(target_path, "w", encoding="utf-8") as target_file:
        json.dump(data, target_file, indent=2)
        target_file.write("\n")


def main() -> int:
    if len(sys.argv) != 3:
        print(
            "Usage: render_airflow_connections.py <source_connections_json> <target_connections_json>",
            file=sys.stderr,
        )
        return 1

    try:
        for var_name in REQUIRED_ENV_VARS:
            require_env(var_name)
        render_connections(sys.argv[1], sys.argv[2])
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to render Airflow connections file: {exc}", file=sys.stderr)
        return 1

    print(f"Rendered Airflow connections file: {sys.argv[2]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

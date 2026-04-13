from __future__ import annotations

import json
import logging
import os
import smtplib
import sqlite3
from datetime import datetime, timezone, timedelta
from email.message import EmailMessage
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logger = logging.getLogger("alertservice")
logging.basicConfig(level=logging.INFO)

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(APP_DIR, "storage", "alerts.db")
CONFIG_PATH = os.path.join(APP_DIR, "config", "pipelines.yaml")
ENV_PATH = os.path.join(APP_DIR, ".env")


class NotificationPayload(BaseModel):
    summary: Optional[Dict[str, Any]] = None


app = FastAPI(title="alertservice", version="1.0")


def _load_config() -> Dict[str, Any]:
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = _get_db()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pipeline TEXT NOT NULL,
                execution_id TEXT,
                status TEXT,
                acceptance_rate REAL,
                rows_failed INTEGER,
                failure_phase TEXT,
                failure_message TEXT,
                generated_at_utc TEXT,
                received_at_utc TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _store_summary(summary: Dict[str, Any]) -> None:
    conn = _get_db()
    try:
        conn.execute(
            """
            INSERT INTO alerts (
                pipeline, execution_id, status, acceptance_rate, rows_failed,
                failure_phase, failure_message, generated_at_utc, received_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                summary.get("pipeline"),
                summary.get("execution_id"),
                summary.get("status"),
                summary.get("acceptance_rate"),
                summary.get("rows_failed"),
                summary.get("failure_phase"),
                summary.get("failure_message"),
                summary.get("generated_at_utc"),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _send_email(subject: str, body: str) -> None:
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "25"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    email_from = os.getenv("EMAIL_FROM")
    email_to = os.getenv("EMAIL_TO", "")
    use_tls = os.getenv("SMTP_USE_TLS", "false").lower() == "true"

    if not smtp_host or not email_from or not email_to:
        raise RuntimeError("SMTP_HOST, EMAIL_FROM, and EMAIL_TO must be set")

    msg = EmailMessage()
    msg["From"] = email_from
    msg["To"] = email_to
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        if use_tls:
            server.starttls()
        if smtp_user and smtp_password:
            server.login(smtp_user, smtp_password)
        server.send_message(msg)


def _format_summary(summary: Dict[str, Any]) -> str:
    return json.dumps(summary, ensure_ascii=False, indent=2, default=str)


def _evaluate_cumulative_warn(pipeline: str, config: Dict[str, Any]) -> bool:
    warning_cfg = config.get(pipeline, {})
    window_cfg = warning_cfg.get("warning_window", {})
    thresholds = warning_cfg.get("warning_thresholds", {})

    if not window_cfg:
        return False

    window_type = window_cfg.get("type", "time")
    window_value = int(window_cfg.get("value", 24))

    conn = _get_db()
    try:
        if window_type == "count":
            rows = conn.execute(
                """
                SELECT status, rows_failed, acceptance_rate, received_at_utc
                FROM alerts
                WHERE pipeline = ?
                ORDER BY received_at_utc DESC
                LIMIT ?
                """,
                (pipeline, window_value),
            ).fetchall()
        else:
            since = datetime.now(timezone.utc) - timedelta(hours=window_value)
            rows = conn.execute(
                """
                SELECT status, rows_failed, acceptance_rate, received_at_utc
                FROM alerts
                WHERE pipeline = ? AND received_at_utc >= ?
                ORDER BY received_at_utc DESC
                """,
                (pipeline, since.isoformat()),
            ).fetchall()
    finally:
        conn.close()

    if not rows:
        return False

    total_failed_rows = sum((r["rows_failed"] or 0) for r in rows)
    failed_ratios = [1 - (r["acceptance_rate"] or 0.0) for r in rows]
    avg_failed_ratio = sum(failed_ratios) / len(failed_ratios)

    max_failed_rows = thresholds.get("max_failed_rows")
    max_failed_ratio = thresholds.get("max_failed_ratio")
    max_consecutive_warn = thresholds.get("max_consecutive_warn")

    if max_failed_rows is not None and total_failed_rows >= max_failed_rows:
        return True
    if max_failed_ratio is not None and avg_failed_ratio >= max_failed_ratio:
        return True
    if max_consecutive_warn is not None:
        consecutive = 0
        for row in rows:
            if row["status"] == "WARN":
                consecutive += 1
                if consecutive >= max_consecutive_warn:
                    return True
            else:
                break
    return False


@app.on_event("startup")
def on_startup() -> None:
    if os.path.exists(ENV_PATH):
        load_dotenv(ENV_PATH)
    _init_db()


@app.post("/notify")
def notify(payload: NotificationPayload) -> Dict[str, Any]:
    summary = payload.summary or {}
    if not summary:
        raise HTTPException(status_code=400, detail="summary is required")

    pipeline = summary.get("pipeline", "unknown")
    status = summary.get("status", "WARN")

    _store_summary(summary)

    config = _load_config()
    notify_on_fail = config.get(pipeline, {}).get("notify_on_fail", True)
    notify_on_warn = config.get(pipeline, {}).get("notify_on_warn", True)

    subject_prefix = os.getenv("EMAIL_SUBJECT_PREFIX", "[DQ]")
    subject = f"{subject_prefix} {pipeline} - {status}"
    body = _format_summary(summary)

    if status == "FAIL" and notify_on_fail:
        _send_email(subject, body)
        return {"status": "sent", "reason": "fail"}

    if status == "WARN" and notify_on_warn:
        if _evaluate_cumulative_warn(pipeline, config):
            _send_email(subject, body)
            return {"status": "sent", "reason": "cumulative_warn"}

    return {"status": "stored"}

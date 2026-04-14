import logging
import os
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List

from .config import get_settings

logger = logging.getLogger(__name__)


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(get_settings().alerts_db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    db_path = get_settings().alerts_db_path
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = get_db()
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


def store_summary(summary: Dict[str, Any]) -> None:
    conn = get_db()
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


def query_window(
    pipeline: str, window_type: str, window_value: int
) -> List[sqlite3.Row]:
    conn = get_db()
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
        return rows
    finally:
        conn.close()

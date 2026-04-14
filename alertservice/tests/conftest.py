import os
import sys

import pytest


def pytest_configure():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)


@pytest.fixture
def pipeline_config():
    return {
        "transformlivedata": {
            "notify_on_fail": True,
            "notify_on_warn": True,
            "warning_window": {"type": "time", "value": 24},
            "warning_thresholds": {
                "max_failed_rows": 500,
                "max_failed_ratio": 0.02,
                "max_consecutive_warn": 3,
            },
        }
    }


@pytest.fixture
def valid_summary_fail():
    return {
        "pipeline": "transformlivedata",
        "execution_id": "exec-001",
        "status": "FAIL",
        "acceptance_rate": 0.80,
        "rows_failed": 200,
        "failure_phase": "validation",
        "failure_message": "Too many failures",
        "generated_at_utc": "2026-04-14T12:00:00Z",
    }


@pytest.fixture
def valid_summary_warn():
    return {
        "pipeline": "transformlivedata",
        "execution_id": "exec-002",
        "status": "WARN",
        "acceptance_rate": 0.98,
        "rows_failed": 9,
        "failure_phase": None,
        "failure_message": None,
        "generated_at_utc": "2026-04-14T12:00:00Z",
    }


@pytest.fixture
def valid_summary_pass():
    return {
        "pipeline": "transformlivedata",
        "execution_id": "exec-003",
        "status": "PASS",
        "acceptance_rate": 1.0,
        "rows_failed": 0,
        "failure_phase": None,
        "failure_message": None,
        "generated_at_utc": "2026-04-14T12:00:00Z",
    }


@pytest.fixture
def email_config():
    from src.infra.config import EmailSendConfig

    return EmailSendConfig(
        smtp_host="smtp.example.com",
        smtp_port=587,
        user="user@example.com",
        password="secret",
        smtp_use_tls=True,
        email_from="from@example.com",
        email_to="to@example.com",
    )

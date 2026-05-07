## Purpose of this subproject

Receive pipeline quality summaries via webhook and send email notifications, including immediate alerts for failures and cumulative alerts for warnings.

## What this subproject does

- exposes an HTTP `POST /notify` endpoint to receive quality `summary` payloads
- stores all events in SQLite for historical analysis
- sends an immediate email when `status=FAIL`
- sends warning emails only when cumulative thresholds are reached
- writes logs both to a rotating file and to stdout

## Prerequisites

- SMTP service configured and reachable
- `.env` file with SMTP credentials
- per-pipeline configuration file in `alertservice/config/pipelines.yaml`

## Configuration

### Endpoint

`POST /notify`

Expected payload example:

```json
{
  "summary": {
    "pipeline": "transformlivedata",
    "execution_id": "...",
    "status": "WARN",
    "acceptance_rate": 0.99,
    "items_failed": 9,
    "failure_phase": null,
    "failure_message": null,
    "generated_at_utc": "2026-04-13T12:34:56Z"
  }
}
```

### Per-pipeline configuration

File: `alertservice/config/pipelines.yaml`

All fields below are required. The service fails at startup if any of them are missing or invalid.

Example:

```yaml
transformlivedata:
  warning_window:
    type: "time"
    value: 24
  warning_thresholds:
    max_failed_items: 500
    max_failed_ratio: 0.02
    max_consecutive_warn: 3
  notify_on_fail: true
  notify_on_warn: true
```

### Environment variables (SMTP)

All variables are required. A template is available in `.env.example`.

The email subject is built dynamically as `{EMAIL_SUBJECT_PREFIX} {pipeline}: {status}`.

Example: `[DQ] status update for pipeline transformlivedata: FAIL`

Note: all fields are mandatory. If any field is missing, the service fails at startup.

## Unit tests

This subproject already includes unit tests focused on the core notification and formatting rules, using fakes to isolate external dependencies.

Current coverage:
- `tests/test_process_notification.py`: main processing flow, persistence, and alert sending
- `tests/test_notifications_evaluator.py`: cumulative warning evaluation
- `tests/test_notifier.py`: summary formatting and email integration

To run:

```bash
cd alertservice
pytest tests/
```

## Installing requirements

- `cd alertservice`
- `python3 -m venv .venv`
- `source .venv/bin/activate`
- `pip install -r requirements.txt`

## Running

### Manual execution (without Docker)

Create `alertservice/.env` based on `.env.example` and fill in all fields:

```bash
SMTP_HOST=
SMTP_PORT=
SMTP_USER=
SMTP_PASSWORD=
SMTP_USE_TLS=
EMAIL_FROM=
EMAIL_TO=
EMAIL_SUBJECT_PREFIX=
```

```bash
cd alertservice
uvicorn src.app:app --host 0.0.0.0 --port 8000
```

### Docker (via Docker Compose)

Non-sensitive configuration variables (`SMTP_HOST`, `SMTP_PORT`, and so on) are already declared in `docker-compose.yml`.

SMTP credentials are injected through substitution variables. Add the following to the root `.env` file:

```bash
ALERTSERVICE_SMTP_USER=
ALERTSERVICE_SMTP_PASSWORD=
```

```bash
docker compose up alertservice
```

## Notes

- Events are stored in SQLite (`alertservice/storage/alerts.db`)
- `FAIL` sends an immediate alert
- `WARN` sends an alert only if cumulative rules are reached
- Logs are written to `alertservice.log` with rotation and also sent to stdout

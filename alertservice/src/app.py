from __future__ import annotations

import logging
import os
import sys
from functools import partial
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .infra.config import (
    get_email_config,
    get_email_subject_prefix,
    load_env,
    load_pipeline_config,
    validate_required_env,
)
from .infra.storage import init_db, store_summary, query_window
from .infra.notifier import format_summary, send_email
from .process_notification import process_notification

logger = logging.getLogger("alertservice")

SRC_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SRC_DIR)
LOG_FILENAME = os.path.join(BASE_DIR, "alertservice.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILENAME, maxBytes=5 * 1024 * 1024, backupCount=5),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)

logger = logging.getLogger(__name__)


class NotificationPayload(BaseModel):
    summary: Optional[Dict[str, Any]] = None


app = FastAPI(title="alertservice", version="1.0")


@app.on_event("startup")
def on_startup() -> None:
    load_env()
    logger.info("Alertservice starting up")
    validate_required_env()
    init_db()


@app.on_event("shutdown")
def on_shutdown() -> None:
    logger.info("Alertservice stopped by user")


@app.post("/notify")
def notify(payload: NotificationPayload) -> Dict[str, Any]:
    try:
        email_config = get_email_config()
        send_email_fn = partial(send_email, email_config=email_config)
        return process_notification(
            payload.summary,
            pipeline_config=load_pipeline_config(),
            subject_prefix=get_email_subject_prefix(),
            store_summary=store_summary,
            query_window=query_window,
            send_email=send_email_fn,
            format_summary=format_summary,
        )
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Notification processing failed: %s", e)
        raise HTTPException(status_code=500, detail="notification processing failed")

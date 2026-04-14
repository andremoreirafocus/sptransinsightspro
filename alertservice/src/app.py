from __future__ import annotations

import logging
import sys
import os
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .infra.config import load_env, validate_required_env
from .infra.storage import init_db
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
logger.setLevel(logging.DEBUG)


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
        return process_notification(payload)
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Notification processing failed: %s", e)
        raise HTTPException(status_code=500, detail="notification processing failed")

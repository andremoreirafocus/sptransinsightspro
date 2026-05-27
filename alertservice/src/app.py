from __future__ import annotations

import logging
import sys
from functools import partial
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional, cast

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from .infra.config import (
    get_settings,
    get_email_send_config,
    load_pipeline_config,
    NotificationDeliveryError,
)
from .infra.storage import init_db, store_summary, query_window
from .infra.notifier import format_summary, send_email
from .process_notification import QueryWindow, process_notification

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)

logger = logging.getLogger(__name__)


class NotificationPayload(BaseModel):
    summary: Optional[Dict[str, Any]] = None


app = FastAPI(title="alertservice", version="1.0")


@app.on_event("startup")
def on_startup() -> None:
    settings = get_settings()
    file_handler = RotatingFileHandler(
        settings.log_file, maxBytes=5 * 1024 * 1024, backupCount=5
    )
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    logging.getLogger().addHandler(file_handler)
    logger.info("Alertservice starting up")
    app.state.pipeline_config = load_pipeline_config()
    init_db()


@app.on_event("shutdown")
def on_shutdown() -> None:
    logger.info("Alertservice stopped by user")


@app.post("/notify")
def notify(request: Request, payload: NotificationPayload) -> Dict[str, Any]:
    try:
        settings = get_settings()
        send_email_fn = partial(
            send_email, email_config=get_email_send_config(settings)
        )
        return process_notification(
            payload.summary,
            pipeline_config=request.app.state.pipeline_config,
            subject_prefix=settings.email_subject_prefix,
            store_summary=store_summary,
            query_window=cast(QueryWindow, query_window),
            send_email=send_email_fn,
            format_summary=format_summary,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except NotificationDeliveryError as e:
        logger.error("Email delivery error: %s", e)
        raise HTTPException(status_code=502, detail="email delivery failed")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Notification processing failed: %s", e)
        raise HTTPException(status_code=500, detail="notification processing failed")

import json
import logging
import os
import smtplib
from email.message import EmailMessage
from typing import Any, Dict

logger = logging.getLogger(__name__)


def format_summary(summary: Dict[str, Any]) -> str:
    return json.dumps(summary, ensure_ascii=False, indent=2, default=str)


def send_email(subject: str, body: str) -> None:
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "25"))
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    email_from = os.getenv("EMAIL_FROM")
    email_to = os.getenv("EMAIL_TO", "")
    use_tls = os.getenv("SMTP_USE_TLS", "false").lower() == "true"

    if not smtp_host or not email_from or not email_to:
        raise RuntimeError("SMTP_HOST, EMAIL_FROM, and EMAIL_TO must be set")

    logger.debug(
        "SMTP config: host=%s port=%s user_set=%s tls=%s",
        smtp_host,
        smtp_port,
        bool(smtp_user),
        use_tls,
    )

    msg = EmailMessage()
    msg["From"] = email_from
    msg["To"] = email_to
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.set_debuglevel(1)
        if use_tls:
            server.starttls()
        if smtp_user and smtp_password:
            server.login(smtp_user, smtp_password)
        server.send_message(msg)

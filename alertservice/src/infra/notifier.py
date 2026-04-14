import json
import logging
import smtplib
from email.message import EmailMessage
from typing import Any, Callable, Dict

from .config import EmailSendConfig

logger = logging.getLogger(__name__)


def format_summary(summary: Dict[str, Any]) -> str:
    return json.dumps(summary, ensure_ascii=False, indent=2, default=str)


def send_email(
    subject: str,
    body: str,
    email_config: EmailSendConfig,
    smtp_client_factory: Callable[[str, int], smtplib.SMTP] = smtplib.SMTP,
) -> None:
    logger.debug(
        "SMTP config: host=%s port=%s user_set=%s tls=%s",
        email_config.smtp_host,
        email_config.smtp_port,
        bool(email_config.user),
        email_config.smtp_use_tls,
    )

    msg = EmailMessage()
    msg["From"] = email_config.email_from
    msg["To"] = email_config.email_to
    msg["Subject"] = subject
    msg.set_content(body)

    with smtp_client_factory(email_config.smtp_host, email_config.smtp_port) as server:
        if logger.isEnabledFor(logging.DEBUG):
            server.set_debuglevel(1)
        if email_config.smtp_use_tls:
            server.starttls()
        server.login(email_config.user, email_config.password)
        server.send_message(msg)

import json
import smtplib

import pytest

from src.infra.config import NotificationDeliveryError
from src.infra.notifier import format_summary, send_email
from tests.fakes.fake_smtp_client import FakeSmtpClient


def test_send_email_calls_starttls_when_tls_true(email_config):
    client = FakeSmtpClient()
    send_email("subject", "body", email_config, smtp_client_factory=client)
    assert client.server.starttls_called is True


def test_send_email_skips_starttls_when_tls_false(email_config):
    from src.infra.config import EmailSendConfig

    config = EmailSendConfig(
        smtp_host=email_config.smtp_host,
        smtp_port=email_config.smtp_port,
        user=email_config.user,
        password=email_config.password,
        smtp_use_tls=False,
        email_from=email_config.email_from,
        email_to=email_config.email_to,
    )
    client = FakeSmtpClient()
    send_email("subject", "body", config, smtp_client_factory=client)
    assert client.server.starttls_called is False


def test_send_email_message_delivered(email_config):
    client = FakeSmtpClient()
    send_email("Test subject", "Test body", email_config, smtp_client_factory=client)
    assert len(client.server.sent_messages) == 1
    msg = client.server.sent_messages[0]
    assert msg["Subject"] == "Test subject"
    assert msg["From"] == email_config.email_from
    assert msg["To"] == email_config.email_to


def test_send_email_raises_delivery_error_on_smtp_exception(email_config):
    client = FakeSmtpClient(raise_on=smtplib.SMTPException)
    with pytest.raises(NotificationDeliveryError):
        send_email("subject", "body", email_config, smtp_client_factory=client)


def test_send_email_raises_delivery_error_on_os_error(email_config):
    client = FakeSmtpClient(raise_on=OSError)
    with pytest.raises(NotificationDeliveryError):
        send_email("subject", "body", email_config, smtp_client_factory=client)


def test_format_summary_is_valid_json():
    summary = {"pipeline": "test", "status": "FAIL", "items_failed": 5}
    result = format_summary(summary)
    parsed = json.loads(result)
    assert parsed["pipeline"] == "test"
    assert parsed["status"] == "FAIL"

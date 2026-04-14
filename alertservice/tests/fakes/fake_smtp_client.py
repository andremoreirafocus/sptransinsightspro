import smtplib
from typing import Optional, Type


class FakeSmtpServer:
    def __init__(self, raise_on: Optional[Type[Exception]] = None):
        self._raise_on = raise_on
        self.starttls_called = False
        self.login_called = False
        self.sent_messages = []

    def set_debuglevel(self, level: int) -> None:
        pass

    def starttls(self) -> None:
        if self._raise_on and issubclass(self._raise_on, smtplib.SMTPException):
            raise self._raise_on("starttls failed")
        self.starttls_called = True

    def login(self, user: str, password: str) -> None:
        self.login_called = True

    def send_message(self, msg) -> None:
        self.sent_messages.append(msg)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class FakeSmtpClient:
    def __init__(self, raise_on: Optional[Type[Exception]] = None):
        self._raise_on = raise_on
        self.server: Optional[FakeSmtpServer] = None

    def __call__(self, host: str, port: int) -> FakeSmtpServer:
        if self._raise_on and issubclass(self._raise_on, OSError):
            raise self._raise_on("connection refused")
        self.server = FakeSmtpServer(raise_on=self._raise_on)
        return self.server

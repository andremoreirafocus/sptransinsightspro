from typing import List, Tuple


class FakeEmailSender:
    def __init__(self):
        self.sent: List[Tuple[str, str]] = []

    def __call__(self, subject: str, body: str) -> None:
        self.sent.append((subject, body))

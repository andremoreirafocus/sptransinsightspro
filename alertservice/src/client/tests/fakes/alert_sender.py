class FakeAlertSender:
    def __init__(self, raise_on_call=False):
        self.raise_on_call = raise_on_call
        self.calls = []

    def __call__(self, webhook_url, report):
        self.calls.append({"webhook_url": webhook_url, "report": report})
        if self.raise_on_call:
            raise RuntimeError("alert send failed")

class FakeHttpResponse:
    def __init__(self, status_code=200, text="true", json_data=None):
        self.status_code = status_code
        self.text = text
        self._json_data = json_data or {}

    def json(self):
        return self._json_data

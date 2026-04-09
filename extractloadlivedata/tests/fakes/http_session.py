from tests.fakes.http_response import FakeHttpResponse


class FakeHttpSession:
    def __init__(self, auth_ok=True, pos_ok=True, payload=None):
        self.auth_ok = auth_ok
        self.pos_ok = pos_ok
        self.payload = payload or {"hr": "09:00", "l": []}
        self.post_calls = []
        self.get_calls = []

    def post(self, url):
        self.post_calls.append(url)
        if self.auth_ok:
            return FakeHttpResponse(status_code=200, text="true")
        return FakeHttpResponse(status_code=401, text="false")

    def get(self, url):
        self.get_calls.append(url)
        if self.pos_ok:
            return FakeHttpResponse(status_code=200, json_data=self.payload)
        return FakeHttpResponse(status_code=500, json_data={})

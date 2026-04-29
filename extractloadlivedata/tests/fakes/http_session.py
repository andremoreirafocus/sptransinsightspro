from tests.fakes.http_response import FakeHttpResponse


class FakeHttpSession:
    def __init__(
        self,
        auth_ok=True,
        pos_ok=True,
        payload=None,
        raise_on_post=None,
        raise_on_get=None,
    ):
        self.auth_ok = auth_ok
        self.pos_ok = pos_ok
        self.payload = payload or {"hr": "09:00", "l": []}
        self.raise_on_post = raise_on_post
        self.raise_on_get = raise_on_get
        self.post_calls = []
        self.get_calls = []

    def post(self, url, **kwargs):
        self.post_calls.append((url, kwargs))
        if self.raise_on_post is not None:
            raise self.raise_on_post
        if self.auth_ok:
            return FakeHttpResponse(status_code=200, text="true")
        return FakeHttpResponse(status_code=401, text="false")

    def get(self, url, **kwargs):
        self.get_calls.append((url, kwargs))
        if self.raise_on_get is not None:
            raise self.raise_on_get
        if self.pos_ok:
            return FakeHttpResponse(status_code=200, json_data=self.payload)
        return FakeHttpResponse(status_code=500, json_data={})

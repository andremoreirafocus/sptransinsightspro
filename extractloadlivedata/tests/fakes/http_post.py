from tests.fakes.http_response import FakeHttpResponse


class FakeHttp:
    def __init__(self, status_code=200, text="true"):
        self.status_code = status_code
        self.text = text
        self.post_calls = []

    def post(self, url, **_kwargs):
        self.post_calls.append(url)
        return FakeHttpResponse(status_code=self.status_code, text=self.text)

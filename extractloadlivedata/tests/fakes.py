class FakeObjectStorageClient:
    def __init__(self):
        self.created_bucket = False
        self.put_calls = []

    def bucket_exists(self, _bucket_name):
        return self.created_bucket

    def make_bucket(self, _bucket_name):
        self.created_bucket = True

    def put_object(self, bucket_name, object_name, data, length, content_type):
        self.put_calls.append(
            {
                "bucket_name": bucket_name,
                "object_name": object_name,
                "data": data,
                "length": length,
                "content_type": content_type,
            }
        )


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


class FakeHttpResponse:
    def __init__(self, status_code=200, text="true", json_data=None):
        self.status_code = status_code
        self.text = text
        self._json_data = json_data or {}

    def json(self):
        return self._json_data

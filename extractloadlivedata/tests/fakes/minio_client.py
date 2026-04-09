from types import SimpleNamespace


class FakeMinioClient:
    def __init__(
        self,
        bucket_exists=True,
        list_objects=None,
        raise_on_list=False,
        raise_on_get=False,
    ):
        self._bucket_exists = bucket_exists
        self._list_objects = list_objects or []
        self._raise_on_list = raise_on_list
        self._raise_on_get = raise_on_get
        self.created_bucket = False
        self.put_calls = []
        self.get_calls = []

    def bucket_exists(self, _bucket_name):
        return self._bucket_exists

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

    def list_objects(self, _bucket_name, prefix=None, recursive=True):
        if self._raise_on_list:
            raise RuntimeError("list error")
        return [
            SimpleNamespace(object_name=name) for name in self._list_objects
        ]

    def get_object(self, _bucket_name, object_name):
        if self._raise_on_get:
            raise RuntimeError("get error")
        self.get_calls.append(object_name)
        return SimpleNamespace(
            read=lambda: b"file-content",
            close=lambda: None,
            release_conn=lambda: None,
        )

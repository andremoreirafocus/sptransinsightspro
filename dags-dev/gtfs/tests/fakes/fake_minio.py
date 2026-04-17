class FakeMinioResponse:
    def __init__(self, payload, recorder):
        self.payload = payload
        self.recorder = recorder

    def read(self):
        return self.payload

    def close(self):
        self.recorder["response_closed"] = True

    def release_conn(self):
        self.recorder["response_released"] = True


class FakeMinioClient:
    payload = b""
    recorder = None

    @classmethod
    def configure(cls, recorder, payload):
        cls.recorder = recorder
        cls.payload = payload

    def __init__(self, endpoint, access_key, secret_key, secure):
        self.__class__.recorder["init"] = {
            "endpoint": endpoint,
            "access_key": access_key,
            "secret_key": secret_key,
            "secure": secure,
        }

    def get_object(self, bucket_name, object_name):
        self.__class__.recorder["get_object"] = (bucket_name, object_name)
        return FakeMinioResponse(self.__class__.payload, self.__class__.recorder)

    def put_object(self, **kwargs):
        data = kwargs["data"].read()
        self.__class__.recorder["put_object"] = {
            "bucket_name": kwargs["bucket_name"],
            "object_name": kwargs["object_name"],
            "length": kwargs["length"],
            "content_type": kwargs["content_type"],
            "payload": data,
        }

    def remove_object(self, bucket_name, object_name):
        self.__class__.recorder["remove_object"] = (bucket_name, object_name)

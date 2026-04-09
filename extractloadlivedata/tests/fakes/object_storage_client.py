class FakeObjectStorageClient:
    def __init__(self):
        self.created_bucket = False
        self.put_calls = []

    def bucket_exists(self, _bucket_name):
        return self.created_bucket

    def make_bucket(self, _bucket_name):
        self.created_bucket = True

    def put_object(self, bucket_name, object_name, data, length, content_type):
        buffer = data.read()
        self.put_calls.append(
            {
                "bucket_name": bucket_name,
                "object_name": object_name,
                "data": data,
                "buffer": buffer,
                "length": length,
                "content_type": content_type,
            }
        )

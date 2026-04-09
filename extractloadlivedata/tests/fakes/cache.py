class FakeCache:
    def __init__(self):
        self.store = {}

    def __iter__(self):
        return iter(self.store.keys())

    def __getitem__(self, key):
        return self.store[key]

    def __setitem__(self, key, value):
        self.store[key] = value

    def __delitem__(self, key):
        del self.store[key]

    def get(self, key):
        return self.store.get(key)


def fake_cache_factory(_cache_dir):
    return FakeCache()

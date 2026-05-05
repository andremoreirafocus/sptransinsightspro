class FakeAirflowVariable:
    values = {}

    @classmethod
    def get(cls, name, deserialize_json=False, default_var=None):
        if name in cls.values:
            return cls.values[name]
        return default_var


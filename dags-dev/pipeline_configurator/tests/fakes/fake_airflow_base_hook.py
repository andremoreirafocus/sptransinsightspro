class FakeAirflowBaseHook:
    connections = {}

    @classmethod
    def get_connection(cls, name):
        return cls.connections[name]


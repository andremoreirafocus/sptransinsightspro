class FakeAirflowConnection:
    def __init__(
        self,
        host="localhost",
        port=0,
        schema="",
        login="",
        password="",
        conn_type="",
        extra_dejson=None,
    ):
        self.host = host
        self.port = port
        self.schema = schema
        self.login = login
        self.password = password
        self.conn_type = conn_type
        self.extra_dejson = extra_dejson or {}


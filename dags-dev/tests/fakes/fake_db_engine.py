from contextlib import contextmanager


class FakeExecuteResult:
    def __init__(self, rowcount=0):
        self.rowcount = rowcount


class FakeDbConnection:
    def __init__(self, engine):
        self._engine = engine

    def execute(self, stmt, params=None):
        self._engine.executed_statements.append((str(stmt), params))
        return FakeExecuteResult(self._engine._rowcount)


class FakeDbEngine:
    def __init__(self, rowcount=0, raises=None):
        self.executed_statements = []
        self._rowcount = rowcount
        self._raises = raises

    @contextmanager
    def begin(self):
        if self._raises:
            raise self._raises
        yield FakeDbConnection(self)

    def dispose(self):
        pass


def make_fake_engine_factory(rowcount=0, raises=None):
    engine = FakeDbEngine(rowcount=rowcount, raises=raises)

    def factory(db_uri):
        return engine

    factory.engine = engine
    return factory

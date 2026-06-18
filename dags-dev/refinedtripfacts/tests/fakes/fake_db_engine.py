from contextlib import contextmanager
from typing import List, Tuple


class FakeRow:
    def __init__(self, value):
        self._value = value

    def __getitem__(self, index):
        return self._value


class FakeExecuteResult:
    def __init__(self, rowcount: int = 0, row=None):
        self.rowcount = rowcount
        self._row = row

    def fetchone(self):
        return self._row


class FakeDbConnection:
    def __init__(self, engine):
        self._engine = engine

    def execute(self, stmt, params=None):
        self._engine.executed_statements.append((str(stmt), params))
        return FakeExecuteResult(row=FakeRow(self._engine._scalar_count))


class FakeDbEngine:
    def __init__(self, scalar_count: int = 0, raises=None):
        self.executed_statements: List[Tuple] = []
        self._scalar_count = scalar_count
        self._raises = raises

    @contextmanager
    def begin(self):
        if self._raises:
            raise self._raises
        yield FakeDbConnection(self)

    def dispose(self):
        pass


def make_fake_engine_factory(scalar_count: int = 0, raises=None):
    engine = FakeDbEngine(scalar_count=scalar_count, raises=raises)

    def factory(db_uri):
        return engine

    factory.engine = engine
    return factory

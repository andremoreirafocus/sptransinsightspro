from contextlib import contextmanager
from datetime import datetime
from typing import Optional


class FakeRow:
    def __init__(self, value):
        self._value = value

    def __getitem__(self, index):
        return self._value


class FakeExecuteResult:
    def __init__(self, rowcount: int = 0, fetchone_value: Optional[datetime] = None):
        self.rowcount = rowcount
        self._fetchone_value = fetchone_value

    def fetchone(self) -> FakeRow:
        return FakeRow(self._fetchone_value)


class FakeDbConnection:
    def __init__(self, engine):
        self._engine = engine

    def execute(self, stmt, params=None):
        sql = str(stmt).upper()
        self._engine.executed_statements.append((str(stmt), params))
        if "MAX(" in sql and "SELECT" in sql:
            return FakeExecuteResult(fetchone_value=self._engine._max_trip_end_time)
        return FakeExecuteResult(rowcount=self._engine._rowcount)


class FakeDbEngine:
    def __init__(self, rowcount: int = 0, raises=None, max_trip_end_time: Optional[datetime] = None):
        self.executed_statements = []
        self._rowcount = rowcount
        self._raises = raises
        self._max_trip_end_time = max_trip_end_time

    @contextmanager
    def begin(self):
        if self._raises:
            raise self._raises
        yield FakeDbConnection(self)

    def dispose(self):
        pass


def make_fake_engine_factory(
    rowcount: int = 0,
    raises=None,
    max_trip_end_time: Optional[datetime] = None,
):
    engine = FakeDbEngine(
        rowcount=rowcount, raises=raises, max_trip_end_time=max_trip_end_time
    )

    def factory(db_uri):
        return engine

    factory.engine = engine
    return factory

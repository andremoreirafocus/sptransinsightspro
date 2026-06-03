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
    def __init__(self, rowcount: int = 0, raises=None, max_trip_end_time: Optional[datetime] = None, raises_on_nth_begin: Optional[int] = None):
        self.executed_statements = []
        self._rowcount = rowcount
        self._raises = raises
        self._max_trip_end_time = max_trip_end_time
        self._raises_on_nth_begin = raises_on_nth_begin
        self._begin_count = 0

    @contextmanager
    def begin(self):
        self._begin_count += 1
        if self._raises:
            raise self._raises
        if self._raises_on_nth_begin is not None and self._begin_count == self._raises_on_nth_begin:
            raise Exception(f"simulated failure on begin #{self._begin_count}")
        yield FakeDbConnection(self)

    def dispose(self):
        pass


def make_fake_engine_factory(
    rowcount: int = 0,
    raises=None,
    max_trip_end_time: Optional[datetime] = None,
    raises_on_nth_begin: Optional[int] = None,
):
    engine = FakeDbEngine(
        rowcount=rowcount,
        raises=raises,
        max_trip_end_time=max_trip_end_time,
        raises_on_nth_begin=raises_on_nth_begin,
    )

    def factory(db_uri):
        return engine

    factory.engine = engine
    return factory

from contextlib import contextmanager
from typing import Any, List, Optional, Tuple


class FakeRow:
    def __init__(self, *values: Any) -> None:
        self._values = values

    def __getitem__(self, index: int) -> Any:
        return self._values[index]


class FakeExecuteResult:
    def __init__(self, rowcount: int = 0, row: Optional[FakeRow] = None) -> None:
        self.rowcount = rowcount
        self._row = row

    def fetchone(self) -> Optional[FakeRow]:
        return self._row


class FakeDbConnection:
    def __init__(self, engine: "FakeDbEngine") -> None:
        self._engine = engine

    def execute(self, stmt: Any, params: Any = None) -> FakeExecuteResult:
        self._engine.executed_statements.append((str(stmt), params))
        if self._engine._responses:
            return self._engine._responses.pop(0)
        return FakeExecuteResult()


class FakeDbEngine:
    def __init__(
        self,
        responses: Optional[List[FakeExecuteResult]] = None,
        raises: Optional[Exception] = None,
    ) -> None:
        self.executed_statements: List[Tuple] = []
        self._responses: List[FakeExecuteResult] = list(responses or [])
        self._raises = raises

    @contextmanager
    def begin(self):
        if self._raises:
            raise self._raises
        yield FakeDbConnection(self)

    def dispose(self) -> None:
        pass


def make_fake_engine_factory(
    responses: Optional[List[FakeExecuteResult]] = None,
    scalar_count: int = 0,
    raises: Optional[Exception] = None,
) -> Any:
    if responses is None:
        responses = [FakeExecuteResult(row=FakeRow(scalar_count))]
    engine = FakeDbEngine(responses=responses, raises=raises)

    def factory(db_uri: str) -> FakeDbEngine:
        return engine

    factory.engine = engine
    return factory

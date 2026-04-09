from contextlib import contextmanager
from types import SimpleNamespace


def make_rows(dict_rows):
    return [SimpleNamespace(_mapping=row) for row in dict_rows]


def build_engine_factory(state, rows=None, raise_on_execute=False):
    def engine_factory(uri):
        state["uri"] = uri

        def execute(query, params=None):
            state.setdefault("execute_calls", []).append(
                {"query": query, "params": params}
            )
            if raise_on_execute:
                raise RuntimeError("db error")
            if rows is None:
                return None
            return SimpleNamespace(fetchall=lambda: rows)

        @contextmanager
        def begin():
            conn = SimpleNamespace(execute=execute)
            yield conn

        return SimpleNamespace(begin=begin)

    return engine_factory

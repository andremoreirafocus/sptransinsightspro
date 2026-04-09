from src.infra.sql_db_v2 import (
    execute_select_query,
    execute_update_query,
    save_row,
)
from tests.fakes.sqlalchemy_engine import build_engine_factory, make_rows


def build_connection():
    return {
        "host": "localhost",
        "port": 5432,
        "database": "db",
        "user": "user",
        "password": "pass",
    }


def test_save_row_missing_connection_key_raises():
    connection = build_connection()
    connection.pop("host")
    try:
        save_row(connection, "schema", "table", ("v",), ["col"])
    except KeyError:
        assert True
    else:
        assert False, "Expected KeyError for missing connection key"


def test_save_row_success_calls_engine():
    state = {}
    engine_factory = build_engine_factory(state)
    connection = build_connection()
    result = save_row(
        connection,
        "schema",
        "table",
        ("value",),
        ["col"],
        engine_factory=engine_factory,
    )
    assert result is True
    assert state["uri"] == "postgresql://user:pass@localhost:5432/db"
    assert len(state.get("execute_calls", [])) == 1
    assert state["execute_calls"][0]["params"] == {"col": "value"}


def test_save_row_returns_false_on_execute_error():
    state = {}
    engine_factory = build_engine_factory(state, raise_on_execute=True)
    connection = build_connection()
    result = save_row(
        connection,
        "schema",
        "table",
        ("value",),
        ["col"],
        engine_factory=engine_factory,
    )
    assert result is False


def test_execute_select_query_success_returns_rows():
    state = {}
    rows = make_rows([{"id": 1}, {"id": 2}])
    engine_factory = build_engine_factory(state, rows=rows)
    connection = build_connection()
    result = execute_select_query(
        connection, "SELECT * FROM table", engine_factory=engine_factory
    )
    assert result == [{"id": 1}, {"id": 2}]
    assert state["uri"] == "postgresql://user:pass@localhost:5432/db"
    assert len(state.get("execute_calls", [])) == 1


def test_execute_select_query_returns_empty_on_error():
    state = {}
    engine_factory = build_engine_factory(state, raise_on_execute=True)
    connection = build_connection()
    result = execute_select_query(
        connection, "SELECT * FROM table", engine_factory=engine_factory
    )
    assert result == []


def test_execute_update_query_success_with_params():
    state = {}
    engine_factory = build_engine_factory(state)
    connection = build_connection()
    result = execute_update_query(
        connection,
        "UPDATE table SET name=:name WHERE id=:id",
        params={"name": "x", "id": 1},
        engine_factory=engine_factory,
    )
    assert result is True
    assert state["execute_calls"][0]["params"] == {"name": "x", "id": 1}


def test_execute_update_query_success_without_params():
    state = {}
    engine_factory = build_engine_factory(state)
    connection = build_connection()
    result = execute_update_query(
        connection,
        "DELETE FROM table",
        engine_factory=engine_factory,
    )
    assert result is True


def test_execute_update_query_returns_false_on_error():
    state = {}
    engine_factory = build_engine_factory(state, raise_on_execute=True)
    connection = build_connection()
    result = execute_update_query(
        connection,
        "DELETE FROM table",
        engine_factory=engine_factory,
    )
    assert result is False

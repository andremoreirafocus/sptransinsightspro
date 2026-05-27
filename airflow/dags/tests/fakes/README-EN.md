# Shared Fakes

This directory contains reusable fake objects shared across pipeline tests. They follow the **Dependency Injection (DI)** pattern adopted in the project: production functions accept collaborators as optional parameters, and tests inject fakes in place of real objects, without any need for monkeypatching.

## Available fakes

### `FakeDuckDBConnection` — `fake_duckdb_connection.py`

Simulates a DuckDB connection. Use it when the function under test receives a `duckdb_client` parameter.

| Parameter | Type | Description |
|---|---|---|
| `df` | `pd.DataFrame` | DataFrame returned by `.df()`. Default: empty `DataFrame` |
| `raises` | `Exception` | Exception raised when calling `.execute()`. Default: `None` |

Implemented methods: `execute(sql)`, `df()`, `close()`. The `closed` attribute is set to `True` after `close()`.

**Example:**
```python
from fakes.fake_duckdb_connection import FakeDuckDBConnection

# Simulate data returned by DuckDB
fake = FakeDuckDBConnection(df=pd.DataFrame({"col": [1, 2]}))
result = my_service(config, duckdb_client=fake)

# Simulate connection failure
fake = FakeDuckDBConnection(raises=RuntimeError("connection refused"))
with pytest.raises(RuntimeError):
    my_service(config, duckdb_client=fake)
```

---

### `FakeDbEngine` and `make_fake_engine_factory` — `fake_db_engine.py`

Simulates a SQLAlchemy engine (PostgreSQL). Use it when the function under test receives an `engine_factory` parameter.

`make_fake_engine_factory` is the recommended entry point: it creates a factory that always returns the same `FakeDbEngine` instance and exposes that engine through `.engine` for assertions.

| Parameter | Type | Description |
|---|---|---|
| `rowcount` | `int` | `rowcount` value returned by `execute`. Default: `0` |
| `raises` | `Exception` | Exception raised when entering the `begin()` context manager. Default: `None` |

The engine records all calls in `engine.executed_statements` as a list of `(str(stmt), params)`.

**Example:**
```python
from fakes.fake_db_engine import make_fake_engine_factory

# Simulate a successful insert
factory = make_fake_engine_factory(rowcount=1)
my_service(config, engine_factory=factory)
assert len(factory.engine.executed_statements) == 1

# Simulate a database failure
factory = make_fake_engine_factory(raises=Exception("DB unavailable"))
with pytest.raises(Exception, match="DB unavailable"):
    my_service(config, engine_factory=factory)
```

---

### `FakeObject` — `fake_object_storage.py`

Simulates an object returned by the object storage client (MinIO) in list operations. It only exposes the `object_name` attribute.

**Example:**
```python
from fakes.fake_object_storage import FakeObject

def fake_list_objects(bucket, prefix):
    return [FakeObject(object_name=f"{prefix}data.parquet")]

result = my_service(config, list_fn=fake_list_objects)
```

---

## When to create a new fake

Create a new fake in this directory when the same collaborator needs to be replaced in **more than one pipeline**. For collaborators specific to a single pipeline, the fake can stay in that pipeline's own `tests/` directory.

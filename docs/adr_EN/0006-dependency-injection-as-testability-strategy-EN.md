# ADR-0006: Dependency injection as a testability strategy

**Date:** 2026-04-15  
**Status:** Accepted

## Context

Pipeline services depend on external components: DuckDB connections, PostgreSQL engines, MinIO clients, and functions for reading and writing on disk. Testing these services with real dependencies would require live infrastructure, such as MinIO, PostgreSQL, and DuckDB configured with S3 access, making tests slow, fragile, and impossible to run in environments without that infrastructure.

The conventional Python approach for decoupling code from dependencies in tests is using `unittest.mock.patch` or pytest `monkeypatch`, temporarily overriding module or class attributes with mock objects during test execution.

This approach has practical limitations:
- Patches are coupled to the import path, for example `mock.patch("pipeline.services.service.client.method")`, and break silently under refactors.
- Mocks that only define `return_value` do not verify whether the function called the right methods with the correct arguments; they only verify the returned value.
- Patches scattered throughout tests make traceability harder: when reading a test, it is difficult to see which dependencies were replaced.

## Decision

Adopt **dependency injection through optional parameters** in all testable services: external components are accepted as parameters with defaults pointing to the production implementation.

```python
# Production: use the real client
def load_trip_details(config, duckdb_client=None):
    con = duckdb_client or get_duckdb_connection(config["connections"])
    ...

# Test: inject a fake
def test_load_trip_details_returns_dataframe():
    fake = FakeDuckDBConnection(df=expected_df)
    result = load_trip_details(config, duckdb_client=fake)
    assert result.equals(expected_df)
```

Reusable fakes shared across pipelines live in `dags-dev/tests/fakes/`:
- `FakeDuckDBConnection`, which simulates a DuckDB connection with configurable return values and exception simulation support.
- `FakeDbEngine` together with `make_fake_engine_factory`, which simulates a SQLAlchemy engine and allows inspection of executed statements.
- `FakeObject`, which simulates objects returned by MinIO listing operations.

Each pipeline `conftest.py` adds `dags-dev/tests/` to `sys.path`, making the fakes available through imports such as `from fakes.fake_duckdb_connection import FakeDuckDBConnection`.

## Alternatives considered

**`unittest.mock.patch` / `monkeypatch`:** Widely used and does not require changes to production code. However, it couples tests to import paths, hurts readability because replacement context is separated from assertions, and does not provide stateful fakes such as one that accumulates executed SQL statements for later verification.

**Integration tests with real infrastructure, for example pytest-docker:** Guarantees that code works with the real system. However, it dramatically increases execution time, requires Docker in the CI environment, and makes each test depend on infrastructure state such as residual data or network timeouts.

**Subclasses and formal interfaces through ABCs:** Defining protocols or abstract classes for each collaborator would be more formal and type-safe, but it adds maintenance overhead in a Python project where duck typing is sufficient and the contracts are simple.

## Consequences

**Positive:**
- Fast, deterministic tests with no real network, database, or disk calls.
- Fakes expressed as simple pure-Python classes, readable and maintainable without mock-library expertise.
- Stateful fakes: `FakeDbEngine.executed_statements` makes it possible to verify exactly which queries were executed, without mock call-magic assertions.
- Refactors of import paths do not break tests because injection is structural, not string-based.
- Shared fakes in `tests/fakes/` ensure consistent simulated behavior across all pipelines.

**Negative / Tradeoffs:**
- Function signatures become slightly longer when multiple collaborators are injected, for example `load_trip_details(config, duckdb_client=None, write_fn=None)`.
- It requires maintenance discipline: when a real collaborator interface changes, the corresponding fake must be updated manually because there is no automatic adherence check.
- The pattern only covers the services layer. The DAG script itself, `transformlivedata-v8.py`, remains uncovered because it depends on Airflow context. This is a known and accepted gap.

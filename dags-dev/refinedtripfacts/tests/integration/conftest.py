import os
from datetime import datetime, timedelta, timezone

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Single source of truth for the integration suite connection.
# To retarget the entire suite to a different instance, update .env — no test edits needed.
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: mark test as requiring a real PostgreSQL (opt-in; default unit run is DB-free)",
    )


@pytest.fixture(scope="session")
def test_config():
    return {
        "general": {
            "tables": {
                "finished_trips_table_name": "refined.finished_trips",
                "trip_facts_table_name": "refined.trip_facts",
                "dim_time_table_name": "refined.dim_time",
            },
            "quality": {
                "completeness_loss_rate_warn_threshold": 0.01,
                "completeness_loss_rate_fail_threshold": 0.05,
                "avg_speed_kmh_max": 120.0,
            },
        },
        "connections": {
            "database": {
                "host": os.environ["DB_HOST"],
                "port": int(os.environ["DB_PORT"]),
                "database": os.environ["DB_DATABASE"],
                "user": os.environ["DB_USER"],
                "password": os.environ["DB_PASSWORD"],
            }
        },
    }


@pytest.fixture(scope="session")
def pg_engine(test_config):
    db = test_config["connections"]["database"]
    uri = (
        f"postgresql://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['database']}"
    )
    engine = create_engine(uri)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def prod_guard(pg_engine):
    """Abort the session if the connected DB does not match DB_DATABASE from .env."""
    expected = os.environ["DB_DATABASE"]
    with pg_engine.connect() as conn:
        db_name = conn.execute(text("SELECT current_database()")).fetchone()[0]
    assert db_name == expected, (
        f"Safety abort: integration suite must run against '{expected}', "
        f"connected to '{db_name}'. Check tests/integration/.env."
    )


@pytest.fixture(scope="session", autouse=True)
def ensure_partitions(pg_engine, prod_guard):
    """Run pg_partman maintenance and pre-create the SP midnight partition window
    needed by test_dim_time_covers_midnight_boundary_trip."""
    now = datetime.now(timezone.utc)
    sp_midnight_utc = now.replace(hour=3, minute=0, second=0, microsecond=0)
    if sp_midnight_utc > now:
        sp_midnight_utc -= timedelta(days=1)

    # Timestamps for the two hourly partitions that straddle SP midnight.
    # strftime output is controlled (no user input), so embedding is safe.
    before_midnight = (sp_midnight_utc - timedelta(hours=1)).strftime("%Y-%m-%d %H:00:00+00")
    at_midnight = sp_midnight_utc.strftime("%Y-%m-%d %H:00:00+00")

    with pg_engine.begin() as conn:
        conn.execute(text("SELECT partman.run_maintenance('refined.finished_trips')"))
        conn.execute(text("SELECT partman.run_maintenance('refined.trip_facts')"))
        conn.execute(
            text(
                f"SELECT partman.create_partition_time("
                f"    'refined.finished_trips',"
                f"    ARRAY['{before_midnight}'::timestamptz, '{at_midnight}'::timestamptz]"
                f")"
            )
        )


@pytest.fixture(autouse=True)
def clean_tables(pg_engine):
    """Truncate all test tables before each test for full isolation."""
    with pg_engine.begin() as conn:
        conn.execute(
            text(
                "TRUNCATE refined.trip_facts, refined.dim_time, refined.finished_trips CASCADE"
            )
        )
    yield

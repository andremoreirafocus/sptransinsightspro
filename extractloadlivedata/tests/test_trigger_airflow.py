import pytest

from src.services.trigger_airflow import (
    create_pending_invokation,
    get_pending_invokations,
    get_utc_logical_date_from_file,
    remove_pending_invokation,
    trigger_airflow_dag_run,
    trigger_pending_airflow_dag_invokations,
)
from src.services.exceptions import IngestNotificationError
from tests.fakes.cache import fake_cache_factory
from tests.fakes.http_post import FakeHttp


def test_get_utc_logical_date_from_file_format():
    logical_date = get_utc_logical_date_from_file("posicoes_onibus-202604090910.json")
    assert logical_date.endswith("Z")
    assert "T" in logical_date


def test_pending_invokations_cache_flow():
    config = {"INVOKATIONS_CACHE_DIR": "/tmp/cache"}
    marker = "posicoes_onibus-202604090910.json"
    create_pending_invokation(
        config, marker, cache_factory=fake_cache_factory
    )
    pending = get_pending_invokations(
        config, cache_factory=fake_cache_factory
    )
    assert len(pending) == 1
    remove_pending_invokation(
        config, pending[0], cache_factory=fake_cache_factory
    )
    pending_after = get_pending_invokations(
        config, cache_factory=fake_cache_factory
    )
    assert pending_after == []


def test_trigger_airflow_dag_run_success_and_failure():
    config = {
        "AIRFLOW_USER": "user",
        "AIRFLOW_PASSWORD": "pass",
        "AIRFLOW_WEBSERVER": "localhost",
        "AIRFLOW_DAG_NAME": "dag",
    }
    http_ok = FakeHttp(status_code=200, text="true")
    assert (
        trigger_airflow_dag_run(
            config,
            "posicoes_onibus-202604090910.json",
            post_fn=http_ok.post,
        )
        is True
    )
    http_fail = FakeHttp(status_code=500, text="false")
    with pytest.raises(IngestNotificationError, match="airflow dag trigger failed"):
        trigger_airflow_dag_run(
            config,
            "posicoes_onibus-202604090910.json",
            post_fn=http_fail.post,
        )


def test_trigger_pending_airflow_dag_invokations_removes_on_success():
    config = {
        "INVOKATIONS_CACHE_DIR": "/tmp/cache",
        "AIRFLOW_USER": "user",
        "AIRFLOW_PASSWORD": "pass",
        "AIRFLOW_WEBSERVER": "localhost",
        "AIRFLOW_DAG_NAME": "dag",
    }
    marker = "posicoes_onibus-202604090910.json"
    create_pending_invokation(
        config, marker, cache_factory=fake_cache_factory
    )

    http_ok = FakeHttp(status_code=200, text="true")
    trigger_pending_airflow_dag_invokations(
        config,
        post_fn=http_ok.post,
        cache_factory=fake_cache_factory,
    )
    pending_after = get_pending_invokations(
        config, cache_factory=fake_cache_factory
    )
    assert pending_after == []

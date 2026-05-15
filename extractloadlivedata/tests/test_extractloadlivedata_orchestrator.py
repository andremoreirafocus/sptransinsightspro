from dataclasses import replace
import json
import logging

from src.orchestration_dependencies import Services
from src.extractloadlivedata import extractloadlivedata
from src.services.exceptions import (
    IngestNotificationError,
    LocalIngestBufferSaveError,
    PositionsDownloadError,
)
from tests.fakes.alert_sender import FakeAlertSender


def _find_event_payload(caplog, event_name: str):
    for record in caplog.records:
        try:
            payload = json.loads(record.message)
        except Exception:
            continue
        if payload.get("event") == event_name:
            return payload
    return None


def _build_services(call_log, pending_list, storage_success=True):
    def extract_buses_positions_with_retries(_config, with_metrics=False):
        call_log.append("extract_buses_positions_with_retries")
        payload = {"payload": True}
        if with_metrics:
            return {"result": payload, "metrics": {"retries": 0}}
        return payload

    def get_buses_positions_with_metadata(_payload):
        call_log.append("get_buses_positions_with_metadata")
        return (
            {
                "metadata": {"extracted_at": "2026-05-13T15:30:45.123456+00:00"},
                "payload": _payload,
            },
            {},
        )

    def save_bus_positions_to_local_volume(_config, _buses_positions):
        call_log.append("save_bus_positions_to_local_volume")

    def save_bus_positions_to_storage_with_retries(
        _config, _content, with_metrics=False
    ):
        call_log.append("save_bus_positions_to_storage_with_retries")
        if with_metrics:
            return {"result": None, "metrics": {"retries": 0}}
        return storage_success

    def load_bus_positions_from_local_volume_file(_folder, _filename):
        call_log.append("load_bus_positions_from_local_volume_file")
        return {"file": _filename}

    def remove_local_file(_config, _content):
        call_log.append("remove_local_file")

    def get_pending_storage_save_list(_config):
        call_log.append("get_pending_storage_save_list")
        return pending_list

    def create_pending_invokation(_config, _filename):
        call_log.append("create_pending_invokation")

    def trigger_pending_airflow_dag_invokations(_config, with_metrics=False):
        call_log.append("trigger_pending_airflow_dag_invokations")
        if with_metrics:
            return {"result": None, "metrics": {"success": 1, "failed": 0, "retries": 0}}

    def create_pending_processing_request(_config, _filename):
        call_log.append("create_pending_processing_request")

    def trigger_pending_processing_requests(_config, with_metrics=False):
        call_log.append("trigger_pending_processing_requests")
        if with_metrics:
            return {"result": None, "metrics": {"success": 1, "failed": 0, "retries": 0}}

    return Services(
        extract_buses_positions_with_retries=extract_buses_positions_with_retries,
        get_buses_positions_with_metadata=get_buses_positions_with_metadata,
        save_bus_positions_to_local_volume=save_bus_positions_to_local_volume,
        save_bus_positions_to_storage_with_retries=save_bus_positions_to_storage_with_retries,
        load_bus_positions_from_local_volume_file=load_bus_positions_from_local_volume_file,
        remove_local_file=remove_local_file,
        get_pending_storage_save_list=get_pending_storage_save_list,
        create_pending_invokation=create_pending_invokation,
        trigger_pending_airflow_dag_invokations=trigger_pending_airflow_dag_invokations,
        create_pending_processing_request=create_pending_processing_request,
        trigger_pending_processing_requests=trigger_pending_processing_requests,
    )


def test_extractloadlivedata_missing_notification_engine_is_handled():
    call_log = []
    services = _build_services(call_log, pending_list=[])
    config = {"INGEST_BUFFER_PATH": "/tmp/ingest"}
    extractloadlivedata(config=config, services=services)
    assert call_log == []


def test_extractloadlivedata_airflow_branch_uses_airflow_notifications():
    call_log = []
    services = _build_services(call_log, pending_list=["posicoes_onibus-202605131530.json"])
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "airflow",
        "NOTIFICATIONS_WEBHOOK_URL": "",
    }
    extractloadlivedata(config=config, services=services)
    assert "create_pending_invokation" in call_log
    assert "trigger_pending_airflow_dag_invokations" in call_log
    assert "create_pending_processing_request" not in call_log
    assert "trigger_pending_processing_requests" not in call_log


def test_extractloadlivedata_processing_requests_branch_uses_processing_requests_notifications():
    call_log = []
    alerts = FakeAlertSender()
    services = _build_services(call_log, pending_list=["posicoes_onibus-202605131530.json"])
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    extractloadlivedata(config=config, services=services, send_alert_fn=alerts)
    assert "create_pending_processing_request" in call_log
    assert "trigger_pending_processing_requests" in call_log
    assert "create_pending_invokation" not in call_log
    assert "trigger_pending_airflow_dag_invokations" not in call_log


def test_extractloadlivedata_positions_download_error_is_handled():
    call_log = []
    alerts = FakeAlertSender()

    def extract_raises(_config, with_metrics=False):
        call_log.append("extract_buses_positions_with_retries")
        raise PositionsDownloadError("download failed")

    services = _build_services(call_log, pending_list=["posicoes_onibus-202605131530.json"])
    services = replace(services, extract_buses_positions_with_retries=extract_raises)
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    extractloadlivedata(config=config, services=services, send_alert_fn=alerts)
    assert "extract_buses_positions_with_retries" in call_log
    assert "get_pending_storage_save_list" in call_log


def test_extractloadlivedata_local_save_error_keeps_pending_processing():
    call_log = []
    alerts = FakeAlertSender()

    def save_local_raises(_config, _buses_positions):
        call_log.append("save_bus_positions_to_local_volume")
        raise LocalIngestBufferSaveError("local save failed")

    services = _build_services(call_log, pending_list=["posicoes_onibus-202605131530.json"])
    services = replace(services, save_bus_positions_to_local_volume=save_local_raises)
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    extractloadlivedata(config=config, services=services, send_alert_fn=alerts)
    assert "save_bus_positions_to_local_volume" in call_log
    assert "save_bus_positions_to_storage_with_retries" in call_log
    assert "trigger_pending_processing_requests" in call_log


def test_extractloadlivedata_notification_error_is_handled():
    call_log = []
    alerts = FakeAlertSender()

    def trigger_raises(_config, with_metrics=False):
        call_log.append("trigger_pending_processing_requests")
        raise IngestNotificationError("notification failed")

    services = _build_services(call_log, pending_list=["posicoes_onibus-202605131530.json"])
    services = replace(services, trigger_pending_processing_requests=trigger_raises)
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    extractloadlivedata(config=config, services=services, send_alert_fn=alerts)
    assert "trigger_pending_processing_requests" in call_log


def test_extractloadlivedata_phase_mapping_positions_download_uses_severe_prefix():
    call_log = []
    alerts = FakeAlertSender()

    def extract_raises(_config, with_metrics=False):
        call_log.append("extract_buses_positions_with_retries")
        raise PositionsDownloadError("download failed")

    services = _build_services(call_log, pending_list=[])
    services = replace(services, extract_buses_positions_with_retries=extract_raises)
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    extractloadlivedata(config=config, services=services, send_alert_fn=alerts)
    assert len(alerts.calls) == 1
    summary = alerts.calls[0]["report"]
    assert summary["status"] == "FAIL"
    assert summary["failure_phase"] == "positions_download"
    assert summary["failure_message"].startswith("[SEVERE] non recoverable ")


def test_extractloadlivedata_unknown_phase_fallback_message():
    call_log = []
    alerts = FakeAlertSender()

    def extract_raises_unknown(_config, with_metrics=False):
        call_log.append("extract_buses_positions_with_retries")
        raise RuntimeError("unexpected boom")

    services = _build_services(call_log, pending_list=[])
    services = replace(services, extract_buses_positions_with_retries=extract_raises_unknown)
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    extractloadlivedata(config=config, services=services, send_alert_fn=alerts)
    assert len(alerts.calls) == 1
    summary = alerts.calls[0]["report"]
    assert summary["status"] == "FAIL"
    assert summary["failure_phase"] == "unknown"
    assert summary["failure_message"] == "ingest execution failed"


def test_extractloadlivedata_counting_rules_in_notification_failure():
    call_log = []
    alerts = FakeAlertSender()

    def trigger_raises_with_metrics(_config, with_metrics=False):
        call_log.append("trigger_pending_processing_requests")
        error = IngestNotificationError("notification failed")
        setattr(error, "metrics", {"success": 2, "failed": 1, "retries": 3})
        raise error

    services = _build_services(call_log, pending_list=["posicoes_onibus-202605131530.json"])
    services = replace(services, trigger_pending_processing_requests=trigger_raises_with_metrics)
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    extractloadlivedata(config=config, services=services, send_alert_fn=alerts)
    assert len(alerts.calls) == 1
    summary = alerts.calls[0]["report"]
    assert summary["status"] == "FAIL"
    assert summary["failure_phase"] == "ingest_notification"
    assert summary["items_total"] == 6
    assert summary["items_failed"] == 1
    assert summary["retries"] == 3


def test_extractloadlivedata_airflow_processing_requests_equivalent_summary():
    call_log_airflow = []
    alerts_airflow = FakeAlertSender()
    services_airflow = _build_services(call_log_airflow, pending_list=["posicoes_onibus-202605131530.json"])
    config_airflow = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "airflow",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    extractloadlivedata(
        config=config_airflow, services=services_airflow, send_alert_fn=alerts_airflow
    )

    call_log_pr = []
    alerts_pr = FakeAlertSender()
    services_pr = _build_services(call_log_pr, pending_list=["posicoes_onibus-202605131530.json"])
    config_pr = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    extractloadlivedata(config=config_pr, services=services_pr, send_alert_fn=alerts_pr)

    assert len(alerts_airflow.calls) == 1
    assert len(alerts_pr.calls) == 1
    summary_airflow = alerts_airflow.calls[0]["report"]
    summary_pr = alerts_pr.calls[0]["report"]
    assert summary_airflow["status"] == "PASS"
    assert summary_pr["status"] == "PASS"
    assert summary_airflow["items_total"] == summary_pr["items_total"] == 3
    assert summary_airflow["items_failed"] == summary_pr["items_failed"] == 0


def test_execution_id_is_iso_format_in_pass_summary():
    call_log = []
    alerts = FakeAlertSender()
    services = _build_services(call_log, pending_list=["posicoes_onibus-202605131530.json"])

    def get_metadata_with_extracted_at(payload):
        return (
            {
                "metadata": {
                    "extracted_at": "2026-05-13T15:30:45.123456+00:00",
                    "source": "sptrans_api_v2",
                    "total_vehicles": 100,
                },
                "payload": payload,
            },
            "ref_time",
        )

    services = replace(services, get_buses_positions_with_metadata=get_metadata_with_extracted_at)
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    extractloadlivedata(config=config, services=services, send_alert_fn=alerts)
    assert len(alerts.calls) == 1
    summary = alerts.calls[0]["report"]
    assert "execution_id" in summary
    execution_id = summary["execution_id"]
    assert "T" in execution_id
    assert "+00:00" in execution_id or execution_id.endswith("Z")


def test_execution_id_is_iso_format_in_failure_summary():
    call_log = []
    alerts = FakeAlertSender()

    def extract_raises(_config, with_metrics=False):
        call_log.append("extract_buses_positions_with_retries")
        raise PositionsDownloadError("download failed")

    services = _build_services(call_log, pending_list=[])
    services = replace(services, extract_buses_positions_with_retries=extract_raises)
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    extractloadlivedata(config=config, services=services, send_alert_fn=alerts)
    assert len(alerts.calls) == 1
    summary = alerts.calls[0]["report"]
    execution_id = summary["execution_id"]
    assert "T" in execution_id
    assert "+00:00" in execution_id or execution_id.endswith("Z")


def test_execution_report_success_without_pending_files(caplog):
    call_log = []
    alerts = FakeAlertSender()
    services = _build_services(call_log, pending_list=[])
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    with caplog.at_level(logging.INFO, logger="src.extractloadlivedata"):
        extractloadlivedata(config=config, services=services, send_alert_fn=alerts)

    payload = _find_event_payload(caplog, "execution_completed")
    assert payload is not None
    metadata = payload["metadata"]
    assert "execution_seconds" in metadata
    assert "items_total" in metadata
    assert "items_failed" in metadata
    assert "retries_seen" in metadata
    assert "correlation_ids" in metadata
    assert "correlation_ids_count" in metadata
    assert metadata["items_failed"] == 0
    assert metadata["correlation_ids_count"] == 1


def test_execution_report_success_with_multiple_pending_files(caplog):
    call_log = []
    alerts = FakeAlertSender()
    services = _build_services(
        call_log,
        pending_list=[
            "posicoes_onibus-202605131530.json",
            "posicoes_onibus-202605131531.json",
        ],
    )
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    with caplog.at_level(logging.INFO, logger="src.extractloadlivedata"):
        extractloadlivedata(config=config, services=services, send_alert_fn=alerts)

    payload = _find_event_payload(caplog, "execution_completed")
    assert payload is not None
    metadata = payload["metadata"]
    assert metadata["correlation_ids_count"] == 3
    assert metadata["correlation_ids"] == [
        "2026-05-13T15:30:45.123456+00:00",
        "2026-05-13T18:30:00Z",
        "2026-05-13T18:31:00Z",
    ]


def test_execution_report_dedup_repeated_logical_datetimes(caplog):
    call_log = []
    alerts = FakeAlertSender()
    services = _build_services(
        call_log,
        pending_list=[
            "posicoes_onibus-202605131530.json",
            "posicoes_onibus-202605131530.json",
            "posicoes_onibus-202605131531.json",
        ],
    )
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    with caplog.at_level(logging.INFO, logger="src.extractloadlivedata"):
        extractloadlivedata(config=config, services=services, send_alert_fn=alerts)

    payload = _find_event_payload(caplog, "execution_completed")
    assert payload is not None
    metadata = payload["metadata"]
    assert metadata["correlation_ids_count"] == 3
    assert metadata["correlation_ids"] == [
        "2026-05-13T15:30:45.123456+00:00",
        "2026-05-13T18:30:00Z",
        "2026-05-13T18:31:00Z",
    ]


def test_execution_report_failure_emits_enriched_final_failure(caplog):
    call_log = []
    alerts = FakeAlertSender()

    def extract_raises(_config, with_metrics=False):
        call_log.append("extract_buses_positions_with_retries")
        raise PositionsDownloadError("download failed")

    services = _build_services(call_log, pending_list=[])
    services = replace(services, extract_buses_positions_with_retries=extract_raises)
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    with caplog.at_level(logging.INFO, logger="src.extractloadlivedata"):
        extractloadlivedata(config=config, services=services, send_alert_fn=alerts)

    payload = _find_event_payload(caplog, "execution_failed_non_recoverable")
    assert payload is not None
    metadata = payload["metadata"]
    assert "execution_seconds" in metadata
    assert "items_total" in metadata
    assert "items_failed" in metadata
    assert "retries_seen" in metadata
    assert "correlation_ids" in metadata
    assert "correlation_ids_count" in metadata
    assert metadata["items_failed"] > 0


def test_alert_rule_scenario_success_without_retries_no_warning_or_failure(caplog):
    call_log = []
    alerts = FakeAlertSender()
    services = _build_services(call_log, pending_list=[])
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    with caplog.at_level(logging.INFO, logger="src.extractloadlivedata"):
        extractloadlivedata(config=config, services=services, send_alert_fn=alerts)

    completed = _find_event_payload(caplog, "execution_completed")
    failed = _find_event_payload(caplog, "execution_failed_non_recoverable")
    assert completed is not None
    assert failed is None
    assert completed["service"] == "extractloadlivedata"
    assert "execution_id" in completed and completed["execution_id"]
    assert completed["metadata"]["retries_seen"] == 0
    # Alert rule expectations:
    # - service_warning_threshold should NOT trigger (retries_seen == 0)
    # - service_failed should NOT trigger (no failure event)


def test_alert_rule_scenario_success_with_retries_triggers_warning_context(caplog):
    call_log = []
    alerts = FakeAlertSender()

    def extract_with_retries(_config, with_metrics=False):
        call_log.append("extract_buses_positions_with_retries")
        payload = {"payload": True}
        if with_metrics:
            return {"result": payload, "metrics": {"retries": 2}}
        return payload

    services = _build_services(call_log, pending_list=[])
    services = replace(services, extract_buses_positions_with_retries=extract_with_retries)
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    with caplog.at_level(logging.INFO, logger="src.extractloadlivedata"):
        extractloadlivedata(config=config, services=services, send_alert_fn=alerts)

    completed = _find_event_payload(caplog, "execution_completed")
    failed = _find_event_payload(caplog, "execution_failed_non_recoverable")
    assert completed is not None
    assert failed is None
    assert completed["service"] == "extractloadlivedata"
    assert "execution_id" in completed and completed["execution_id"]
    assert completed["metadata"]["retries_seen"] > 0
    # Alert rule expectation:
    # - service_warning_threshold should trigger (retries_seen > 0)


def test_alert_rule_scenario_non_recoverable_failure_triggers_failure_context(caplog):
    call_log = []
    alerts = FakeAlertSender()

    def extract_raises(_config, with_metrics=False):
        call_log.append("extract_buses_positions_with_retries")
        raise PositionsDownloadError("download failed")

    services = _build_services(call_log, pending_list=[])
    services = replace(services, extract_buses_positions_with_retries=extract_raises)
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
        "NOTIFICATIONS_WEBHOOK_URL": "http://fake-webhook",
    }
    with caplog.at_level(logging.INFO, logger="src.extractloadlivedata"):
        extractloadlivedata(config=config, services=services, send_alert_fn=alerts)

    completed = _find_event_payload(caplog, "execution_completed")
    failed = _find_event_payload(caplog, "execution_failed_non_recoverable")
    assert completed is None
    assert failed is not None
    assert failed["service"] == "extractloadlivedata"
    assert "execution_id" in failed and failed["execution_id"]
    assert failed["metadata"]["items_failed"] > 0
    # Alert rule expectation:
    # - service_failed should trigger (failure event exists)

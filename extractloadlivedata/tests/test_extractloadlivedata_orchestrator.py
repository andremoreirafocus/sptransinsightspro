from src.extractloadlivedata import Services, extractloadlivedata
from src.services.exceptions import (
    IngestNotificationError,
    LocalIngestBufferSaveError,
    PositionsDownloadError,
)


def _build_services(call_log, pending_list, storage_success=True):
    def extract_buses_positions_with_retries(_config):
        call_log.append("extract_buses_positions_with_retries")
        return {"payload": True}

    def get_buses_positions_with_metadata(_payload):
        call_log.append("get_buses_positions_with_metadata")
        return ["bus"], {}

    def save_bus_positions_to_local_volume(_config, _buses_positions):
        call_log.append("save_bus_positions_to_local_volume")

    def save_bus_positions_to_storage_with_retries(_config, _content):
        call_log.append("save_bus_positions_to_storage_with_retries")
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

    def trigger_pending_airflow_dag_invokations(_config):
        call_log.append("trigger_pending_airflow_dag_invokations")

    def create_pending_processing_request(_config, _filename):
        call_log.append("create_pending_processing_request")

    def trigger_pending_processing_requests(_config):
        call_log.append("trigger_pending_processing_requests")

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
    services = _build_services(call_log, pending_list=["posicoes.json"])
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "airflow",
    }
    extractloadlivedata(config=config, services=services)
    assert "create_pending_invokation" in call_log
    assert "trigger_pending_airflow_dag_invokations" in call_log
    assert "create_pending_processing_request" not in call_log
    assert "trigger_pending_processing_requests" not in call_log


def test_extractloadlivedata_processing_requests_branch_uses_processing_requests_notifications():
    call_log = []
    services = _build_services(call_log, pending_list=["posicoes.json"])
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
    }
    extractloadlivedata(config=config, services=services)
    assert "create_pending_processing_request" in call_log
    assert "trigger_pending_processing_requests" in call_log
    assert "create_pending_invokation" not in call_log
    assert "trigger_pending_airflow_dag_invokations" not in call_log


def test_extractloadlivedata_positions_download_error_is_handled():
    call_log = []

    def extract_raises(_config):
        call_log.append("extract_buses_positions_with_retries")
        raise PositionsDownloadError("download failed")

    services = _build_services(call_log, pending_list=["posicoes.json"])
    services = Services(
        extract_buses_positions_with_retries=extract_raises,
        get_buses_positions_with_metadata=services.get_buses_positions_with_metadata,
        save_bus_positions_to_local_volume=services.save_bus_positions_to_local_volume,
        save_bus_positions_to_storage_with_retries=services.save_bus_positions_to_storage_with_retries,
        load_bus_positions_from_local_volume_file=services.load_bus_positions_from_local_volume_file,
        remove_local_file=services.remove_local_file,
        get_pending_storage_save_list=services.get_pending_storage_save_list,
        create_pending_invokation=services.create_pending_invokation,
        trigger_pending_airflow_dag_invokations=services.trigger_pending_airflow_dag_invokations,
        create_pending_processing_request=services.create_pending_processing_request,
        trigger_pending_processing_requests=services.trigger_pending_processing_requests,
    )
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
    }
    extractloadlivedata(config=config, services=services)
    assert "extract_buses_positions_with_retries" in call_log
    assert "get_pending_storage_save_list" in call_log


def test_extractloadlivedata_local_save_error_keeps_pending_processing():
    call_log = []

    def save_local_raises(_config, _buses_positions):
        call_log.append("save_bus_positions_to_local_volume")
        raise LocalIngestBufferSaveError("local save failed")

    services = _build_services(call_log, pending_list=["posicoes.json"])
    services = Services(
        extract_buses_positions_with_retries=services.extract_buses_positions_with_retries,
        get_buses_positions_with_metadata=services.get_buses_positions_with_metadata,
        save_bus_positions_to_local_volume=save_local_raises,
        save_bus_positions_to_storage_with_retries=services.save_bus_positions_to_storage_with_retries,
        load_bus_positions_from_local_volume_file=services.load_bus_positions_from_local_volume_file,
        remove_local_file=services.remove_local_file,
        get_pending_storage_save_list=services.get_pending_storage_save_list,
        create_pending_invokation=services.create_pending_invokation,
        trigger_pending_airflow_dag_invokations=services.trigger_pending_airflow_dag_invokations,
        create_pending_processing_request=services.create_pending_processing_request,
        trigger_pending_processing_requests=services.trigger_pending_processing_requests,
    )
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
    }
    extractloadlivedata(config=config, services=services)
    assert "save_bus_positions_to_local_volume" in call_log
    assert "save_bus_positions_to_storage_with_retries" in call_log
    assert "trigger_pending_processing_requests" in call_log


def test_extractloadlivedata_notification_error_is_handled():
    call_log = []

    def trigger_raises(_config):
        call_log.append("trigger_pending_processing_requests")
        raise IngestNotificationError("notification failed")

    services = _build_services(call_log, pending_list=["posicoes.json"])
    services = Services(
        extract_buses_positions_with_retries=services.extract_buses_positions_with_retries,
        get_buses_positions_with_metadata=services.get_buses_positions_with_metadata,
        save_bus_positions_to_local_volume=services.save_bus_positions_to_local_volume,
        save_bus_positions_to_storage_with_retries=services.save_bus_positions_to_storage_with_retries,
        load_bus_positions_from_local_volume_file=services.load_bus_positions_from_local_volume_file,
        remove_local_file=services.remove_local_file,
        get_pending_storage_save_list=services.get_pending_storage_save_list,
        create_pending_invokation=services.create_pending_invokation,
        trigger_pending_airflow_dag_invokations=services.trigger_pending_airflow_dag_invokations,
        create_pending_processing_request=services.create_pending_processing_request,
        trigger_pending_processing_requests=trigger_raises,
    )
    config = {
        "INGEST_BUFFER_PATH": "/tmp/ingest",
        "NOTIFICATION_ENGINE": "processing_requests",
    }
    extractloadlivedata(config=config, services=services)
    assert "trigger_pending_processing_requests" in call_log

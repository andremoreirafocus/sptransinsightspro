import json
import logging
import pytest

from src.services.save_load_bus_positions import (
    data_structure_is_valid,
    get_file_name_from_data,
    get_payload_summary,
    load_bus_positions_from_local_volume_file,
    remove_local_file,
    get_pending_storage_save_list,
    save_bus_positions_to_storage,
    save_bus_positions_to_storage_with_retries,
    save_data_to_raw_object_storage,
)
from src.services.exceptions import SavePositionsToRawError
from src.infra.compression import compress_data
from tests.fakes.object_storage_client import FakeObjectStorageClient

_LOGGER_NAME = "src.services.save_load_bus_positions"


def _find_event(caplog: pytest.LogCaptureFixture, event: str) -> bool:
    for record in caplog.records:
        try:
            payload = json.loads(record.message)
            if payload.get("event") == event:
                return True
        except (json.JSONDecodeError, AttributeError):
            pass
    return False


def build_sample_data():
    return {
        "metadata": {
            "extracted_at": "2026-04-09T09:10:00-03:00",
            "source": "sptrans_api_v2",
            "total_vehicles": 3,
        },
        "payload": {
            "hr": "09:10",
            "l": [
                {"qv": 2, "vs": [{}, {}]},
                {"qv": 1, "vs": [{}]},
            ],
        },
    }


def test_get_file_name_from_data_formats():
    data = build_sample_data()
    data["payload"]["hr"] = "09:47"
    filename, partition = get_file_name_from_data(data)
    assert filename == "posicoes_onibus-202604090910.json"
    assert partition == "year=2026/month=04/day=09/"


def test_get_payload_summary_counts():
    data = build_sample_data()
    hour_minute, total_qv, total_lines = get_payload_summary(data)
    assert hour_minute == "0910"
    assert total_qv == 3
    assert total_lines == 2


def test_data_structure_is_valid():
    data = build_sample_data()
    assert data_structure_is_valid(data) is True


def test_data_structure_is_valid_rejects_missing_payload():
    data = build_sample_data()
    data.pop("payload")
    assert data_structure_is_valid(data) is False


def test_data_structure_is_valid_rejects_missing_metadata_fields():
    data = build_sample_data()
    data["metadata"].pop("source")
    assert data_structure_is_valid(data) is False


def test_save_bus_positions_to_storage_rejects_invalid_data():
    config = {"DATA_COMPRESSION_ON_SAVE": "false"}
    invalid_data = {"payload": {}}
    try:
        save_bus_positions_to_storage(config, invalid_data)
    except ValueError:
        assert True
    else:
        assert False, "Expected ValueError for invalid data"


def test_load_bus_positions_from_local_volume_file_json(tmp_path):
    data = build_sample_data()
    file_path = tmp_path / "posicoes_onibus-202604090910.json"
    file_path.write_text(json.dumps(data))
    loaded = load_bus_positions_from_local_volume_file(str(tmp_path), file_path.name)
    assert loaded == data


def test_load_bus_positions_from_local_volume_file_compressed(tmp_path):
    data = build_sample_data()
    file_path = tmp_path / "posicoes_onibus-202604090910.json.zst"
    compressed_bytes, _ = compress_data(json.dumps(data))
    file_path.write_bytes(compressed_bytes)
    loaded = load_bus_positions_from_local_volume_file(str(tmp_path), file_path.name)
    assert loaded == data


def test_save_bus_positions_to_storage_with_retries_success():
    data = build_sample_data()
    config = {"STORAGE_MAX_RETRIES": 2}

    def fake_save(*_args, **_kwargs):
        return None

    def fake_sleep(_):
        return None

    result = save_bus_positions_to_storage_with_retries(
        config, data, sleep_fn=fake_sleep, save_fn=fake_save
    )
    assert result is True


def test_save_bus_positions_to_storage_with_retries_success_with_metrics():
    data = build_sample_data()
    config = {"STORAGE_MAX_RETRIES": 2}

    def fake_save(*_args, **_kwargs):
        return None

    def fake_sleep(_):
        return None

    result = save_bus_positions_to_storage_with_retries(
        config, data, sleep_fn=fake_sleep, save_fn=fake_save, with_metrics=True
    )
    assert result["result"] is None
    assert result["metrics"]["retries"] == 0


def test_save_bus_positions_to_storage_with_retries_exhausts():
    data = build_sample_data()
    config = {"STORAGE_MAX_RETRIES": 2}
    calls = {"count": 0}

    def fake_save(*_args, **_kwargs):
        calls["count"] += 1
        raise RuntimeError("fail")

    def fake_sleep(_):
        return None

    with pytest.raises(
        SavePositionsToRawError, match="max retries reached while saving positions"
    ) as exc_info:
        save_bus_positions_to_storage_with_retries(
            config, data, sleep_fn=fake_sleep, save_fn=fake_save
        )
    assert calls["count"] == 2
    assert getattr(exc_info.value, "retries", None) == 2


def test_save_data_to_raw_object_storage_calls_object_storage():
    data = build_sample_data()
    config = {
        "SOURCE_BUCKET": "raw",
        "APP_FOLDER": "sptrans",
        "MINIO_ENDPOINT": "localhost:9000",
        "ACCESS_KEY": "key",
        "SECRET_KEY": "secret",
    }

    fake_client = FakeObjectStorageClient()
    save_data_to_raw_object_storage(
        config,
        data=json.dumps(data),
        compression=False,
        client=fake_client,
    )
    assert len(fake_client.put_calls) == 1
    call = fake_client.put_calls[0]
    assert call["bucket_name"] == "raw"
    assert call["object_name"].startswith("sptrans/")


def test_save_data_to_raw_object_storage_compresses():
    data = build_sample_data()
    config = {
        "SOURCE_BUCKET": "raw",
        "APP_FOLDER": "sptrans",
        "MINIO_ENDPOINT": "localhost:9000",
        "ACCESS_KEY": "key",
        "SECRET_KEY": "secret",
    }

    fake_client = FakeObjectStorageClient()
    save_data_to_raw_object_storage(
        config,
        data=json.dumps(data),
        compression=True,
        client=fake_client,
    )
    assert len(fake_client.put_calls) == 1
    call = fake_client.put_calls[0]
    assert call["object_name"].endswith(".zst")
    assert isinstance(call["buffer"], (bytes, bytearray))
    assert len(call["buffer"]) > 0


def test_remove_local_file_no_matches():
    data = build_sample_data()
    config = {"INGEST_BUFFER_PATH": "/tmp"}
    removed = []

    def fake_glob(_pattern):
        return []

    def fake_remove(path):
        removed.append(path)

    remove_local_file(
        config,
        data,
        glob_fn=fake_glob,
        remove_fn=fake_remove,
    )
    assert removed == []


def test_remove_local_file_removes_all_matches():
    data = build_sample_data()
    config = {"INGEST_BUFFER_PATH": "/tmp"}
    removed = []

    def fake_glob(_pattern):
        return [
            "/tmp/posicoes_onibus-202604090910.json",
            "/tmp/posicoes_onibus-202604090910.json.zst",
        ]

    def fake_remove(path):
        removed.append(path)

    remove_local_file(
        config,
        data,
        glob_fn=fake_glob,
        remove_fn=fake_remove,
    )
    assert len(removed) == 2


def test_get_pending_storage_save_list_filters():
    config = {"INGEST_BUFFER_PATH": "/tmp"}

    def fake_listdir(_path):
        return ["posicoes_onibus-1.json", "other.json", "posicoes_onibus-2.json.zst"]

    pending = get_pending_storage_save_list(config, listdir_fn=fake_listdir)
    assert pending == ["posicoes_onibus-1.json", "posicoes_onibus-2.json.zst"]


# ── Step 0: new tests for previously-silent escape paths ─────────────────────


# ── Step 2: new test for single-event validation failure ─────────────────────


def test_data_structure_is_valid_invalid_data_emits_single_event(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
        result = data_structure_is_valid({"payload": "not-a-dict", "metadata": {}})
    assert result is False
    matching = [
        r
        for r in caplog.records
        if json.loads(r.message).get("event") == "object_storage_persist_failed"
    ]
    assert len(matching) == 1, f"Expected exactly 1 event, got {len(matching)}"
    payload = json.loads(matching[0].message)
    assert "metadata" in payload


# ── Step 0: new tests for previously-silent escape paths ─────────────────────


def test_get_file_name_from_data_missing_metadata_emits_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
        with pytest.raises(ValueError):
            get_file_name_from_data({"metadata": None})
    assert _find_event(caplog, "metadata_validation_failed")


def test_get_file_name_from_data_missing_extracted_at_emits_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
        with pytest.raises(ValueError):
            get_file_name_from_data({"metadata": {"source": "x"}})
    assert _find_event(caplog, "metadata_validation_failed")


def test_get_payload_summary_invalid_payload_emits_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
        with pytest.raises(ValueError):
            get_payload_summary({"payload": "not-a-dict"})
    assert _find_event(caplog, "metadata_validation_failed")


def test_get_pending_storage_save_list_listdir_error_emits_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = {"INGEST_BUFFER_PATH": "/nonexistent"}

    def bad_listdir(_path):
        raise OSError("permission denied")

    with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
        with pytest.raises(OSError):
            get_pending_storage_save_list(config, listdir_fn=bad_listdir)
    assert _find_event(caplog, "object_storage_list_failed")


def test_save_bus_positions_to_storage_with_retries_config_error_emits_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
        with pytest.raises(SavePositionsToRawError):
            save_bus_positions_to_storage_with_retries({}, build_sample_data())
    assert _find_event(caplog, "object_storage_persist_failed")


def test_save_bus_positions_to_storage_config_error_emits_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.ERROR, logger=_LOGGER_NAME):
        with pytest.raises(Exception):
            save_bus_positions_to_storage({}, build_sample_data())
    assert _find_event(caplog, "object_storage_persist_failed")

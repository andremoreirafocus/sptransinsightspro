import json
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


def build_sample_data():
    return {
        "metadata": {
            "extracted_at": "2026-04-09T09:10:00",
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
    filename, partition = get_file_name_from_data(data)
    assert filename.startswith("posicoes_onibus-")
    assert filename.endswith(".json")
    assert "year=" in partition and "month=" in partition and "day=" in partition


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
        return ["/tmp/posicoes_onibus-202604090910.json", "/tmp/posicoes_onibus-202604090910.json.zst"]

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

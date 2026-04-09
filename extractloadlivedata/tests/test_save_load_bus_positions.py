import json

from src.services.save_load_bus_positions import (
    data_structure_is_valid,
    get_file_name_from_data,
    get_payload_summary,
    load_bus_positions_from_local_volume_file,
    save_bus_positions_to_storage_with_retries,
    save_data_to_raw_object_storage,
)
from tests.fakes import FakeObjectStorageClient


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


def test_load_bus_positions_from_local_volume_file_json(tmp_path):
    data = build_sample_data()
    file_path = tmp_path / "posicoes_onibus-202604090910.json"
    file_path.write_text(json.dumps(data))
    loaded = load_bus_positions_from_local_volume_file(str(tmp_path), file_path.name)
    assert loaded == data


def test_save_bus_positions_to_storage_with_retries_success(monkeypatch):
    data = build_sample_data()
    config = {"STORAGE_MAX_RETRIES": 2}

    def fake_save(*_args, **_kwargs):
        return None

    def fake_sleep(_):
        return None

    monkeypatch.setattr(
        "src.services.save_load_bus_positions.save_bus_positions_to_storage",
        fake_save,
    )
    result = save_bus_positions_to_storage_with_retries(
        config, data, sleep_fn=fake_sleep
    )
    assert result is True


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

import io
import json
from transformlivedata.services.load_positions import load_positions


def make_config(compressed=False):
    return {
        "general": {
            "storage": {
                "raw_bucket": "raw",
                "app_folder": "sptrans",
            },
            "compression": {
                "raw_data_compression": compressed,
                "raw_data_compression_extension": ".zst",
            },
        },
        "connections": {
            "object_storage": {
                "endpoint": "localhost",
                "access_key": "key",
                "secret_key": "secret",
            }
        },
    }


SAMPLE_DATA = {"metadata": {"total_vehicles": 1}, "payload": {"hr": "10:00", "l": []}}


def test_loads_uncompressed_json():
    def fake_read_str(connection, bucket, object_name):
        return json.dumps(SAMPLE_DATA)

    result = load_positions(
        make_config(compressed=False),
        "year=2026/month=02/day=15/",
        "posicoes_onibus-202602150936.json",
        read_str_fn=fake_read_str,
    )
    assert result["metadata"]["total_vehicles"] == 1


def test_loads_compressed_data():
    compressed_bytes = b"fake-compressed"

    def fake_read_bytes(connection, bucket, object_name):
        return io.BytesIO(compressed_bytes)

    def fake_decompress(data):
        return json.dumps(SAMPLE_DATA)

    result = load_positions(
        make_config(compressed=True),
        "year=2026/month=02/day=15/",
        "posicoes_onibus-202602150936.json",
        read_bytes_fn=fake_read_bytes,
        decompress_fn=fake_decompress,
    )
    assert result["payload"]["hr"] == "10:00"


def test_object_name_built_correctly_uncompressed():
    calls = []

    def fake_read_str(connection, bucket, object_name):
        calls.append(object_name)
        return json.dumps(SAMPLE_DATA)

    load_positions(
        make_config(compressed=False),
        "year=2026/month=02/day=15/",
        "posicoes_onibus-202602150936.json",
        read_str_fn=fake_read_str,
    )
    assert (
        calls[0]
        == "sptrans/year=2026/month=02/day=15/posicoes_onibus-202602150936.json"
    )


def test_object_name_has_compression_extension_when_compressed():
    calls = []

    def fake_read_bytes(connection, bucket, object_name):
        calls.append(object_name)
        return io.BytesIO(b"bytes")

    def fake_decompress(data):
        return json.dumps(SAMPLE_DATA)

    load_positions(
        make_config(compressed=True),
        "year=2026/month=02/day=15/",
        "posicoes_onibus-202602150936.json",
        read_bytes_fn=fake_read_bytes,
        decompress_fn=fake_decompress,
    )
    assert calls[0].endswith(".zst")

import io
from gtfs.services.transforms import transform_csv_table_to_parquet


def make_config():
    return {
        "general": {
            "storage": {
                "raw_bucket": "raw",
                "trusted_bucket": "trusted",
                "gtfs_folder": "gtfs",
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


def test_load_convert_save_called_in_order():
    call_log = []

    def fake_load(config, table_name):
        call_log.append(("load", table_name))
        return io.BytesIO(b"csv")

    def fake_convert(buffer):
        call_log.append(("convert",))
        return b"parquet"

    def fake_save(config, file_name, buffer):
        call_log.append(("save", file_name))

    transform_csv_table_to_parquet(
        make_config(),
        "stops",
        load_fn=fake_load,
        convert_fn=fake_convert,
        save_fn=fake_save,
    )
    assert call_log == [("load", "stops"), ("convert",), ("save", "stops.parquet")]


def test_csv_buffer_passed_to_convert():
    csv_data = io.BytesIO(b"stop_id,stop_name\n1,A")
    received = {}

    def fake_load(config, table_name):
        return csv_data

    def fake_convert(buffer):
        received["buffer"] = buffer
        return b"parquet"

    def fake_save(config, file_name, buffer):
        pass

    transform_csv_table_to_parquet(
        make_config(),
        "stops",
        load_fn=fake_load,
        convert_fn=fake_convert,
        save_fn=fake_save,
    )
    assert received["buffer"] is csv_data


def test_parquet_buffer_passed_to_save():
    received = {}

    def fake_load(config, table_name):
        return io.BytesIO(b"csv")

    def fake_convert(buffer):
        return b"parquet-output"

    def fake_save(config, file_name, buffer):
        received["buffer"] = buffer

    transform_csv_table_to_parquet(
        make_config(),
        "stops",
        load_fn=fake_load,
        convert_fn=fake_convert,
        save_fn=fake_save,
    )
    assert received["buffer"] == b"parquet-output"


def test_file_name_has_parquet_extension():
    received = {}

    def fake_load(config, table_name):
        return io.BytesIO(b"csv")

    def fake_convert(buffer):
        return b"parquet"

    def fake_save(config, file_name, buffer):
        received["file_name"] = file_name

    transform_csv_table_to_parquet(
        make_config(),
        "routes",
        load_fn=fake_load,
        convert_fn=fake_convert,
        save_fn=fake_save,
    )
    assert received["file_name"] == "routes.parquet"

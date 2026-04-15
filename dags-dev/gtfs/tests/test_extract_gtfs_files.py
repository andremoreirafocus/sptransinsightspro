import io
import zipfile
import pytest
from gtfs.services.extract_gtfs_files import extract_gtfs_files


def make_config():
    return {
        "general": {
            "extraction": {
                "local_downloads_folder": "/tmp/gtfs",
            },
        },
        "connections": {
            "http": {
                "conn_type": "https",
                "host": "example.com",
                "schema": "/gtfs.zip",
                "login": "user",
                "password": "pass",
            }
        },
    }


def make_zip_bytes(filenames):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name in filenames:
            z.writestr(name, b"data")
    return buf.getvalue()


class FakeResponse:
    def __init__(self, status_code, content=b""):
        self.status_code = status_code
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def test_returns_file_list_on_success(tmp_path):
    zip_bytes = make_zip_bytes(["stops.txt", "routes.txt"])

    def fake_get(url, auth):
        return FakeResponse(200, zip_bytes)

    config = make_config()
    config["general"]["extraction"]["local_downloads_folder"] = str(tmp_path)

    result = extract_gtfs_files(config, http_get_fn=fake_get)
    assert sorted(result) == ["routes.txt", "stops.txt"]


def test_returns_none_on_404():
    def fake_get(url, auth):
        return FakeResponse(404)

    result = extract_gtfs_files(make_config(), http_get_fn=fake_get)
    assert result is None


def test_raises_on_non_404_http_error():
    def fake_get(url, auth):
        return FakeResponse(500)

    with pytest.raises(RuntimeError):
        extract_gtfs_files(make_config(), http_get_fn=fake_get)


def test_missing_config_key_raises_value_error():
    config = make_config()
    del config["connections"]["http"]["host"]
    with pytest.raises(ValueError, match="Missing required configuration key"):
        extract_gtfs_files(config)


def test_files_extracted_to_folder(tmp_path):
    zip_bytes = make_zip_bytes(["agency.txt"])

    def fake_get(url, auth):
        return FakeResponse(200, zip_bytes)

    config = make_config()
    config["general"]["extraction"]["local_downloads_folder"] = str(tmp_path)

    extract_gtfs_files(config, http_get_fn=fake_get)
    assert (tmp_path / "agency.txt").exists()

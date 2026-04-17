from gtfs.services.validate_raw_gtfs_files import validate_raw_gtfs_files
import csv
import io
import pytest


def make_config(folder):
    return {
        "general": {
            "extraction": {
                "local_downloads_folder": str(folder),
            }
        }
    }


def test_returns_valid_when_all_files_pass(tmp_path):
    (tmp_path / "stops.txt").write_text("stop_id,stop_name\n1,Central\n", encoding="utf-8")
    result = validate_raw_gtfs_files(make_config(tmp_path), ["stops.txt"], min_lines=2)
    assert result["is_valid"] is True
    assert result["errors_by_file"] == {}


def test_flags_insufficient_lines(tmp_path):
    (tmp_path / "stops.txt").write_text("stop_id,stop_name\n", encoding="utf-8")
    result = validate_raw_gtfs_files(make_config(tmp_path), ["stops.txt"], min_lines=2)
    assert result["is_valid"] is False
    assert "stops.txt" in result["errors_by_file"]
    assert "insufficient_lines" in result["errors_by_file"]["stops.txt"][0]


def test_flags_missing_file(tmp_path):
    result = validate_raw_gtfs_files(make_config(tmp_path), ["missing.txt"], min_lines=2)
    assert result["is_valid"] is False
    assert result["errors_by_file"]["missing.txt"] == ["file_not_found"]


def test_flags_file_not_readable(tmp_path):
    def fake_read_file(*args, **kwargs):
        raise PermissionError("no read permission")

    result = validate_raw_gtfs_files(
        make_config(tmp_path),
        ["stops.txt"],
        read_file_fn=fake_read_file,
    )
    assert result["is_valid"] is False
    assert result["errors_by_file"]["stops.txt"] == ["file_not_readable"]


def test_flags_invalid_encoding_utf8(tmp_path):
    def fake_read_file(*args, **kwargs):
        raise UnicodeDecodeError("utf-8", b"\x80", 0, 1, "bad")

    result = validate_raw_gtfs_files(
        make_config(tmp_path),
        ["stops.txt"],
        read_file_fn=fake_read_file,
    )
    assert result["is_valid"] is False
    assert result["errors_by_file"]["stops.txt"] == ["invalid_encoding_utf8"]


def test_flags_invalid_csv_error(tmp_path):
    def fake_read_file(*args, **kwargs):
        raise csv.Error("bad csv")

    result = validate_raw_gtfs_files(
        make_config(tmp_path),
        ["stops.txt"],
        read_file_fn=fake_read_file,
    )
    assert result["is_valid"] is False
    assert result["errors_by_file"]["stops.txt"][0].startswith("invalid_csv:")


def test_flags_unexpected_validation_error(tmp_path):
    def fake_read_file(*args, **kwargs):
        raise RuntimeError("boom")

    result = validate_raw_gtfs_files(
        make_config(tmp_path),
        ["stops.txt"],
        read_file_fn=fake_read_file,
    )
    assert result["is_valid"] is False
    assert result["errors_by_file"]["stops.txt"][0].startswith(
        "unexpected_validation_error:"
    )


def test_missing_config_key_raises_value_error():
    with pytest.raises(ValueError, match="Missing required configuration key"):
        validate_raw_gtfs_files({}, ["stops.txt"])

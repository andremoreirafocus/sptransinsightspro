import csv
import json

from gtfs.services.validate_raw_gtfs_files import validate_raw_gtfs_files


def _parse_log_events(caplog, event_name: str) -> list[dict]:
    results = []
    for record in caplog.records:
        try:
            parsed = json.loads(record.getMessage())
        except Exception:
            continue
        if parsed.get("event") == event_name:
            results.append(parsed)
    return results


def make_config(folder):
    return {
        "general": {
            "extraction": {
                "local_downloads_folder": str(folder),
            }
        }
    }


def test_returns_valid_when_all_files_pass(tmp_path):
    (tmp_path / "stops.txt").write_text(
        "stop_id,stop_name\n1,Central\n", encoding="utf-8"
    )
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
    result = validate_raw_gtfs_files(
        make_config(tmp_path), ["missing.txt"], min_lines=2
    )
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


def test_insufficient_lines_emits_raw_file_validation_error(caplog, tmp_path):
    caplog.set_level("ERROR")
    (tmp_path / "stops.txt").write_text("stop_id,stop_name\n", encoding="utf-8")

    validate_raw_gtfs_files(make_config(tmp_path), ["stops.txt"], min_lines=2)

    events = _parse_log_events(caplog, "raw_file_validation_error")
    assert len(events) == 1
    assert events[0]["metadata"]["error_type"] == "insufficient_lines"
    assert events[0]["metadata"]["file_name"] == "stops.txt"
    assert events[0]["metadata"]["line_count"] == 1
    assert events[0]["metadata"]["min_lines"] == 2


def test_raw_validation_completed_includes_validated_files_count(caplog, tmp_path):
    caplog.set_level("INFO")
    (tmp_path / "stops.txt").write_text("stop_id,stop_name\n1,Central\n", encoding="utf-8")
    (tmp_path / "routes.txt").write_text("route_id,route_name\n1,Line1\n", encoding="utf-8")

    validate_raw_gtfs_files(
        make_config(tmp_path), ["stops.txt", "routes.txt"], min_lines=2
    )

    events = _parse_log_events(caplog, "raw_validation_completed")
    assert len(events) == 1
    assert events[0]["metadata"]["validated_files_count"] == 2

from gtfs.services.validate_raw_gtfs_files import validate_raw_gtfs_files


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

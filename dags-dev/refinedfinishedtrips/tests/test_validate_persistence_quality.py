from refinedfinishedtrips.services.validate_persistence_quality import (
    validate_persistence_quality,
)


def test_all_duplicates_warns():
    result = validate_persistence_quality({"new_rows": 0, "skipped_rows": 10})
    assert result["status"] == "WARN"
    assert "note" in result


def test_mix_of_new_and_duplicates_passes():
    result = validate_persistence_quality({"new_rows": 5, "skipped_rows": 5})
    assert result["status"] == "PASS"
    assert "note" not in result


def test_all_new_passes():
    result = validate_persistence_quality({"new_rows": 10, "skipped_rows": 0})
    assert result["status"] == "PASS"


def test_empty_save_result_passes():
    result = validate_persistence_quality({"new_rows": 0, "skipped_rows": 0})
    assert result["status"] == "PASS"


def test_result_contains_row_counts():
    result = validate_persistence_quality({"new_rows": 3, "skipped_rows": 7})
    assert result["new_rows"] == 3
    assert result["skipped_rows"] == 7

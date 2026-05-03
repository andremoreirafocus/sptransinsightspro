from refinedfinishedtrips.services.validate_persistence_quality import (
    validate_persistence_quality,
)


def test_all_previously_saved_rows_passes():
    result = validate_persistence_quality(
        {"added_rows": 0, "previously_saved_rows": 10}
    )
    assert result["status"] == "PASS"


def test_mix_of_added_and_previously_saved_rows_passes():
    result = validate_persistence_quality(
        {"added_rows": 5, "previously_saved_rows": 5}
    )
    assert result["status"] == "PASS"


def test_all_added_rows_passes():
    result = validate_persistence_quality(
        {"added_rows": 10, "previously_saved_rows": 0}
    )
    assert result["status"] == "PASS"


def test_empty_save_result_passes():
    result = validate_persistence_quality(
        {"added_rows": 0, "previously_saved_rows": 0}
    )
    assert result["status"] == "PASS"


def test_result_contains_row_counts():
    result = validate_persistence_quality(
        {"added_rows": 3, "previously_saved_rows": 7}
    )
    assert result["added_rows"] == 3
    assert result["previously_saved_rows"] == 7

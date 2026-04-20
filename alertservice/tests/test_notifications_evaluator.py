from src.notifications_evaluator import evaluate_cumulative_warn


def make_row(status="WARN", items_failed=10, acceptance_rate=0.99):
    return {
        "status": status,
        "items_failed": items_failed,
        "acceptance_rate": acceptance_rate,
    }


def test_empty_rows_returns_false():
    assert evaluate_cumulative_warn([], {}) is False


def test_max_failed_items_exceeded():
    rows = [make_row(items_failed=300), make_row(items_failed=300)]
    assert evaluate_cumulative_warn(rows, {"max_failed_items": 500}) is True


def test_max_failed_items_not_exceeded():
    rows = [make_row(items_failed=100), make_row(items_failed=100)]
    assert evaluate_cumulative_warn(rows, {"max_failed_items": 500}) is False


def test_max_failed_ratio_exceeded():
    rows = [make_row(acceptance_rate=0.95), make_row(acceptance_rate=0.95)]
    assert evaluate_cumulative_warn(rows, {"max_failed_ratio": 0.02}) is True


def test_max_failed_ratio_not_exceeded():
    rows = [make_row(acceptance_rate=0.99), make_row(acceptance_rate=0.99)]
    assert evaluate_cumulative_warn(rows, {"max_failed_ratio": 0.02}) is False


def test_max_consecutive_warn_exceeded():
    rows = [make_row(status="WARN"), make_row(status="WARN"), make_row(status="WARN")]
    assert evaluate_cumulative_warn(rows, {"max_consecutive_warn": 3}) is True


def test_max_consecutive_warn_not_exceeded():
    rows = [make_row(status="WARN"), make_row(status="WARN")]
    assert evaluate_cumulative_warn(rows, {"max_consecutive_warn": 3}) is False


def test_consecutive_warn_broken_by_pass():
    rows = [make_row(status="WARN"), make_row(status="PASS"), make_row(status="WARN")]
    assert evaluate_cumulative_warn(rows, {"max_consecutive_warn": 2}) is False


def test_no_thresholds_returns_false():
    rows = [make_row(items_failed=1000, acceptance_rate=0.0)]
    assert evaluate_cumulative_warn(rows, {}) is False

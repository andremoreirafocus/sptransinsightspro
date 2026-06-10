from src.quality.reporting import MAX_CORRELATION_IDS_IN_REPORT, build_execution_report_metadata


def _base_kwargs(**overrides):
    kwargs = {
        "execution_seconds": 1.5,
        "items_total": 4,
        "items_failed": 0,
        "retries_seen": 0,
        "worked_correlation_ids": ["2026-05-13T18:30:00Z"],
    }
    kwargs.update(overrides)
    return kwargs


def test_required_fields_are_present():
    result = build_execution_report_metadata(**_base_kwargs())
    for field in (
        "execution_seconds",
        "items_total",
        "items_failed",
        "retries_seen",
        "correlation_ids",
        "correlation_ids_count",
        "correlation_ids_truncated",
        "pending_object_storage_save_files_count",
        "pending_ingest_notifications_count",
    ):
        assert field in result


def test_scalar_fields_match_inputs():
    result = build_execution_report_metadata(
        **_base_kwargs(execution_seconds=3.7, items_total=5, items_failed=2, retries_seen=1)
    )
    assert result["execution_seconds"] == 3.7
    assert result["items_total"] == 5
    assert result["items_failed"] == 2
    assert result["retries_seen"] == 1


def test_pending_counts_match_inputs():
    result = build_execution_report_metadata(
        **_base_kwargs(
            pending_object_storage_save_files_count=3,
            pending_ingest_notifications_count=7,
        )
    )
    assert result["pending_object_storage_save_files_count"] == 3
    assert result["pending_ingest_notifications_count"] == 7


def test_pending_counts_default_to_zero():
    result = build_execution_report_metadata(**_base_kwargs())
    assert result["pending_object_storage_save_files_count"] == 0
    assert result["pending_ingest_notifications_count"] == 0


def test_correlation_ids_deduplication_preserves_order():
    result = build_execution_report_metadata(
        **_base_kwargs(worked_correlation_ids=["c", "a", "b", "a", "c"])
    )
    assert result["correlation_ids"] == ["c", "a", "b"]
    assert result["correlation_ids_count"] == 3
    assert result["correlation_ids_truncated"] is False


def test_correlation_ids_empty_list():
    result = build_execution_report_metadata(**_base_kwargs(worked_correlation_ids=[]))
    assert result["correlation_ids"] == []
    assert result["correlation_ids_count"] == 0
    assert result["correlation_ids_truncated"] is False


def test_correlation_ids_truncated_above_max():
    ids = [f"id-{i}" for i in range(MAX_CORRELATION_IDS_IN_REPORT + 5)]
    result = build_execution_report_metadata(**_base_kwargs(worked_correlation_ids=ids))
    assert len(result["correlation_ids"]) == MAX_CORRELATION_IDS_IN_REPORT
    assert result["correlation_ids_count"] == MAX_CORRELATION_IDS_IN_REPORT + 5
    assert result["correlation_ids_truncated"] is True


def test_correlation_ids_not_truncated_at_exact_max():
    ids = [f"id-{i}" for i in range(MAX_CORRELATION_IDS_IN_REPORT)]
    result = build_execution_report_metadata(**_base_kwargs(worked_correlation_ids=ids))
    assert len(result["correlation_ids"]) == MAX_CORRELATION_IDS_IN_REPORT
    assert result["correlation_ids_count"] == MAX_CORRELATION_IDS_IN_REPORT
    assert result["correlation_ids_truncated"] is False


def test_phase_metrics_absent_when_none():
    result = build_execution_report_metadata(**_base_kwargs())
    assert "phase_metrics" not in result


def test_phase_metrics_present_when_provided():
    metrics = {"extract": {"attempted": 1, "succeeded": 1, "failed": 0}}
    result = build_execution_report_metadata(**_base_kwargs(phase_metrics=metrics))
    assert result["phase_metrics"] == metrics


def test_phase_durations_absent_when_none():
    result = build_execution_report_metadata(**_base_kwargs())
    assert "phase_durations" not in result


def test_phase_durations_present_when_provided():
    durations = {"extract": 1.2, "local_save": 0.05}
    result = build_execution_report_metadata(**_base_kwargs(phase_durations=durations))
    assert result["phase_durations"] == durations


def test_logical_datetime_absent_when_none():
    result = build_execution_report_metadata(**_base_kwargs())
    assert "logical_datetime" not in result


def test_logical_datetime_present_when_provided():
    result = build_execution_report_metadata(
        **_base_kwargs(logical_datetime="2026-05-13T18:30:00Z")
    )
    assert result["logical_datetime"] == "2026-05-13T18:30:00Z"

from transformlivedata.services.build_logical_date_context import (
    build_logical_date_context,
)
import pytest


def test_returns_expected_keys():
    result = build_logical_date_context("2026-02-15T12:35:00+00:00")
    assert "partition_path" in result
    assert "source_file" in result


def test_partition_path_uses_sao_paulo_timezone():
    # UTC 2026-02-15T02:00:00 => Sao Paulo 2026-02-14T23:00:00 (UTC-3)
    result = build_logical_date_context("2026-02-15T02:00:00+00:00")
    assert result["partition_path"] == "year=2026/month=02/day=14/"


def test_source_file_includes_hhmm():
    # UTC 2026-02-15T12:35:00 => Sao Paulo 2026-02-15T09:35:00
    result = build_logical_date_context("2026-02-15T12:35:00+00:00")
    assert result["source_file"] == "posicoes_onibus-202602150935.json"


def test_invalid_date_raises_runtime_error():
    with pytest.raises(RuntimeError, match="Failed to build logical date context"):
        build_logical_date_context("not-a-date")

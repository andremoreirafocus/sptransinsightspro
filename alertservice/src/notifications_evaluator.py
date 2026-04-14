from typing import Any, Dict

from .infra.storage import query_window


def evaluate_cumulative_warn(pipeline: str, config: Dict[str, Any]) -> bool:
    warning_cfg = config.get(pipeline, {})
    window_cfg = warning_cfg.get("warning_window", {})
    thresholds = warning_cfg.get("warning_thresholds", {})

    if not window_cfg:
        return False

    window_type = window_cfg.get("type", "time")
    window_value = int(window_cfg.get("value", 24))

    rows = query_window(pipeline, window_type, window_value)
    if not rows:
        return False

    total_failed_rows = sum((r["rows_failed"] or 0) for r in rows)
    failed_ratios = [1 - (r["acceptance_rate"] or 0.0) for r in rows]
    avg_failed_ratio = sum(failed_ratios) / len(failed_ratios)

    max_failed_rows = thresholds.get("max_failed_rows")
    max_failed_ratio = thresholds.get("max_failed_ratio")
    max_consecutive_warn = thresholds.get("max_consecutive_warn")

    if max_failed_rows is not None and total_failed_rows >= max_failed_rows:
        return True
    if max_failed_ratio is not None and avg_failed_ratio >= max_failed_ratio:
        return True
    if max_consecutive_warn is not None:
        consecutive = 0
        for row in rows:
            if row["status"] == "WARN":
                consecutive += 1
                if consecutive >= max_consecutive_warn:
                    return True
            else:
                break
    return False

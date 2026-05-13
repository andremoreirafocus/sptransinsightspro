import logging
from typing import Any, Dict, Iterable, Mapping


logger = logging.getLogger(__name__)


def evaluate_cumulative_warn(
    rows: Iterable[Mapping[str, Any]], thresholds: Dict[str, Any]
) -> bool:
    rows_list = list(rows)
    if not rows_list:
        return False

    total_failed_items = sum((r["items_failed"] or 0) for r in rows_list)
    failed_ratios = [1 - (r["acceptance_rate"] or 0.0) for r in rows_list]
    avg_failed_ratio = sum(failed_ratios) / len(failed_ratios)

    max_failed_items = thresholds.get("max_failed_items")
    max_failed_ratio = thresholds.get("max_failed_ratio")
    max_consecutive_warn = thresholds.get("max_consecutive_warn")

    if max_failed_items is not None and total_failed_items >= max_failed_items:
        return True
    if max_failed_ratio is not None and avg_failed_ratio >= max_failed_ratio:
        return True
    if max_consecutive_warn is not None:
        consecutive = 0
        for row in rows_list:
            if row["status"] == "WARN":
                consecutive += 1
                if consecutive >= max_consecutive_warn:
                    return True
            else:
                break
    return False

import logging
from typing import Any, Dict, Iterable


logger = logging.getLogger(__name__)

def evaluate_cumulative_warn(rows: Iterable[Dict[str, Any]], thresholds: Dict[str, Any]) -> bool:
    rows = list(rows)
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

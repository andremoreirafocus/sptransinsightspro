import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def validate_persistence_quality(save_result: Dict[str, Any]) -> Dict[str, Any]:
    new_rows = save_result["new_rows"]
    skipped_rows = save_result["skipped_rows"]

    if new_rows == 0 and skipped_rows > 0:
        status = "WARN"
        note = f"all {skipped_rows} trips were already present in the database (duplicates)"
    else:
        status = "PASS"
        note = None

    logger.info(
        f"Persistence quality check: new_rows={new_rows}, skipped_rows={skipped_rows} → {status}."
    )
    result = {
        "status": status,
        "new_rows": new_rows,
        "skipped_rows": skipped_rows,
    }
    if note:
        result["note"] = note
    return result

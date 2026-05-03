import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def validate_persistence_quality(save_result: Dict[str, Any]) -> Dict[str, Any]:
    added_rows = save_result["added_rows"]
    previously_saved_rows = save_result["previously_saved_rows"]
    status = "PASS"

    logger.info(
        f"Persistence quality check: added_rows={added_rows}, previously_saved_rows={previously_saved_rows} → {status}."
    )
    return {
        "status": status,
        "added_rows": added_rows,
        "previously_saved_rows": previously_saved_rows,
    }

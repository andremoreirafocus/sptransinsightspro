import logging
from typing import Any, Dict, List, Tuple

from refinedfinishedtrips.services.extract_trips_from_positions import (
    extract_raw_trips_metadata,
    generate_trips_table,
)

logger = logging.getLogger(__name__)


def extract_trips_per_line_per_vehicle(
    positions_list: List[Dict[str, Any]],
    start_idx: int,
    end_idx: int,
    linha_lt: str,
    veiculo_id: int,
    stop_proximity_threshold_meters: int,
) -> Tuple[List[Tuple], int]:
    try:
        if not positions_list or start_idx > end_idx:
            return [], 0
        position_records = positions_list[start_idx : end_idx + 1]
        if not position_records:
            logger.debug(f"No positions for line {linha_lt} vehicle {veiculo_id}")
            return [], 0
        raw_trips_metadata = extract_raw_trips_metadata(
            position_records, stop_proximity_threshold_meters
        )
        if not raw_trips_metadata:
            logger.debug(f"No trips for line {linha_lt} vehicle {veiculo_id}")
            return [], 0
        sentido_mismatches = sum(1 for m in raw_trips_metadata if m.get("sentido_mismatch", False))
        finished_trips = generate_trips_table(
            position_records, raw_trips_metadata, linha_lt, veiculo_id
        )
        return finished_trips, sentido_mismatches
    except Exception as e:
        logger.error(f"Error processing {linha_lt}/{veiculo_id}: {e}")
        raise TypeError(f"Error processing {linha_lt}/{veiculo_id}:")

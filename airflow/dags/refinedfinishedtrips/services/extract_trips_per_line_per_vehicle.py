import logging
from typing import Any, Dict, List, Tuple

from observability.structured_event_logger import get_structured_logger
from refinedfinishedtrips.services.extract_trips_from_positions import (
    extract_raw_trips_metadata,
    generate_trips_table,
)
from refinedfinishedtrips.services.sanitize_position_records import (
    sanitize_position_records,
)

logger = logging.getLogger(__name__)
structured_logger = get_structured_logger(logger_name=__name__)


def extract_trips_per_line_per_vehicle(
    positions_list: List[Dict[str, Any]],
    start_idx: int,
    end_idx: int,
    linha_lt: str,
    veiculo_id: str,
    stop_proximity_threshold_meters: int,
    ) -> Tuple[List[Tuple], int, int]:
    try:
        if not positions_list or start_idx > end_idx:
            return [], 0, 0
        position_records = positions_list[start_idx : end_idx + 1]
        if not position_records:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"No positions for line {linha_lt} vehicle {veiculo_id}")
            return [], 0, 0
        position_records, sanitization = sanitize_position_records(position_records)
        if sanitization["dropped_points_count"] > 0:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "Dropped %s anomalous position record(s) for line %s vehicle %s",
                    sanitization["dropped_points_count"],
                    linha_lt,
                    veiculo_id,
                )
        raw_trips_metadata = extract_raw_trips_metadata(
            position_records, stop_proximity_threshold_meters
        )
        if not raw_trips_metadata:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"No trips for line {linha_lt} vehicle {veiculo_id}")
            return [], 0, sanitization["dropped_points_count"]
        source_sentido_discrepancies = sum(
            1
            for trip_metadata in raw_trips_metadata
            if trip_metadata.get("source_sentido_discrepancy", False)
        )
        finished_trips = generate_trips_table(
            position_records, raw_trips_metadata, linha_lt, veiculo_id
        )
        return (
            finished_trips,
            source_sentido_discrepancies,
            sanitization["dropped_points_count"],
        )
    except Exception as e:
        error_message = (
            "Trip extraction failed while processing line/vehicle: "
            f"linha_lt='{linha_lt}', veiculo_id='{veiculo_id}'"
        )
        structured_logger.error(
            event="vehicle_trip_extraction_failed",
            message=error_message,
            metadata={"linha_lt": linha_lt, "veiculo_id": veiculo_id},
        )
        raise ValueError(error_message) from e

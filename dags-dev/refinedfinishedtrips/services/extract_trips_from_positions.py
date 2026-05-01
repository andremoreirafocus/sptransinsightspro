import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_SEEKING_DEPARTURE = "SEEKING_DEPARTURE"
_IN_TRIP = "IN_TRIP"
_DIRECTION_FIRST_TO_LAST = "first_to_last"
_DIRECTION_LAST_TO_FIRST = "last_to_first"


def extract_raw_trips_metadata(
    position_records: List[Dict[str, Any]],
    stop_proximity_threshold_meters: int,
) -> List[Dict[str, Any]]:
    trips_metadata = []
    if len(position_records) < 2:
        return trips_metadata

    state = _SEEKING_DEPARTURE
    trip_start_record_index: Optional[int] = None
    departure_direction: Optional[str] = None
    has_moved_away_from_departure_stop = False

    for current_index, position_record in enumerate(position_records):
        distance_to_first_stop = position_record["distance_to_first_stop"]
        distance_to_last_stop = position_record["distance_to_last_stop"]
        at_first_stop = distance_to_first_stop < stop_proximity_threshold_meters
        at_last_stop = distance_to_last_stop < stop_proximity_threshold_meters
        is_circular = position_record["is_circular"]

        if state == _SEEKING_DEPARTURE:
            if at_first_stop and not at_last_stop:
                trip_start_record_index = current_index
                departure_direction = _DIRECTION_FIRST_TO_LAST
                has_moved_away_from_departure_stop = False
            elif at_last_stop and not at_first_stop:
                trip_start_record_index = current_index
                departure_direction = _DIRECTION_LAST_TO_FIRST
                has_moved_away_from_departure_stop = False
            elif trip_start_record_index is not None:
                # Bus has exited the departure zone — trip is now in progress
                has_moved_away_from_departure_stop = True
                state = _IN_TRIP

        elif state == _IN_TRIP:
            if departure_direction == _DIRECTION_FIRST_TO_LAST:
                if not at_first_stop:
                    has_moved_away_from_departure_stop = True
                if has_moved_away_from_departure_stop and at_last_stop:
                    derived_sentido = 1
                    _emit_trip(
                        trips_metadata,
                        position_records,
                        trip_start_record_index,
                        current_index,
                        derived_sentido,
                        is_circular,
                    )
                    state = _SEEKING_DEPARTURE
                    trip_start_record_index = None
                    departure_direction = None
                    has_moved_away_from_departure_stop = False

            elif departure_direction == _DIRECTION_LAST_TO_FIRST:
                if not at_last_stop:
                    has_moved_away_from_departure_stop = True
                if has_moved_away_from_departure_stop and at_first_stop:
                    derived_sentido = 2
                    _emit_trip(
                        trips_metadata,
                        position_records,
                        trip_start_record_index,
                        current_index,
                        derived_sentido,
                        is_circular,
                    )
                    state = _SEEKING_DEPARTURE
                    trip_start_record_index = None
                    departure_direction = None
                    has_moved_away_from_departure_stop = False

    return trips_metadata


def _emit_trip(
    trips_metadata: List[Dict[str, Any]],
    position_records: List[Dict[str, Any]],
    trip_start_record_index: int,
    trip_end_record_index: int,
    derived_sentido: int,
    is_circular: bool,
) -> bool:
    last_record_sentido = position_records[trip_end_record_index]["linha_sentido"]
    trip_start_time = position_records[trip_start_record_index]["veiculo_ts"]
    trip_end_time = position_records[trip_end_record_index]["veiculo_ts"]
    mismatch = derived_sentido != last_record_sentido
    if mismatch:
        logger.warning(
            f"Sentido mismatch for vehicle {position_records[0]['veiculo_id']} "
            f"on line {position_records[0]['linha_lt']}: "
            f"derived={derived_sentido}, linha_sentido={last_record_sentido}"
            f" (circular={is_circular}, "
            f"start_idx={trip_start_record_index}, end_idx={trip_end_record_index}, "
            f"trip_start_time={trip_start_time}, trip_end_time={trip_end_time})"
        )
    trips_metadata.append(
        {
            "start_position_index": trip_start_record_index,
            "end_position_index": trip_end_record_index,
            "sentido": derived_sentido,
            "sentido_mismatch": mismatch,
        }
    )
    return mismatch


def get_trip_id(linha: str, sentido: int) -> str:
    def sentido_convertido(sentido):
        if sentido == 1:
            return 0
        elif sentido == 2:
            return 1
        else:
            return 999

    this_trip_id = f"{linha}-{sentido_convertido(sentido)}"
    return this_trip_id


def generate_trips_table(position_records: List[Dict[str, Any]], trips_metadata: List[Dict[str, Any]], linha_lt: str, veiculo_id: int) -> List[Tuple]:
    trips = []
    for trip_metadata in trips_metadata:
        sentido = trip_metadata["sentido"]
        trip_id = get_trip_id(linha_lt, sentido)
        vehicle_id = veiculo_id
        trip_start_time = position_records[trip_metadata["start_position_index"]][
            "veiculo_ts"
        ]
        trip_end_time = position_records[trip_metadata["end_position_index"]][
            "veiculo_ts"
        ]
        duration = trip_end_time - trip_start_time
        is_circular = position_records[0]["is_circular"]
        average_speed = 0.0
        trip_record = (
            trip_id,
            int(vehicle_id),
            trip_start_time,
            trip_end_time,
            duration,
            is_circular,
            average_speed,
        )
        trips.append(trip_record)
    return trips

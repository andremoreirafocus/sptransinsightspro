import logging
from collections import Counter
from math import atan2, cos, radians, sin, sqrt
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_SEEKING_DEPARTURE = "SEEKING_DEPARTURE"
_IN_TRIP = "IN_TRIP"
_DIRECTION_FIRST_TO_LAST = "first_to_last"
_DIRECTION_LAST_TO_FIRST = "last_to_first"
_CIRCULAR_SEEKING_START = "SEEKING_START"
_CIRCULAR_SEEKING_TERMINAL_EXIT = "SEEKING_TERMINAL_EXIT"
_CIRCULAR_IN_TRIP = "IN_TRIP"
_MIN_MOVEMENT_DISTANCE_METERS = 30.0


def extract_raw_trips_metadata(
    position_records: List[Dict[str, Any]],
    stop_proximity_threshold_meters: int,
) -> List[Dict[str, Any]]:
    if not position_records:
        return []
    if position_records[0]["is_circular"]:
        trips_metadata = extract_circular_trips_metadata(
            position_records, stop_proximity_threshold_meters
        )
        if trips_metadata:
            logger.debug(
                "Using circular trip extraction for line %s vehicle %s; detected %s trip(s)",
                position_records[0]["linha_lt"],
                position_records[0]["veiculo_id"],
                len(trips_metadata),
            )
        return trips_metadata
    trips_metadata = extract_non_circular_trips_metadata(
        position_records, stop_proximity_threshold_meters
    )
    if trips_metadata:
        logger.debug(
            "Using non-circular trip extraction for line %s vehicle %s; detected %s trip(s)",
            position_records[0]["linha_lt"],
            position_records[0]["veiculo_id"],
            len(trips_metadata),
        )
    return trips_metadata


def extract_non_circular_trips_metadata(
    position_records: List[Dict[str, Any]],
    stop_proximity_threshold_meters: int,
) -> List[Dict[str, Any]]:
    trips_metadata: List[Dict[str, Any]] = []
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
                    if trip_start_record_index is None:
                        continue
                    _emit_trip(
                        trips_metadata,
                        position_records,
                        trip_start_record_index,
                        current_index,
                        derived_sentido,
                        is_circular,
                        stop_proximity_threshold_meters,
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
                    if trip_start_record_index is None:
                        continue
                    _emit_trip(
                        trips_metadata,
                        position_records,
                        trip_start_record_index,
                        current_index,
                        derived_sentido,
                        is_circular,
                        stop_proximity_threshold_meters,
                    )
                    state = _SEEKING_DEPARTURE
                    trip_start_record_index = None
                    departure_direction = None
                    has_moved_away_from_departure_stop = False

    return trips_metadata


def extract_circular_trips_metadata(
    position_records: List[Dict[str, Any]],
    stop_proximity_threshold_meters: int,
) -> List[Dict[str, Any]]:
    trips_metadata: List[Dict[str, Any]] = []
    if len(position_records) < 2:
        return trips_metadata

    state = _CIRCULAR_SEEKING_START
    trip_start_record_index: Optional[int] = None
    active_sentido: Optional[int] = None
    synchronized = False

    for current_index, position_record in enumerate(position_records):
        at_anchor_stop = _is_at_anchor_stop(
            position_record, stop_proximity_threshold_meters
        )
        current_sentido = int(position_record["linha_sentido"])

        if not synchronized:
            if not at_anchor_stop:
                continue
            synchronized = True

        if state == _CIRCULAR_SEEKING_START:
            if current_index > 0:
                previous_record = position_records[current_index - 1]
                previous_sentido = int(previous_record["linha_sentido"])
                previous_at_anchor_stop = _is_at_anchor_stop(
                    previous_record, stop_proximity_threshold_meters
                )
                if current_sentido != previous_sentido:
                    trip_start_record_index = current_index
                    active_sentido = current_sentido
                    if at_anchor_stop:
                        state = _CIRCULAR_SEEKING_TERMINAL_EXIT
                    else:
                        state = _CIRCULAR_IN_TRIP
                    continue
                if (
                    previous_at_anchor_stop
                    and not at_anchor_stop
                    and previous_sentido == current_sentido
                    and _has_meaningful_movement(previous_record, position_record)
                ):
                    trip_start_record_index = current_index - 1
                    active_sentido = current_sentido
                    state = _CIRCULAR_IN_TRIP
                    continue
            if at_anchor_stop:
                trip_start_record_index = current_index
                active_sentido = current_sentido
                state = _CIRCULAR_SEEKING_TERMINAL_EXIT
                continue

        if state == _CIRCULAR_SEEKING_TERMINAL_EXIT:
            if trip_start_record_index is None or active_sentido is None:
                state = _CIRCULAR_SEEKING_START
                continue
            if at_anchor_stop:
                if current_sentido != active_sentido:
                    trip_start_record_index = current_index
                    active_sentido = current_sentido
                else:
                    trip_start_record_index = current_index
                continue
            if current_sentido == active_sentido:
                state = _CIRCULAR_IN_TRIP
                continue
            trip_start_record_index = current_index
            active_sentido = current_sentido
            state = _CIRCULAR_IN_TRIP
            continue

        if state == _CIRCULAR_IN_TRIP and active_sentido is not None:
            if current_sentido != active_sentido and trip_start_record_index is not None:
                trip_end_record_index = current_index - 1
                if trip_end_record_index >= trip_start_record_index:
                    _emit_trip(
                        trips_metadata,
                        position_records,
                        trip_start_record_index,
                        trip_end_record_index,
                        active_sentido,
                        True,
                        stop_proximity_threshold_meters,
                    )
                trip_start_record_index = current_index
                active_sentido = current_sentido
                if at_anchor_stop:
                    state = _CIRCULAR_SEEKING_TERMINAL_EXIT
                else:
                    state = _CIRCULAR_IN_TRIP
                continue
            if at_anchor_stop and trip_start_record_index is not None:
                _emit_trip(
                    trips_metadata,
                    position_records,
                    trip_start_record_index,
                    current_index,
                    active_sentido,
                    True,
                    stop_proximity_threshold_meters,
                )
                trip_start_record_index = current_index
                active_sentido = current_sentido
                state = _CIRCULAR_SEEKING_TERMINAL_EXIT

    return trips_metadata


def _emit_trip(
    trips_metadata: List[Dict[str, Any]],
    position_records: List[Dict[str, Any]],
    trip_start_record_index: int,
    trip_end_record_index: int,
    derived_sentido: int,
    is_circular: bool,
    stop_proximity_threshold_meters: int,
) -> bool:
    representative_sentido = _get_representative_in_trip_sentido(
        position_records,
        trip_start_record_index,
        trip_end_record_index,
        stop_proximity_threshold_meters,
    )
    trip_start_time = position_records[trip_start_record_index]["veiculo_ts"]
    trip_end_time = position_records[trip_end_record_index]["veiculo_ts"]
    source_sentido_discrepancy = (
        representative_sentido is not None and derived_sentido != representative_sentido
    )
    if source_sentido_discrepancy:
        logger.debug(
            f"Source sentido discrepancy for vehicle {position_records[0]['veiculo_id']} "
            f"on line {position_records[0]['linha_lt']}: "
            f"trip_start_time={trip_start_time}, trip_end_time={trip_end_time}"
        )
    trips_metadata.append(
        {
            "start_position_index": trip_start_record_index,
            "end_position_index": trip_end_record_index,
            "sentido": derived_sentido,
            "source_sentido_discrepancy": source_sentido_discrepancy,
        }
    )
    return source_sentido_discrepancy


def _is_at_anchor_stop(
    position_record: Dict[str, Any], stop_proximity_threshold_meters: int
) -> bool:
    return (
        position_record["distance_to_first_stop"] < stop_proximity_threshold_meters
        or position_record["distance_to_last_stop"] < stop_proximity_threshold_meters
    )


def _has_meaningful_movement(
    current_record: Dict[str, Any], next_record: Dict[str, Any]
) -> bool:
    distance_meters = _calculate_distance_meters(
        float(current_record["veiculo_lat"]),
        float(current_record["veiculo_long"]),
        float(next_record["veiculo_lat"]),
        float(next_record["veiculo_long"]),
    )
    return distance_meters > _MIN_MOVEMENT_DISTANCE_METERS


def _calculate_distance_meters(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> float:
    earth_radius_meters = 6371000
    phi1 = radians(lat1)
    phi2 = radians(lat2)
    delta_phi = radians(lat2 - lat1)
    delta_lambda = radians(lon2 - lon1)
    a = sin(delta_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(delta_lambda / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return earth_radius_meters * c


def _get_representative_in_trip_sentido(
    position_records: List[Dict[str, Any]],
    trip_start_record_index: int,
    trip_end_record_index: int,
    stop_proximity_threshold_meters: int,
) -> Optional[int]:
    in_trip_sentidos = []
    for position_record in position_records[
        trip_start_record_index : trip_end_record_index + 1
    ]:
        at_first_stop = (
            position_record["distance_to_first_stop"] < stop_proximity_threshold_meters
        )
        at_last_stop = (
            position_record["distance_to_last_stop"] < stop_proximity_threshold_meters
        )
        if at_first_stop or at_last_stop:
            continue
        in_trip_sentidos.append(position_record["linha_sentido"])

    if not in_trip_sentidos:
        return None

    counts = Counter(in_trip_sentidos).most_common()
    if len(counts) != 1:
        return None
    return int(counts[0][0])


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


def generate_trips_table(position_records: List[Dict[str, Any]], trips_metadata: List[Dict[str, Any]], linha_lt: str, veiculo_id: str) -> List[Tuple]:
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

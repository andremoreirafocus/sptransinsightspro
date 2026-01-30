import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def extract_raw_trips_metadata(records):
    trips_metadata = []
    if records:
        if len(records) < 2:
            return trips_metadata
        current_trip_start_index = 0
        current_trip_end_index = 0
        previous_trip_end_index = -1
        for i in range(1, len(records)):
            # print(records[i]["veiculo_ts"].astimezone(ZoneInfo("America/Sao_Paulo")))
            previous_index, current_index = i - 1, i
            if (
                records[current_index]["linha_sentido"]
                != records[previous_index]["linha_sentido"]
            ):
                current_trip_start_index = previous_trip_end_index + 1
                current_trip_end_index = previous_index
                discovered_trip = {
                    "start_position_index": current_trip_start_index,
                    "end_position_index": current_trip_end_index,
                    "sentido": records[previous_index]["linha_sentido"],
                }
                previous_trip_end_index = current_trip_end_index
                trips_metadata.append(discovered_trip)
                # start = records[discovered_trip["start_position_index"]]["veiculo_ts"]
                # end = records[discovered_trip["end_position_index"]]["veiculo_ts"]
                # print(
                #     f"Start: {start.astimezone(ZoneInfo('America/Sao_Paulo'))}, End: {end.astimezone(ZoneInfo('America/Sao_Paulo'))}-> Sentido: {discovered_trip['sentido']}"
                # )
        discovered_trip = {
            "start_position_index": previous_trip_end_index + 1,
            "end_position_index": current_index,
            "sentido": records[current_index]["linha_sentido"],
        }
        trips_metadata.append(discovered_trip)
        # start = records[discovered_trip["start_position_index"]]["veiculo_ts"]
        # end = records[discovered_trip["end_position_index"]]["veiculo_ts"]
        # print(
        #     f"Start: {start.astimezone(ZoneInfo('America/Sao_Paulo'))}, End: {end.astimezone(ZoneInfo('America/Sao_Paulo'))}-> Sentido: {discovered_trip['sentido']}"
        # )
    return trips_metadata


def filter_healthy_trips(trips_metadata, positions_records):
    MIN_TRIP_DURATION_FOR_CIRCULAR = 1200
    MIN_TRIP_DURATION_FOR_NON_CIRCULAR = 1800
    MAX_TRIP_DURATION = 10800
    filtered_trips_metadata = []
    max_trip_duration = MAX_TRIP_DURATION
    for trip_metadata in trips_metadata:
        start = positions_records[trip_metadata["start_position_index"]]["veiculo_ts"]
        end = positions_records[trip_metadata["end_position_index"]]["veiculo_ts"]
        trip_duration = end - start
        trip_duration_in_seconds = trip_duration.total_seconds()
        is_circular = positions_records[0]["is_circular"]
        if is_circular:
            min_trip_duration = MIN_TRIP_DURATION_FOR_CIRCULAR
        else:
            min_trip_duration = MIN_TRIP_DURATION_FOR_NON_CIRCULAR
        if (trip_duration_in_seconds > min_trip_duration) and (
            trip_duration_in_seconds < max_trip_duration
        ):
            filtered_trips_metadata.append(trip_metadata)
    return filtered_trips_metadata


def get_trip_id(linha, sentido):
    def sentido_convertido(sentido):
        if sentido == 1:
            return 0
        elif sentido == 2:
            return 1
        else:
            return 999

    this_trip_id = f"{linha}-{sentido_convertido(sentido)}"
    return this_trip_id


def generate_trips_table(position_records, trips_metadata, linha_lt, veiculo_id):
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
        print(trip_record)
    return trips

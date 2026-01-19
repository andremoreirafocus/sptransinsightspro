# algoritmo de busca das trips a partir das posições
# extrai as posições de um veículo em uma linha específica e um veiculo específico
# checa se tem descontinuidades para dividir em segmentos
# identifica as trips em um segmento com base apenas no sentido, salvando start record e end record
# Para cada trip
#   valida se a trip está completa (
#       checa se começa no ponto inicial e se acaba no ponto final
#           se nao for valida
#               adiciona a lista de trips em quarentena para análise posterior
#               vai para a próxima trip
#           se for valida
#               extrai os trechos de parada nos terminais
#               adiciona a trip na lista de trips válidas
#

from zoneinfo import ZoneInfo
import logging

from src.infra.db import fetch_data_from_db_as_df
from datetime import timedelta

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


# def load_positions_for_line_and_vehicle(config, year, month, day, linha_lt, veiculo_id):
#     table_name = config["POSITIONS_TABLE_NAME"]
#     sql = f"""select veiculo_ts, veiculo_id,
#        distance_to_first_stop, distance_to_last_stop,
#        is_circular, linha_sentido,
# 	   lt_origem, lt_destino
#        from  {table_name}
#        where linha_lt = '{linha_lt}' and veiculo_id = {veiculo_id}
#        order by veiculo_ts asc;"""

#     logger.info(f"Loading position data for line: {linha_lt}, veiculo_id: {veiculo_id}")
#     logger.debug(f"SQL Query: {sql}")
#     df_filtered_positions = fetch_data_from_db_as_df(config, sql)
#     logger.info(f"Loaded position data for line: {linha_lt}, veiculo_id: {veiculo_id}")
#     logger.info(df_filtered_positions.head(5))
#     return df_filtered_positions


logger = logging.getLogger(__name__)


def load_positions_for_line_and_vehicle(config, year, month, day, linha_lt, veiculo_id):
    table_name = config["POSITIONS_TABLE_NAME"]
    sql = f"""
        SELECT veiculo_ts, linha_sentido, distance_to_first_stop, distance_to_last_stop, is_circular, lt_origem, lt_destino
        FROM {table_name}
        WHERE linha_lt = '{linha_lt}' AND veiculo_id = {veiculo_id}
        ORDER BY veiculo_ts ASC;
    """

    df_raw = fetch_data_from_db_as_df(config, sql)
    position_records = df_raw.to_dict("records")

    if not position_records:
        return []

    raw_trips_metadata = extract_raw_trips_metadata(position_records)
    # return raw_trips_metadata
    clean_trips_metadata = raw_trips_metadata
    clean_trips = generate_trips_table(
        position_records, clean_trips_metadata, linha_lt, veiculo_id
    )
    save_trips(clean_trips)


def extract_raw_trips_metadata(records):
    trips_metadata = []
    if records:
        current_trip_start_index = 0
        # previous_trip_start_index = 0
        current_trip_end_index = 0
        previous_trip_end_index = -1
        for i in range(1, len(records)):
            # print(records[i]["veiculo_ts"].astimezone(ZoneInfo("America/Sao_Paulo")))
            previous_index, current_index = i - 1, i

            # Split condition for the operational raw_trip
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
                start = records[discovered_trip["start_position_index"]]["veiculo_ts"]
                end = records[discovered_trip["end_position_index"]]["veiculo_ts"]
                print(
                    f"Start: {start.astimezone(ZoneInfo('America/Sao_Paulo'))}, End: {end.astimezone(ZoneInfo('America/Sao_Paulo'))}-> Sentido: {discovered_trip['sentido']}"
                )
        discovered_trip = {
            "start_position_index": previous_trip_end_index + 1,
            "end_position_index": current_index,
            "sentido": records[current_index]["linha_sentido"],
        }
        trips_metadata.append(discovered_trip)
        start = records[discovered_trip["start_position_index"]]["veiculo_ts"]
        end = records[discovered_trip["end_position_index"]]["veiculo_ts"]
        print(
            f"Start: {start.astimezone(ZoneInfo('America/Sao_Paulo'))}, End: {end.astimezone(ZoneInfo('America/Sao_Paulo'))}-> Sentido: {discovered_trip['sentido']}"
        )

    # print_legs_summary(legs, title="RAW OPERATIONAL LEGS (Sense/Gap based)")
    return trips_metadata


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
            vehicle_id,
            trip_start_time,
            trip_end_time,
            duration,
            is_circular,
            average_speed,
        )
        trips.append(trip_record)
        print(trip_record)
    return trips


def save_trips(clean_trips):
    pass


#     # --- LAYER 2: Sub-segment Legs into Moving/Stationary Blocks ---
#     final_segments = []
#     for leg_records in legs:
#         if not leg_records:
#             continue

#         # Initialize the first sub-segment of the leg
#         first_ping = leg_records[0]
#         current_sub = {
#             "state": "STATIONARY",  # Default
#             "sentido": first_ping["linha_sentido"],
#             "start_ts": first_ping["veiculo_ts"],
#             "end_ts": first_ping["veiculo_ts"],
#             "start_dist": first_ping["distance_to_first_stop"],
#             "end_dist": first_ping["distance_to_first_stop"],
#             "pings": 1,
#         }

#         for i in range(1, len(leg_records)):
#             prev_p, curr_p = leg_records[i - 1], leg_records[i]

#             # Movement logic: check displacement since the last 2-minute ping
#             displacement = abs(
#                 curr_p["distance_to_first_stop"] - prev_p["distance_to_first_stop"]
#             )
#             new_state = "MOVING" if displacement > 50 else "STATIONARY"

#             if new_state != current_sub["state"]:
#                 # State flipped: Close current sub-segment if it has duration
#                 if current_sub["end_ts"] > current_sub["start_ts"]:
#                     final_segments.append(finalize_segment(current_sub))

#                 # Start new sub-segment
#                 current_sub = {
#                     "state": new_state,
#                     "sentido": curr_p["linha_sentido"],
#                     "start_ts": curr_p["veiculo_ts"],
#                     "end_ts": curr_p["veiculo_ts"],
#                     "start_dist": curr_p["distance_to_first_stop"],
#                     "end_dist": curr_p["distance_to_first_stop"],
#                     "pings": 1,
#                 }
#             else:
#                 current_sub["end_ts"] = curr_p["veiculo_ts"]
#                 current_sub["end_dist"] = curr_p["distance_to_first_stop"]
#                 current_sub["pings"] += 1

#         # Add the last sub-segment of the leg
#         if current_sub["end_ts"] > current_sub["start_ts"]:
#             final_segments.append(finalize_segment(current_sub))

#     return final_segments


# def finalize_segment(seg):
#     duration = seg["end_ts"] - seg["start_ts"]
#     displacement = abs(seg["end_dist"] - seg["start_dist"])
#     hrs = duration.total_seconds() / 3600
#     speed = (displacement / 1000) / hrs if hrs > 0 else 0

#     return {
#         "state": seg["state"],
#         "sentido": seg["sentido"],
#         "start": seg["start_ts"],
#         "end": seg["end_ts"],
#         "duration": str(duration),
#         "dist_km": round(displacement / 1000, 2),
#         "speed_kmh": round(speed, 1),
#         "pings": seg["pings"],
#     }


# def print_legs_summary(legs, title="OPERATIONAL LEGS SUMMARY"):
#     """
#     Prints a high-level summary of the raw operational legs (Sense/Gap based).
#     'legs' is expected to be a list of lists of dictionaries.
#     """
#     if not legs:
#         print("\nNo legs to display.")
#         return

#     # Header alignment: #, Sense, Start Time, End Time, Duration, Pings, Route
#     header = (
#         f"{'#':<3} {'SENSE':<6} {'START':<20} {'END':<20} {'DURATION':<12} {'PINGS':<6}"
#     )
#     divider = "-" * len(header)

#     print(f"\n{'=' * 30} {title} {'=' * 30}")
#     print(header)
#     print(divider)

#     for i, leg in enumerate(legs):
#         first = leg[0]
#         last = leg[-1]

#         start_ts = first["veiculo_ts"]
#         end_ts = last["veiculo_ts"]
#         duration = end_ts - start_ts

#         # Skip zero-duration legs if necessary, but usually legs have duration
#         if duration.total_seconds() <= 0:
#             continue

#         start_s = start_ts.strftime("%Y-%m-%d %H:%M:%S")
#         end_s = end_ts.strftime("%Y-%m-%d %H:%M:%S")

#         print(
#             f"{i:<3} {first['linha_sentido']:<6} {start_s:<20} {end_s:<20} {str(duration):<12} {len(leg):<6}"
#         )

#     print(divider)
#     print(f"Total Legs Identified: {len(legs)}")

import pandas as pd

INTERMEDIATE_STOP_NAME = "Parada intermediaria"


def transform_trip_details_for_refined(trip_details_dataframe: pd.DataFrame) -> pd.DataFrame:
    if trip_details_dataframe.empty:
        return trip_details_dataframe.copy()

    transformed_trip_details_dataframe = trip_details_dataframe.copy()

    circular_direction_0_mask = (
        transformed_trip_details_dataframe["is_circular"].fillna(False)
        & transformed_trip_details_dataframe["trip_id"].astype(str).str.endswith("-0")
    )
    circular_direction_1_mask = (
        transformed_trip_details_dataframe["is_circular"].fillna(False)
        & transformed_trip_details_dataframe["trip_id"].astype(str).str.endswith("-1")
    )

    transformed_trip_details_dataframe.loc[
        circular_direction_0_mask, "last_stop_name"
    ] = INTERMEDIATE_STOP_NAME
    transformed_trip_details_dataframe.loc[
        circular_direction_0_mask, "last_stop_id"
    ] = None
    transformed_trip_details_dataframe.loc[
        circular_direction_0_mask, "last_stop_lat"
    ] = None
    transformed_trip_details_dataframe.loc[
        circular_direction_0_mask, "last_stop_lon"
    ] = None

    transformed_trip_details_dataframe.loc[
        circular_direction_1_mask, "first_stop_name"
    ] = INTERMEDIATE_STOP_NAME
    transformed_trip_details_dataframe.loc[
        circular_direction_1_mask, "first_stop_id"
    ] = None
    transformed_trip_details_dataframe.loc[
        circular_direction_1_mask, "first_stop_lat"
    ] = None
    transformed_trip_details_dataframe.loc[
        circular_direction_1_mask, "first_stop_lon"
    ] = None

    return transformed_trip_details_dataframe

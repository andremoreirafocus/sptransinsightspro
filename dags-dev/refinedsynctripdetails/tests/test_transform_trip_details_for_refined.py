import pandas as pd

from refinedsynctripdetails.services.transform_trip_details_for_refined import (
    INTERMEDIATE_STOP_NAME,
    transform_trip_details_for_refined,
)


def test_non_circular_trip_details_remain_unchanged():
    trip_details_dataframe = pd.DataFrame(
        [
            {
                "trip_id": "1012-10-0",
                "first_stop_id": 1,
                "first_stop_name": "Terminal A",
                "first_stop_lat": -23.5,
                "first_stop_lon": -46.6,
                "last_stop_id": 2,
                "last_stop_name": "Terminal B",
                "last_stop_lat": -23.6,
                "last_stop_lon": -46.7,
                "trip_linear_distance": 1000.0,
                "is_circular": False,
            }
        ]
    )

    transformed_trip_details_dataframe = transform_trip_details_for_refined(
        trip_details_dataframe
    )

    transformed_row = transformed_trip_details_dataframe.iloc[0]
    assert transformed_row["trip_id"] == "1012-10-0"
    assert transformed_row["first_stop_id"] == 1
    assert transformed_row["first_stop_name"] == "Terminal A"
    assert transformed_row["last_stop_id"] == 2
    assert transformed_row["last_stop_name"] == "Terminal B"
    assert transformed_row["is_circular"] == False


def test_circular_direction_0_replaces_last_stop_with_intermediate_placeholder():
    trip_details_dataframe = pd.DataFrame(
        [
            {
                "trip_id": "9018-10-0",
                "first_stop_id": 1,
                "first_stop_name": "Terminal Circular",
                "first_stop_lat": -23.5,
                "first_stop_lon": -46.6,
                "last_stop_id": 2,
                "last_stop_name": "Terminal Circular",
                "last_stop_lat": -23.5,
                "last_stop_lon": -46.6,
                "trip_linear_distance": 1000.0,
                "is_circular": True,
            }
        ]
    )

    transformed_trip_details_dataframe = transform_trip_details_for_refined(
        trip_details_dataframe
    )

    transformed_row = transformed_trip_details_dataframe.iloc[0]
    assert transformed_row["first_stop_name"] == "Terminal Circular"
    assert transformed_row["last_stop_name"] == INTERMEDIATE_STOP_NAME
    assert pd.isna(transformed_row["last_stop_id"])
    assert pd.isna(transformed_row["last_stop_lat"])
    assert pd.isna(transformed_row["last_stop_lon"])


def test_circular_direction_1_replaces_first_stop_with_intermediate_placeholder():
    trip_details_dataframe = pd.DataFrame(
        [
            {
                "trip_id": "9018-10-1",
                "first_stop_id": 1,
                "first_stop_name": "Terminal Circular",
                "first_stop_lat": -23.5,
                "first_stop_lon": -46.6,
                "last_stop_id": 2,
                "last_stop_name": "Terminal Circular",
                "last_stop_lat": -23.5,
                "last_stop_lon": -46.6,
                "trip_linear_distance": 1000.0,
                "is_circular": True,
            }
        ]
    )

    transformed_trip_details_dataframe = transform_trip_details_for_refined(
        trip_details_dataframe
    )

    transformed_row = transformed_trip_details_dataframe.iloc[0]
    assert transformed_row["last_stop_name"] == "Terminal Circular"
    assert transformed_row["first_stop_name"] == INTERMEDIATE_STOP_NAME
    assert pd.isna(transformed_row["first_stop_id"])
    assert pd.isna(transformed_row["first_stop_lat"])
    assert pd.isna(transformed_row["first_stop_lon"])

import pandas as pd
from observability.structured_event_logger import get_structured_logger

INTERMEDIATE_STOP_NAME = "Parada intermediaria"
structured_logger = get_structured_logger(logger_name=__name__)


def transform_trip_details_for_refined(trip_details_dataframe: pd.DataFrame) -> pd.DataFrame:
    try:
        if trip_details_dataframe.empty:
            structured_logger.warning(
                event="trip_details_transform_skipped",
                message="Empty dataframe received — transformation skipped",
                metadata={"record_count": 0},
            )
            return trip_details_dataframe.copy()

        structured_logger.info(
            event="trip_details_transform_started",
            message="Starting trip details transformation",
            metadata={"record_count": len(trip_details_dataframe)},
        )

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

        structured_logger.info(
            event="trip_details_transform_succeeded",
            message="Trip details transformation completed",
            metadata={"record_count": len(transformed_trip_details_dataframe)},
        )
        return transformed_trip_details_dataframe
    except Exception as e:
        structured_logger.error(
            event="trip_details_transform_failed",
            message=f"Failed to transform trip details for refined layer: {e}",
            error_type=type(e).__name__,
            error_message=str(e),
        )
        raise ValueError(f"Failed to transform trip details for refined layer: {e}") from e

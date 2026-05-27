from dataclasses import dataclass
from typing import Callable

from pipeline_configurator.config import get_config
from refinedsynctripdetails.services.load_trip_details_from_storage_to_dataframe import (
    load_trip_details_from_storage_to_dataframe,
)
from refinedsynctripdetails.services.transform_trip_details_for_refined import (
    transform_trip_details_for_refined,
)
from refinedsynctripdetails.services.save_trip_details_from_dataframe_to_refined import (
    save_trip_details_from_dataframe_to_refined,
)


@dataclass(frozen=True)
class RefinedSyncTripDetailsOrchestrationDependencies:
    get_config: Callable[..., dict]
    load_trip_details: Callable
    transform_trip_details: Callable
    save_trip_details: Callable


def get_refinedsynctripdetails_orchestration_dependencies() -> (
    RefinedSyncTripDetailsOrchestrationDependencies
):
    return RefinedSyncTripDetailsOrchestrationDependencies(
        get_config=get_config,
        load_trip_details=load_trip_details_from_storage_to_dataframe,
        transform_trip_details=transform_trip_details_for_refined,
        save_trip_details=save_trip_details_from_dataframe_to_refined,
    )

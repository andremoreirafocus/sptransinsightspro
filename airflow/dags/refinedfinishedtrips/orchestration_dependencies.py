from dataclasses import dataclass
from typing import Callable

from pipeline_configurator.config import get_config
from refinedfinishedtrips.services.create_quality_report import (
    create_failure_quality_report,
    create_final_quality_report,
)
from refinedfinishedtrips.services.get_all_finished_trips import get_all_finished_trips
from refinedfinishedtrips.services.get_recent_positions import get_recent_positions
from refinedfinishedtrips.services.save_finished_trips_to_db import save_finished_trips_to_db
from refinedfinishedtrips.services.validate_positions_quality import validate_positions_quality
from refinedfinishedtrips.services.validate_trips_quality import validate_trips_quality


@dataclass(frozen=True)
class RefinedFinishedTripsOrchestrationDependencies:
    get_config: Callable[..., dict]
    get_recent_positions: Callable
    get_all_finished_trips: Callable
    validate_positions_quality: Callable
    validate_trips_quality: Callable
    save_finished_trips_to_db: Callable
    create_failure_quality_report: Callable
    create_final_quality_report: Callable


def get_refinedfinishedtrips_orchestration_dependencies() -> (
    RefinedFinishedTripsOrchestrationDependencies
):
    return RefinedFinishedTripsOrchestrationDependencies(
        get_config=get_config,
        get_recent_positions=get_recent_positions,
        get_all_finished_trips=get_all_finished_trips,
        validate_positions_quality=validate_positions_quality,
        validate_trips_quality=validate_trips_quality,
        save_finished_trips_to_db=save_finished_trips_to_db,
        create_failure_quality_report=create_failure_quality_report,
        create_final_quality_report=create_final_quality_report,
    )

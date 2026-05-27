from dataclasses import dataclass
from typing import Callable

from pipeline_configurator.config import get_config
from updatelatestpositions.services.create_latest_positions import (
    create_latest_positions_table,
)


@dataclass(frozen=True)
class UpdateLatestPositionsOrchestrationDependencies:
    get_config: Callable[..., dict]
    create_latest_positions: Callable


def get_updatelatestpositions_orchestration_dependencies() -> (
    UpdateLatestPositionsOrchestrationDependencies
):
    return UpdateLatestPositionsOrchestrationDependencies(
        get_config=get_config,
        create_latest_positions=create_latest_positions_table,
    )

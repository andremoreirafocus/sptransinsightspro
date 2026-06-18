from dataclasses import dataclass
from typing import Callable

from pipeline_configurator.config import get_config
from refinedtripfacts.lineage.trip_facts_lineage import get_trip_facts_table_columns
from refinedtripfacts.services.create_quality_report import (
    create_failure_quality_report,
    create_final_quality_report,
)
from refinedtripfacts.services.create_trip_facts import create_trip_facts
from refinedtripfacts.services.measure_input_trips import measure_input_trips
from refinedtripfacts.services.measure_persisted_facts import measure_persisted_facts
from refinedtripfacts.services.provision_dim_time import provision_dim_time
from refinedtripfacts.services.validate_trip_facts_quality import validate_trip_facts_quality


@dataclass(frozen=True)
class RefinedTripFactsOrchestrationDependencies:
    get_config: Callable[..., dict]
    measure_input_trips: Callable
    provision_dim_time: Callable
    create_trip_facts: Callable
    measure_persisted_facts: Callable
    get_trip_facts_table_columns: Callable
    validate_trip_facts_quality: Callable
    create_failure_quality_report: Callable
    create_final_quality_report: Callable


def get_refinedtripfacts_orchestration_dependencies() -> RefinedTripFactsOrchestrationDependencies:
    return RefinedTripFactsOrchestrationDependencies(
        get_config=get_config,
        measure_input_trips=measure_input_trips,
        provision_dim_time=provision_dim_time,
        create_trip_facts=create_trip_facts,
        measure_persisted_facts=measure_persisted_facts,
        get_trip_facts_table_columns=get_trip_facts_table_columns,
        validate_trip_facts_quality=validate_trip_facts_quality,
        create_failure_quality_report=create_failure_quality_report,
        create_final_quality_report=create_final_quality_report,
    )

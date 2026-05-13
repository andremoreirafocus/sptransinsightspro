from dataclasses import dataclass
from typing import Any, Callable, List, Tuple

from src.services.extract_buses_positions import (
    extract_buses_positions_with_retries,
    get_buses_positions_with_metadata,
)
from src.services.save_load_bus_positions import (
    save_bus_positions_to_local_volume,
    save_bus_positions_to_storage_with_retries,
    load_bus_positions_from_local_volume_file,
    remove_local_file,
    get_pending_storage_save_list,
)
from src.services.trigger_airflow import (
    create_pending_invokation,
    trigger_pending_airflow_dag_invokations,
)
from src.services.save_processing_requests import (
    create_pending_processing_request,
    trigger_pending_processing_requests,
)

ConfigDict = dict[str, Any]
PayloadDict = dict[str, Any]


@dataclass(frozen=True)
class Services:
    extract_buses_positions_with_retries: Callable[..., Any]
    get_buses_positions_with_metadata: Callable[[PayloadDict], Tuple[PayloadDict, str]]
    save_bus_positions_to_local_volume: Callable[[ConfigDict, PayloadDict], None]
    save_bus_positions_to_storage_with_retries: Callable[..., Any]
    load_bus_positions_from_local_volume_file: Callable[[str, str], PayloadDict]
    remove_local_file: Callable[[ConfigDict, PayloadDict], None]
    get_pending_storage_save_list: Callable[[ConfigDict], List[str]]
    create_pending_invokation: Callable[[ConfigDict, str], None]
    trigger_pending_airflow_dag_invokations: Callable[..., Any]
    create_pending_processing_request: Callable[[ConfigDict, str], None]
    trigger_pending_processing_requests: Callable[..., Any]


def build_orchestrator_dependencies() -> Services:
    return Services(
        extract_buses_positions_with_retries=extract_buses_positions_with_retries,
        get_buses_positions_with_metadata=get_buses_positions_with_metadata,
        save_bus_positions_to_local_volume=save_bus_positions_to_local_volume,
        save_bus_positions_to_storage_with_retries=save_bus_positions_to_storage_with_retries,
        load_bus_positions_from_local_volume_file=load_bus_positions_from_local_volume_file,
        remove_local_file=remove_local_file,
        get_pending_storage_save_list=get_pending_storage_save_list,
        create_pending_invokation=create_pending_invokation,
        trigger_pending_airflow_dag_invokations=trigger_pending_airflow_dag_invokations,
        create_pending_processing_request=create_pending_processing_request,
        trigger_pending_processing_requests=trigger_pending_processing_requests,
    )

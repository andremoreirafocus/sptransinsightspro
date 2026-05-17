from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd

from infra.notifications import send_webhook
from pipeline_configurator.config import get_config
from quality.validate_expectations import validate_expectations
from quality.validate_json_data_schema import validate_json_data_schema
from transformlivedata.services.build_logical_date_context import (
    build_logical_date_context,
)
from transformlivedata.services.create_data_quality_report import (
    create_data_quality_report,
    create_failure_quality_report,
)
from transformlivedata.services.load_positions import load_positions
from transformlivedata.services.processed_requests_helper import (
    mark_request_as_processed,
)
from transformlivedata.services.save_positions_to_storage import (
    save_positions_to_storage,
)
from transformlivedata.services.transform_positions import transform_positions


@dataclass(frozen=True)
class TransformLiveDataOrchestrationDependencies:
    build_logical_date_context: Callable[[str], dict]
    get_config: Callable[..., dict]
    load_positions: Callable[..., dict]
    validate_json_data_schema: Callable[[dict, dict], tuple[bool, Any]]
    transform_positions: Callable[..., dict]
    validate_expectations: Callable[[pd.DataFrame, dict], dict]
    save_positions_to_storage: Callable[..., None]
    mark_request_as_processed: Callable[[dict, str], Any]
    create_data_quality_report: Callable[..., dict]
    create_failure_quality_report: Callable[..., dict]
    send_webhook: Callable[..., Any]


def get_transformlivedata_orchestration_dependencies() -> (
    TransformLiveDataOrchestrationDependencies
):
    return TransformLiveDataOrchestrationDependencies(
        build_logical_date_context=build_logical_date_context,
        get_config=get_config,
        load_positions=load_positions,
        validate_json_data_schema=validate_json_data_schema,
        transform_positions=transform_positions,
        validate_expectations=validate_expectations,
        save_positions_to_storage=save_positions_to_storage,
        mark_request_as_processed=mark_request_as_processed,
        create_data_quality_report=create_data_quality_report,
        create_failure_quality_report=create_failure_quality_report,
        send_webhook=send_webhook,
    )

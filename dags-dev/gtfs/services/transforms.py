from gtfs.services.load_raw_csv_to_buffer_from_storage import (
    load_raw_csv_to_buffer_from_storage,
)
from infra.buffer_manipulation_functions import (
    convert_df_to_parquet_buffer,
)
from gtfs.services.save_buffer_to_storage import save_buffer_to_storage
from quality.validate_expectations import validate_expectations
import pandas as pd
import logging
from typing import Any, Callable, Dict

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def transform_and_validate_table(
    config: Dict[str, Any],
    table_name: str,
    load_fn: Callable[..., Any] = load_raw_csv_to_buffer_from_storage,
    convert_fn: Callable[..., Any] = convert_df_to_parquet_buffer,
    save_fn: Callable[..., Any] = save_buffer_to_storage,
    validate_expectations_fn: Callable[..., Any] = validate_expectations,
) -> Dict[str, Any]:
    logger.info("TRANSFORMATION STAGE - Processing table '%s'...", table_name)
    file_name = f"{table_name}.parquet"
    staging_object_name = (
        f"{config['general']['storage']['gtfs_folder']}/"
        f"{config['general']['storage']['staging_subfolder'].strip('/')}/"
        f"{file_name}"
    )
    result: Dict[str, Any] = {
        "table_name": table_name,
        "staging_object_name": staging_object_name,
        "is_valid": True,
        "errors": [],
        "expectations_summary": None,
        "staged_written": False,
    }

    try:
        csv_bytes = load_fn(config, table_name)
    except Exception as e:
        result["is_valid"] = False
        result["errors"].append(f"load_failed:{e}")
        return result

    try:
        df = pd.read_csv(csv_bytes)
    except Exception as e:
        result["is_valid"] = False
        result["errors"].append(f"csv_parse_failed:{e}")
        return result

    suite_key = f"data_expectations_{table_name}"
    suite = config.get(suite_key)
    if isinstance(suite, dict) and len(suite.get("expectations", [])) > 0:
        logger.info(
            "TRANSFORMATION STAGE - Running expectations validation for table '%s' using suite key '%s'",
            table_name,
            suite_key,
        )
        try:
            expectations_result = validate_expectations_fn(df, suite)
            summary = expectations_result.get("expectations_summary", {})
            result["expectations_summary"] = summary
            if (
                summary.get("rows_failed", 0) > 0
                or summary.get("expectations_with_violations", 0) > 0
                or summary.get("expectations_failed_due_to_exceptions", 0) > 0
            ):
                result["is_valid"] = False
                result["errors"].append(f"gx_validation_failed:{summary}")
                logger.error(
                    "TRANSFORMATION STAGE - Validation failed for table '%s': %s",
                    table_name,
                    summary,
                )
            else:
                logger.info(
                    "TRANSFORMATION STAGE - Validation passed for table '%s'",
                    table_name,
                )
        except Exception as e:
            result["is_valid"] = False
            result["errors"].append(f"gx_validation_exception:{e}")
            logger.error(
                "TRANSFORMATION STAGE - Validation exception for table '%s': %s",
                table_name,
                e,
            )
    else:
        logger.info("Validation not required and skipped for table %s", table_name)

    try:
        parquet_buffer = convert_fn(df)
        save_fn(
            config,
            file_name,
            parquet_buffer,
            subfolder=config["general"]["storage"]["staging_subfolder"],
        )
        result["staged_written"] = True
    except Exception as e:
        result["is_valid"] = False
        result["errors"].append(f"staging_save_failed:{e}")

    return result


def transform_routes(config: Dict[str, Any]) -> Dict[str, Any]:
    table_name = "routes"
    return transform_and_validate_table(config, table_name)


def transform_trips(config: Dict[str, Any]) -> Dict[str, Any]:
    table_name = "trips"
    return transform_and_validate_table(config, table_name)


def transform_stop_times(config: Dict[str, Any]) -> Dict[str, Any]:
    table_name = "stop_times"
    return transform_and_validate_table(config, table_name)


def transform_stops(config: Dict[str, Any]) -> Dict[str, Any]:
    table_name = "stops"
    return transform_and_validate_table(config, table_name)


def transform_calendar(config: Dict[str, Any]) -> Dict[str, Any]:
    table_name = "calendar"
    return transform_and_validate_table(config, table_name)


def transform_frequencies(config: Dict[str, Any]) -> Dict[str, Any]:
    table_name = "frequencies"
    return transform_and_validate_table(config, table_name)

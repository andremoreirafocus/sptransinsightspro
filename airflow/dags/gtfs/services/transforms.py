from gtfs.services.load_raw_csv_to_buffer_from_storage import (
    load_raw_csv_to_buffer_from_storage,
)
from gtfs.services.save_buffer_to_storage import save_buffer_to_storage
from quality.validate_expectations import validate_expectations
import pandas as pd
from io import BytesIO
from typing import Any, Callable, Dict

from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


def convert_df_to_parquet_buffer(df: pd.DataFrame) -> BytesIO:
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, compression="snappy")
    buffer.seek(0)
    return buffer


def transform_and_validate_table(
    config: Dict[str, Any],
    table_name: str,
    load_fn: Callable[..., Any] = load_raw_csv_to_buffer_from_storage,
    save_fn: Callable[..., Any] = save_buffer_to_storage,
    validate_expectations_fn: Callable[..., Any] = validate_expectations,
) -> Dict[str, Any]:
    structured_logger.info(
        event="table_transform_started",
        message=f"Processing table '{table_name}'",
        metadata={"table_name": table_name},
    )
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
        structured_logger.error(
            event="table_csv_load_failed",
            message=f"Failed to load raw csv for table '{table_name}': {e}",
            error_type=type(e).__name__,
            error_message=str(e),
            metadata={"table_name": table_name},
        )
        result["is_valid"] = False
        result["errors"].append(f"load_failed:{e}")
        return result
    try:
        df = pd.read_csv(csv_bytes)
    except Exception as e:
        structured_logger.error(
            event="table_csv_parse_failed",
            message=f"Failed to parse csv for table '{table_name}': {e}",
            error_type=type(e).__name__,
            error_message=str(e),
            metadata={"table_name": table_name},
        )
        result["is_valid"] = False
        result["errors"].append(f"csv_parse_failed:{e}")
        return result

    suite_key = f"data_expectations_{table_name}"
    suite = config.get(suite_key)
    if isinstance(suite, dict) and len(suite.get("expectations", [])) > 0:
        structured_logger.info(
            event="table_validation_started",
            message=f"Running expectations validation for table '{table_name}'",
            metadata={"table_name": table_name, "suite_key": suite_key},
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
                structured_logger.error(
                    event="table_validation_failed",
                    message=f"Validation failed for table '{table_name}'",
                    metadata={"table_name": table_name, "summary": summary},
                )
        except Exception as e:
            result["is_valid"] = False
            result["errors"].append(f"gx_validation_exception:{e}")
            structured_logger.error(
                event="table_validation_failed",
                message=f"Validation exception for table '{table_name}': {e}",
                error_type=type(e).__name__,
                error_message=str(e),
                metadata={"table_name": table_name},
            )
    else:
        structured_logger.info(
            event="table_validation_skipped",
            message=f"Validation not required and skipped for table '{table_name}'",
            metadata={"table_name": table_name},
        )

    try:
        parquet_buffer = convert_df_to_parquet_buffer(df)
        save_fn(
            config,
            file_name,
            parquet_buffer,
            subfolder=config["general"]["storage"]["staging_subfolder"],
        )
        result["staged_written"] = True
    except Exception as e:
        structured_logger.error(
            event="table_staging_failed",
            message=f"Failed to stage parquet for table '{table_name}': {e}",
            error_type=type(e).__name__,
            error_message=str(e),
            metadata={"table_name": table_name},
        )
        result["is_valid"] = False
        result["errors"].append(f"staging_save_failed:{e}")

    structured_logger.info(
        event="table_transform_succeeded",
        message=f"Table '{table_name}' transform completed",
        metadata={"table_name": table_name, "row_count": len(df), "staged_written": result["staged_written"]},
    )
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

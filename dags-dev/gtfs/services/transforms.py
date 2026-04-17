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

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def transform_and_validate_table(
    config,
    table_name,
    load_fn=load_raw_csv_to_buffer_from_storage,
    convert_fn=convert_df_to_parquet_buffer,
    save_fn=save_buffer_to_storage,
    validate_expectations_fn=validate_expectations,
):
    logger.info("TRANSFORMATION STAGE - Processing table '%s'...", table_name)
    file_name = f"{table_name}.parquet"
    staging_object_name = (
        f"{config['general']['storage']['gtfs_folder']}/"
        f"{config['general']['storage']['staging_subfolder'].strip('/')}/"
        f"{file_name}"
    )
    result = {
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
        except Exception as e:
            result["is_valid"] = False
            result["errors"].append(f"gx_validation_exception:{e}")
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


def transform_routes(config):
    table_name = "routes"
    return transform_and_validate_table(config, table_name)


def transform_trips(config):
    table_name = "trips"
    return transform_and_validate_table(config, table_name)


def transform_stop_times(config):
    table_name = "stop_times"
    return transform_and_validate_table(config, table_name)


def transform_stops(config):
    table_name = "stops"
    return transform_and_validate_table(config, table_name)


def transform_calendar(config):
    table_name = "calendar"
    return transform_and_validate_table(config, table_name)


def transform_frequencies(config):
    table_name = "frequencies"
    return transform_and_validate_table(config, table_name)

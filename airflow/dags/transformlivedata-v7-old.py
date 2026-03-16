from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from transformlivedata.services.load_positions import load_positions
from transformlivedata.services.transform_positions import (
    transform_positions,
)
from transformlivedata.services.save_positions_to_storage import (
    save_positions_to_storage,
)
from transformlivedata.services.processed_requests_helper import (
    mark_request_as_processed,
)
from transformlivedata.quality.validate_expectations import (
    validate_expectations,
)
from transformlivedata.config.config import get_config
from transformlivedata.quality.validate_json_data_schema import (
    validate_json_data_schema,
)
from transformlivedata.services.create_data_quality_report import (
    build_data_quality_report,
    format_data_quality_report_report,
    save_data_quality_report_to_storage,
)
import pandas as pd
from zoneinfo import ZoneInfo
import uuid

import logging

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(0),
    "max_active_runs": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


# def load_transform_save_positions(logical_date_string, **kwargs):
def load_transform_save_positions(**context):
    # logical_date = context["logical_date"]
    logical_date = context["dag_run"].logical_date
    logical_date_string = logical_date.isoformat()
    print("logical_date:", logical_date)
    print("logical_date type:", type(logical_date))
    dt = logical_date.astimezone(ZoneInfo("America/Sao_Paulo"))
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    hour = dt.strftime("%H")
    minute = dt.strftime("%M")
    execution_id = str(uuid.uuid4())
    logger.info(f"Starting execution {execution_id}")
    config = get_config()
    general_config = config["general"]
    logging.info(f"Transforming position for {dt}...")
    execution_id = str(uuid.uuid4())
    logger.info("=== LOAD STAGE: load_positions ===")
    raw_positions = load_positions(general_config, year, month, day, hour, minute)
    if not raw_positions:
        logger.error("No position data found to transform.")
        raise ValueError("No position data found to transform.")
    logger.info("=== RAW DATA VALIDATION STAGE ===")
    is_valid, validation_errors = validate_json_data_schema(
        raw_positions, config["raw_data_json_schema"]
    )
    if not is_valid:
        error_msg = f"Raw data validation failed: {validation_errors}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    logger.info("Raw data validation passed ✓")
    logger.info("=== TRANSFORM STAGE: transform_positions ===")
    transform_result = transform_positions(config, raw_positions)
    if (
        not transform_result
        or transform_result.get("positions") is None
        or transform_result["positions"].empty
    ):
        logger.error("No valid position records found after transformation.")
        raise ValueError("No valid position records found after transformation.")
    positions_df = transform_result["positions"]
    logger.info("=== EXPECTATIONS VALIDATION STAGE: validate_expectations ===")
    logger.info("Validating positions expectations...")
    valid_postions_df, invalid_positions_df, expectations_summary = (
        validate_expectations(
            positions_df,
            config["data_expectations"],
        )
    )
    data_quality_report = build_data_quality_report(
        config=general_config,
        execution_id=execution_id,
        logical_date_utc=logical_date_string,
        source_file=f"posicoes_onibus-{year}{month}{day}{hour}{minute}.json",
        transform_result=transform_result,
        valid_df=valid_postions_df,
        invalid_df=invalid_positions_df,
        expectations_summary=expectations_summary,
        pass_threshold=1.0,
        warn_threshold=0.980,
        batch_ts=transform_result["batch_ts"],
    )
    validation_report = format_data_quality_report_report(data_quality_report)
    logger.info(validation_report)
    save_data_quality_report_to_storage(
        general_config, data_quality_report, transform_result["batch_ts"]
    )
    logger.info("=== SAVE STAGE: save_positions_to_storage ===")
    logger.info("Saving valid positions to storage...")
    save_positions_to_storage(general_config, valid_postions_df, "trusted")
    logger.info(f"Saved {valid_postions_df.shape[0]} records to trusted layer")
    transform_invalid_df = transform_result.get("invalid_positions")
    invalid_frames = [
        df for df in [transform_invalid_df, invalid_positions_df] if df is not None
    ]
    combined_invalid_df = (
        pd.concat(invalid_frames, ignore_index=True)
        if len(invalid_frames) > 0
        else None
    )
    if combined_invalid_df is not None and not combined_invalid_df.empty:
        logger.info("Saving invalid positions to quarantine...")
        save_positions_to_storage(general_config, combined_invalid_df, "quarantined")
        logger.info(
            f"Saved {combined_invalid_df.shape[0]} records to quarantined layer"
        )
    mark_request_as_processed(general_config, logical_date_string)
    logger.info(f"Execution {execution_id} completed successfully")
    return {
        "execution_id": execution_id,
        "records_processed": valid_postions_df.shape[0],
        # "validation_passed": validation_results["overall_success"],
        # "lineage_report": report_filename,
        # "validation_report": validation_filename,
    }


# Criando o DAG
with DAG(
    "transformlivedata-v7-old",
    default_args=default_args,
    description="Load data from raw layer, process it, and store it in trusted layer",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["sptrans"],
) as dag:
    load_transform_save_positions_task = PythonOperator(
        task_id="transform_positions",
        python_callable=load_transform_save_positions,
        # op_kwargs={"logical_date_string": "{{ ts }}"},
    )

    load_transform_save_positions_task

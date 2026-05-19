from zoneinfo import ZoneInfo

import pandas as pd
from typing import Any, Callable, Dict, Optional, Tuple
from infra.duck_db_v3 import get_duckdb_connection
from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(
    service="transformlivedata",
    component="save_positions_to_storage",
    logger_name=__name__,
)


def save_positions_to_storage(
    config: Dict[str, Any],
    positions_df: Optional[pd.DataFrame],
    target_bucket: str,
    get_duckdb_connection_fn: Callable[[Dict[str, Any]], Any] = get_duckdb_connection,
) -> None:
    """
    Storage Layer:
    Saves a list of position tuples to MinIO in Parquet format.
    - Partitioning: year/month/day/hour (Hive style)
    - Filename pattern: positions_HHMM_*
    - Supports target_bucket: "trusted" or "quarantined"
    """

    def get_config(
        config: Dict[str, Any], target_bucket: str
    ) -> Tuple[str, str, str, Dict[str, Any]]:
        general = config["general"]
        connections = config["connections"]
        storage = general["storage"]
        tables = general["tables"]
        if target_bucket == "trusted":
            bucket_name = storage["trusted_bucket"]
        elif target_bucket == "quarantined":
            bucket_name = storage["quarantined_bucket"]
        else:
            structured_logger.error(
                event="save_positions_invalid_target_bucket",
                message="Invalid target_bucket. Use 'trusted' or 'quarantined'.",
                status="FAILED",
                metadata={"target_bucket": target_bucket},
            )
            raise ValueError(
                f"Invalid target_bucket '{target_bucket}'. Use 'trusted' or 'quarantined'."
            )
        app_folder = storage["app_folder"]
        positions_table_name = tables["positions_table_name"]
        connection_data = {
            **connections["object_storage"],
            "secure": False,
        }
        return bucket_name, app_folder, positions_table_name, connection_data

    bucket_name, app_folder, positions_table_name, connection_data = get_config(
        config, target_bucket
    )
    if positions_df is None or positions_df.empty:
        structured_logger.warning(
            event="save_positions_skipped_empty",
            message="No positions to save. DataFrame is empty.",
            status="SKIPPED",
            metadata={"target_bucket": target_bucket},
        )
        return
    try:
        structured_logger.info(
            event="save_positions_started",
            message="Starting positions save",
            status="STARTED",
            metadata={"target_bucket": target_bucket, "rows": int(len(positions_df))},
        )
        positions_df["extracao_ts"] = pd.to_datetime(positions_df["extracao_ts"], utc=True).dt.tz_convert(ZoneInfo("America/Sao_Paulo"))
        batch_ts = positions_df["extracao_ts"].iloc[0] 
        file_name = batch_ts.strftime("positions_%H%M.parquet")
        positions_df["year"] = positions_df["extracao_ts"].dt.strftime("%Y")
        positions_df["month"] = positions_df["extracao_ts"].dt.strftime("%m")
        positions_df["day"] = positions_df["extracao_ts"].dt.strftime("%d")
        positions_df["hour"] = positions_df["extracao_ts"].dt.strftime("%H")
        con = get_duckdb_connection_fn(connection_data)
        output_base_path = f"s3://{bucket_name}/{app_folder}/{positions_table_name}"
        structured_logger.info(
            event="save_positions_export_started",
            message="Exporting positions to storage",
            status="STARTED",
            metadata={
                "target_bucket": target_bucket,
                "rows": int(len(positions_df)),
                "output_base_path": output_base_path,
            },
        )
        con.execute(f"""
            COPY (SELECT * FROM positions_df) 
            TO '{output_base_path}' 
            (
                FORMAT PARQUET, 
                PARTITION_BY (year, month, day, hour), 
                FILENAME_PATTERN 'positions_{batch_ts.strftime("%H%M")}_',
                OVERWRITE_OR_IGNORE 1
            );
        """)
        structured_logger.info(
            event="save_positions_succeeded",
            message="Positions saved successfully",
            status="SUCCEEDED",
            metadata={"target_bucket": target_bucket, "file_name": file_name},
        )
    except Exception as e:
        structured_logger.error(
            event="save_positions_failed",
            message=f"Failed to save positions to {target_bucket} layer.",
            status="FAILED",
            error_type=type(e).__name__,
            error_message=str(e),
            metadata={"target_bucket": target_bucket},
        )
        raise ValueError(f"Failed to save positions to {target_bucket} layer: {e}") from e
    finally:
        if "con" in locals():
            con.close()

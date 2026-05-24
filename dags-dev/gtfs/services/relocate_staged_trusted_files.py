import io
from typing import Any, Callable, Dict, List, Optional

from minio import Minio

from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


def _move_object_with_minio(
    connection: Dict[str, Any],
    bucket_name: str,
    source_object_name: str,
    destination_object_name: str,
    client_factory: Callable[..., Any] = Minio,
) -> None:
    client = client_factory(
        connection["endpoint"],
        access_key=connection["access_key"],
        secret_key=connection["secret_key"],
        secure=connection["secure"],
    )
    response = client.get_object(bucket_name, source_object_name)
    try:
        content = response.read()
    finally:
        response.close()
        response.release_conn()
    client.put_object(
        bucket_name=bucket_name,
        object_name=destination_object_name,
        data=io.BytesIO(content),
        length=len(content),
        content_type="application/octet-stream",
    )
    client.remove_object(bucket_name, source_object_name)


def relocate_staged_trusted_files(
    config: Dict[str, Any],
    staged_results: Optional[List[Dict[str, Any]]],
    target: str,
    move_fn: Callable[..., Any] = _move_object_with_minio,
) -> Dict[str, Any]:
    def get_config(cfg):
        storage = cfg["general"]["storage"]
        connection_data = {
            **cfg["connections"]["object_storage"],
            "secure": False,
        }
        return (
            storage["trusted_bucket"],
            storage["gtfs_folder"],
            storage["quarantined_subfolder"].strip("/"),
            connection_data,
        )

    if target not in {"quarantine", "final"}:
        raise ValueError(f"Invalid relocation target: {target}")

    bucket_name, gtfs_folder, quarantined_subfolder, connection_data = get_config(config)
    moved = []
    errors = []

    structured_logger.info(
        event="file_relocation_started",
        message=f"Relocating staged files to '{target}'",
        metadata={"target": target, "item_count": len(staged_results or [])},
    )
    for item in staged_results or []:
        table_name = item["table_name"]
        source_object_name = item["staging_object_name"]
        if target == "quarantine":
            destination_object_name = (
                f"{gtfs_folder}/{quarantined_subfolder}/{table_name}.parquet"
            )
        else:
            destination_object_name = f"{gtfs_folder}/{table_name}/{table_name}.parquet"
        try:
            move_fn(
                connection_data,
                bucket_name=bucket_name,
                source_object_name=source_object_name,
                destination_object_name=destination_object_name,
            )
            moved.append(
                {
                    "table_name": table_name,
                    "source_object_name": source_object_name,
                    "destination_object_name": destination_object_name,
                }
            )
        except Exception as e:
            structured_logger.error(
                event="file_relocation_item_failed",
                message=f"Failed to relocate '{table_name}'",
                error_type=type(e).__name__,
                error_message=str(e),
                metadata={
                    "table_name": table_name,
                    "source": source_object_name,
                    "destination": destination_object_name,
                    "target": target,
                },
            )
            errors.append(
                {
                    "table_name": table_name,
                    "source_object_name": source_object_name,
                    "destination_object_name": destination_object_name,
                    "error": str(e),
                }
            )

    structured_logger.info(
        event="file_relocation_completed",
        message="File relocation completed",
        metadata={"target": target, "moved_count": len(moved), "error_count": len(errors)},
    )
    return {
        "status": "FAILED" if errors else "SUCCESS",
        "moved": moved,
        "errors": errors,
    }

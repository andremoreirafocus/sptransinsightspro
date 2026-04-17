import io
import logging

from minio import Minio

logger = logging.getLogger(__name__)


def _move_object_with_minio(
    connection,
    bucket_name,
    source_object_name,
    destination_object_name,
    client_factory=Minio,
):
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
    config,
    staged_results,
    target,
    move_fn=_move_object_with_minio,
):
    def get_config(cfg):
        try:
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
        except KeyError as e:
            logger.error("Missing required configuration key: %s", e)
            raise ValueError(f"Missing required configuration key: {e}")

    if target not in {"quarantine", "final"}:
        raise ValueError(f"Invalid relocation target: {target}")

    bucket_name, gtfs_folder, quarantined_subfolder, connection_data = get_config(config)
    moved = []
    errors = []

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
            logger.error(
                "Failed to relocate staged artifact for table '%s': %s",
                table_name,
                e,
            )
            errors.append(
                {
                    "table_name": table_name,
                    "source_object_name": source_object_name,
                    "destination_object_name": destination_object_name,
                    "error": str(e),
                }
            )

    return {
        "status": "FAILED" if errors else "SUCCESS",
        "moved": moved,
        "errors": errors,
    }

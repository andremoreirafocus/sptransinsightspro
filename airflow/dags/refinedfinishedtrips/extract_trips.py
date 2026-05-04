from datetime import datetime, timezone
from typing import Any, Callable, Dict
import uuid

from pipeline_configurator.config import get_config
from refinedfinishedtrips.config.refinedfinishedtrips_config_schema import (
    GeneralConfig,
)
from refinedfinishedtrips.lineage import (
    get_finished_trips_lineage,
    get_finished_trips_output_columns,
    validate_finished_trips_lineage,
)
from refinedfinishedtrips.services.create_quality_report import (
    create_failure_quality_report,
    create_final_quality_report,
    create_quality_report,
)
from refinedfinishedtrips.services.get_all_finished_trips import (
    get_all_finished_trips,
)
from refinedfinishedtrips.services.get_recent_positions import get_recent_positions
from refinedfinishedtrips.services.save_finished_trips_to_db import (
    save_finished_trips_to_db,
)
from refinedfinishedtrips.services.validate_positions_quality import (
    validate_positions_quality,
)
from refinedfinishedtrips.services.validate_persistence_quality import validate_persistence_quality
from refinedfinishedtrips.services.validate_trips_quality import validate_trips_quality
from infra.notifications import send_webhook
import logging

logger = logging.getLogger(__name__)


def _load_pipeline_config(pipeline_name: str) -> Dict[str, Any]:
    try:
        pipeline_config = get_config(
            pipeline_name,
            None,
            GeneralConfig,
            None,
            "minio_conn",
            "postgres_conn",
        )
    except Exception as e:
        logger.error(f"Pipeline configuration validation failed: {e}")
        raise ValueError(f"Pipeline configuration validation failed: {e}")
    return pipeline_config


def _parse_trip_extraction_output(trip_extraction_output: Any) -> tuple[Any, Dict[str, Any]]:
    if (
        isinstance(trip_extraction_output, tuple)
        and len(trip_extraction_output) == 2
        and isinstance(trip_extraction_output[1], dict)
    ):
        return trip_extraction_output
    return trip_extraction_output, {}


def _send_webhook_from_report(
    report: Dict[str, Any],
    config: Dict[str, Any],
    send_webhook_fn: Callable[..., Any],
    path: str,
) -> None:
    def get_config(config):
        return config.get("general", {}).get("notifications", {}).get("webhook_url")

    summary = report.get("summary", {})
    execution_id = summary.get("execution_id")
    status = summary.get("status")
    failure_phase = summary.get("failure_phase")
    webhook_url = get_config(config)
    normalized_webhook_url = str(webhook_url or "").strip()
    if normalized_webhook_url.lower() in {"", "disabled", "none", "null"}:
        logger.info(
            "Webhook notification disabled (%s): execution_id=%s status=%s failure_phase=%s",
            path,
            execution_id,
            status,
            failure_phase,
        )
        return
    try:
        send_webhook_fn(summary, normalized_webhook_url)
        logger.info(
            "Webhook notification sent (%s): execution_id=%s status=%s failure_phase=%s",
            path,
            execution_id,
            status,
            failure_phase,
        )
    except Exception as exc:
        logger.error(
            "Webhook notification failed (%s): execution_id=%s status=%s failure_phase=%s error=%s",
            path,
            execution_id,
            status,
            failure_phase,
            exc,
        )


def _build_column_lineage() -> Dict[str, Any]:
    column_lineage = get_finished_trips_lineage()
    actual_output_columns = get_finished_trips_output_columns()
    return validate_finished_trips_lineage(column_lineage, actual_output_columns)


def _handle_positions_result(
    positions_result: Dict[str, Any],
    config: Dict[str, Any],
    execution_id: str,
    run_ts: datetime,
    create_report_fn: Callable[..., Any],
    create_failure_report_fn: Callable[..., Any],
    send_webhook_fn: Callable[..., Any],
) -> None:
    def checks_message(status):
        return "; ".join(
            c.get("note", f"{c['check']} check {status.lower()}")
            for c in positions_result["checks"]
            if c["status"] == status
        )
    if positions_result["status"] == "FAIL":
        failure_message = checks_message("FAIL")
        logger.error(f"Positions quality FAIL: {failure_message}")
        report = create_failure_report_fn(
            config,
            execution_id,
            run_ts,
            "positions",
            failure_message,
            positions_result,
        )
        _send_webhook_from_report(
            report, config, send_webhook_fn, "positions fail"
        )
        raise ValueError(f"Positions quality check FAILED: {failure_message}")
    if positions_result["status"] == "WARN":
        logger.warning(f"Positions quality WARN: {checks_message('WARN')}")
        report = create_report_fn(config, execution_id, run_ts, positions_result)
        _send_webhook_from_report(
            report, config, send_webhook_fn, "positions warn"
        )


def _handle_trips_result(trips_result: Dict[str, Any]) -> None:
    if trips_result["status"] == "WARN":
        warn_notes = "; ".join(
            c.get("note", f"{c['check']} check warn")
            for c in trips_result["checks"]
            if c["status"] == "WARN"
        )
        logger.warning(f"Trip extraction quality WARN: {warn_notes}")


def _handle_persistence_result(persistence_result: Dict[str, Any]) -> None:
    if persistence_result["status"] == "WARN":
        logger.warning(f"Persistence quality WARN: {persistence_result.get('note', 'all trips were duplicates')}")


def extract_trips_for_all_Lines_and_vehicles(
    pipeline_name_or_config: str | Dict[str, Any],
    get_recent_positions_fn: Callable[..., Any] = get_recent_positions,
    save_trips_fn: Callable[..., Any] = save_finished_trips_to_db,
    extract_trips_fn: Callable[..., Any] = get_all_finished_trips,
    validate_positions_fn: Callable[..., Any] = validate_positions_quality,
    validate_trips_fn: Callable[..., Any] = validate_trips_quality,
    validate_persistence_fn: Callable[..., Any] = validate_persistence_quality,
    create_report_fn: Callable[..., Any] = create_quality_report,
    create_failure_report_fn: Callable[..., Any] = create_failure_quality_report,
    create_final_report_fn: Callable[..., Any] = create_final_quality_report,
    send_webhook_fn: Callable[..., Any] = send_webhook,
) -> None:
    if isinstance(pipeline_name_or_config, dict):
        config = pipeline_name_or_config
    else:
        config = _load_pipeline_config(pipeline_name_or_config)
    execution_id = str(uuid.uuid4())
    run_ts = datetime.now(timezone.utc)
    positions_result = None
    trips_result = None
    persistence_result = None
    extraction_metrics = {}
    column_lineage = None
    logger.info(f"Starting pipeline run. execution_id={execution_id}")
    df_recent_positions = get_recent_positions_fn(config)
    logger.info(f"Validating quality of {len(df_recent_positions)} position records.")
    positions_result = validate_positions_fn(config, df_recent_positions)
    _handle_positions_result(
        positions_result,
        config,
        execution_id,
        run_ts,
        create_report_fn,
        create_failure_report_fn,
        send_webhook_fn,
    )
    try:
        trip_extraction_output = extract_trips_fn(config, df_recent_positions)
        all_finished_trips, extraction_metrics = _parse_trip_extraction_output(
            trip_extraction_output
        )
        column_lineage = _build_column_lineage()
        trips_result = validate_trips_fn(
            config, df_recent_positions, all_finished_trips, extraction_metrics
        )
        _handle_trips_result(trips_result)
    except Exception as exc:
        failure_message = str(exc)
        logger.error(f"Trip extraction failed: {failure_message}")
        report = create_failure_report_fn(
            config,
            execution_id,
            run_ts,
            "trip_extraction",
            failure_message,
            positions_result,
            trips_result=trips_result,
            column_lineage=column_lineage,
        )
        _send_webhook_from_report(
            report, config, send_webhook_fn, "trip extraction fail"
        )
        raise
    try:
        save_result = save_trips_fn(config, all_finished_trips)
        persistence_result = validate_persistence_fn(save_result)
        _handle_persistence_result(persistence_result)
    except Exception as exc:
        failure_message = str(exc)
        logger.error(f"Persistence failed: {failure_message}")
        report = create_failure_report_fn(
            config,
            execution_id,
            run_ts,
            "persistence",
            failure_message,
            positions_result,
            trips_result=trips_result,
            persistence_result=persistence_result,
            column_lineage=column_lineage,
        )
        _send_webhook_from_report(
            report, config, send_webhook_fn, "persistence fail"
        )
        raise
    report = create_final_report_fn(
        config,
        execution_id,
        run_ts,
        positions_result,
        trips_result,
        persistence_result,
        column_lineage=column_lineage,
    )
    _send_webhook_from_report(
        report, config, send_webhook_fn, "final report"
    )
    logger.info(f"Pipeline run complete. execution_id={execution_id}, status={report['summary']['status']}")

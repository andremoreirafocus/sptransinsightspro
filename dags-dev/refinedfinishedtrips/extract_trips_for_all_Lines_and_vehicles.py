from datetime import datetime, timezone
import uuid

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


def _handle_positions_result(
    positions_result,
    config,
    execution_id,
    run_ts,
    webhook_url,
    create_report_fn,
    create_failure_report_fn,
    send_webhook_fn,
):
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
            config, execution_id, run_ts, "positions", failure_message, positions_result
        )
        send_webhook_fn(report["summary"], webhook_url)
        raise ValueError(f"Positions quality check FAILED: {failure_message}")
    if positions_result["status"] == "WARN":
        logger.warning(f"Positions quality WARN: {checks_message('WARN')}")
        report = create_report_fn(config, execution_id, run_ts, positions_result)
        send_webhook_fn(report["summary"], webhook_url)
        logger.info(f"Positions quality warning report sent to webhook: {webhook_url}")


def _handle_trips_result(trips_result):
    if trips_result["status"] == "WARN":
        warn_notes = "; ".join(
            c.get("note", f"{c['check']} check warn")
            for c in trips_result["checks"]
            if c["status"] == "WARN"
        )
        logger.warning(f"Trip extraction quality WARN: {warn_notes}")


def _handle_persistence_result(persistence_result):
    if persistence_result["status"] == "WARN":
        logger.warning(f"Persistence quality WARN: {persistence_result.get('note', 'all trips were duplicates')}")


def extract_trips_for_all_Lines_and_vehicles(
    config,
    get_recent_positions_fn=get_recent_positions,
    save_trips_fn=save_finished_trips_to_db,
    extract_trips_fn=get_all_finished_trips,
    validate_positions_fn=validate_positions_quality,
    validate_trips_fn=validate_trips_quality,
    validate_persistence_fn=validate_persistence_quality,
    create_report_fn=create_quality_report,
    create_failure_report_fn=create_failure_quality_report,
    create_final_report_fn=create_final_quality_report,
    send_webhook_fn=send_webhook,
):
    def get_config(config):
        return config["general"]["notifications"]["webhook_url"]

    webhook_url = get_config(config)
    execution_id = str(uuid.uuid4())
    run_ts = datetime.now(timezone.utc)
    logger.info(f"Starting pipeline run. execution_id={execution_id}")
    df_recent_positions = get_recent_positions_fn(config)
    logger.info(f"Validating quality of {len(df_recent_positions)} position records.")
    positions_result = validate_positions_fn(df_recent_positions, config)
    _handle_positions_result(
        positions_result,
        config,
        execution_id,
        run_ts,
        webhook_url,
        create_report_fn,
        create_failure_report_fn,
        send_webhook_fn,
    )
    all_finished_trips = extract_trips_fn(df_recent_positions)
    trips_result = validate_trips_fn(df_recent_positions, all_finished_trips, config)
    _handle_trips_result(trips_result)
    save_result = save_trips_fn(config, all_finished_trips)
    persistence_result = validate_persistence_fn(save_result)
    _handle_persistence_result(persistence_result)
    report = create_final_report_fn(
        config, execution_id, run_ts, positions_result, trips_result, persistence_result
    )
    send_webhook_fn(report["summary"], webhook_url)
    logger.info(f"Pipeline run complete. execution_id={execution_id}, status={report['summary']['status']}")

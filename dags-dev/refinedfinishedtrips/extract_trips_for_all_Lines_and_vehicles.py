from datetime import datetime, timezone
import uuid

from refinedfinishedtrips.services.create_quality_report import (
    create_failure_quality_report,
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


def extract_trips_for_all_Lines_and_vehicles(
    config,
    get_recent_positions_fn=get_recent_positions,
    save_trips_fn=save_finished_trips_to_db,
    extract_trips_fn=get_all_finished_trips,
    validate_positions_fn=validate_positions_quality,
    create_report_fn=create_quality_report,
    create_failure_report_fn=create_failure_quality_report,
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
    save_trips_fn(config, all_finished_trips)

from src.extractloadlivedata import extractloadlivedata
from src.config.config import get_config, validate_config
from src.domain.events import ALLOWED_EVENTS, ALLOWED_EVENT_STATUSES, LogStatus
from src.infra.structured_logging import (
    get_process_structured_logger,
)
from apscheduler.schedulers.blocking import BlockingScheduler
from typing import Tuple, Union
import sys

structured_logger = get_process_structured_logger(
    service="extractloadlivedata",
    component="scheduler",
    logger_name=__name__,
    allowed_events=ALLOWED_EVENTS,
    allowed_statuses=ALLOWED_EVENT_STATUSES,
    stream=sys.stdout,
)


def run_extractloadlivedata_task() -> None:
    structured_logger.info(
        event="scheduler_tick_started",
        status=LogStatus.STARTED,
        message="Scheduler tick execution started.",
    )
    extractloadlivedata()
    structured_logger.info(
        event="scheduler_tick_completed",
        status=LogStatus.SUCCEEDED,
        message="Scheduler tick execution completed.",
    )


def get_scheduling_config() -> Tuple[str, Union[int, str], int, int, int]:
    config = get_config()
    interval = int(config["EXTRACTION_INTERVAL_SECONDS"])
    if interval <= 0:
        structured_logger.error(
            event="config_validation_failed",
            status=LogStatus.FAILED,
            message="Invalid scheduling configuration: EXTRACTION_INTERVAL_SECONDS must be positive.",
            metadata={"extraction_interval_seconds": interval},
        )
        raise ValueError("EXTRACTION_INTERVAL_SECONDS must be a positive integer.")

    if interval > 60:
        # 'cron' with minute='*/2' ensures it runs at :00, :02, :04...
        # 'second=0' ensures it starts exactly at the start of the minute
        minutes = interval // 60
        seconds = 0
        minutes_schedule = f"*/{minutes}"
        seconds_schedule = 0
        grace_time = 10
        structured_logger.info(
            event="scheduler_config_loaded",
            status=LogStatus.SUCCEEDED,
            message="Scheduler configuration loaded for minute-based cron trigger.",
            metadata={
                "interval_seconds": interval,
                "minutes": minutes,
                "grace_time_seconds": grace_time,
            },
        )
    else:
        minutes = 0
        seconds = interval
        minutes_schedule = "*"
        seconds_schedule = f"*/{interval}"
        grace_time = 5
        structured_logger.warning(
            event="scheduler_config_loaded",
            status=LogStatus.SUCCEEDED,
            message="Scheduler configuration loaded for second-based cron trigger.",
            metadata={
                "interval_seconds": interval,
                "seconds": seconds,
                "grace_time_seconds": grace_time,
            },
        )
    return minutes_schedule, seconds_schedule, grace_time, minutes, seconds


def main() -> None:
    structured_logger.info(
        event="config_validation_started",
        status=LogStatus.STARTED,
        message="Configuration validation started.",
    )
    validate_config(get_config())
    minutes_schedule, seconds_schedule, grace_time, minutes, seconds = (
        get_scheduling_config()
    )
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_extractloadlivedata_task,
        trigger="cron",
        minute=minutes_schedule,
        second=seconds_schedule,
        misfire_grace_time=grace_time,  # Allows a time window to start if the system was bogged down
    )
    if minutes == 0:
        structured_logger.info(
            event="scheduler_started",
            status=LogStatus.STARTED,
            message="Scheduler configured and starting with second-based cadence.",
            metadata={"seconds": seconds, "grace_time_seconds": grace_time},
        )
    else:
        structured_logger.info(
            event="scheduler_started",
            status=LogStatus.STARTED,
            message="Scheduler configured and starting with minute-based cadence.",
            metadata={"minutes": minutes, "grace_time_seconds": grace_time},
        )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        structured_logger.info(
            event="scheduler_stopped",
            status=LogStatus.SUCCEEDED,
            message="Scheduler stop requested by user.",
        )
        scheduler.shutdown()
        structured_logger.info(
            event="scheduler_shutdown_completed",
            status=LogStatus.SUCCEEDED,
            message="Scheduler shutdown completed.",
        )


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "dev":
            structured_logger.info(
                event="cli_dev_mode_requested",
                status=LogStatus.STARTED,
                message="CLI dev mode detected. Running one immediate task execution.",
            )
            run_extractloadlivedata_task()
        else:
            structured_logger.error(
                event="cli_invalid_parameter",
                status=LogStatus.FAILED,
                message="Invalid CLI parameter for extractloadlivedata.",
                metadata={
                    "received_args": sys.argv[1:],
                    "valid_usage": [
                        "without parameters for scheduled mode",
                        "with 'dev' parameter for one immediate run",
                    ],
                },
            )
            sys.exit(1)
    else:
        main()

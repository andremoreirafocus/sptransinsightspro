from src.extractloadlivedata import extractloadlivedata
from src.config import get_config
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

import logging
from logging.handlers import RotatingFileHandler

LOG_FILENAME = "extractlivedata.log"

# In Airflow just remove this logging configuration block
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        # Rotation: 5MB per file, keeping the last 5 files
        RotatingFileHandler(LOG_FILENAME, maxBytes=5 * 1024 * 1024, backupCount=5),
        logging.StreamHandler(),  # Also keeps console output
    ],
)
logger = logging.getLogger(__name__)


def run_extractloadlivedata_task():
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Task executing...")
    extractloadlivedata()
    logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] Task executed!")


def get_scheduling_config():
    config = get_config()
    interval = int(config["EXTRACTION_INTERVAL_SECONDS"])
    if interval <= 0:
        logger.error("EXTRACTION_INTERVAL_SECONDS must be a positive integer.")
        raise ValueError("EXTRACTION_INTERVAL_SECONDS must be a positive integer.")

    if interval > 60:
        # 'cron' with minute='*/2' ensures it runs at :00, :02, :04...
        # 'second=0' ensures it starts exactly at the start of the minute
        minutes = interval // 60
        seconds = 0
        minutes_schedule = f"*/{minutes}"
        seconds_schedule = 0
        grace_time = 10
        logger.info(
            f"EXTRACTION_INTERVAL_SECONDS translated and set to {minutes} minutes with a grace period of {grace_time} seconds."
        )
    else:
        minutes = 0
        seconds = interval
        minutes_schedule = "*"
        seconds_schedule = f"*/{interval}"
        grace_time = 5
        logger.warning(
            f"EXTRACTION_INTERVAL_SECONDS set to {interval} seconds with a grace period of {grace_time} seconds."
        )
    return minutes_schedule, seconds_schedule, grace_time, minutes, seconds


def main():
    minutes_schedule, seconds_schedule, grace_time, minutes, seconds = (
        get_scheduling_config()
    )
    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_extractloadlivedata_task,
        trigger="cron",
        minute=minutes_schedule,
        second=seconds_schedule,
        misfire_grace_time=grace_time,  # Allows a 30s window to start if the system was bogged down
    )
    if minutes == 0:
        logger.info(
            f"Scheduler configured to run every {seconds} seconds with a grace period of {grace_time} seconds."
        )
    else:
        logger.info(
            f"Scheduler configured to run every {minutes} minutes with a grace period of {grace_time} seconds."
        )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main()
    # run_extractloadlivedata_task()

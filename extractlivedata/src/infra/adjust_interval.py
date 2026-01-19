from datetime import datetime
import time
import logging


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        # Rotation: 5MB per file, keeping the last 5 files
        logging.StreamHandler(),  # Also keeps console output
    ],
)

logger = logging.getLogger(__name__)


def adjust_time():
    # Get the current date and time
    logger.info("Syncing to 2 minute window...")
    while True:
        now = datetime.now()
        # Extract minutes and seconds
        current_minute = now.minute
        current_second = now.second
        if current_minute % 2 != 0:
            print(f"Minute {current_minute} is odd")
        else:
            if current_second < 5:
                logger.info("Synced to 2 minute window start...")
                break
            else:
                logger.info("Syncing to 2 minute window start...")
        # Print the results
        print(f"Current minutes:seconds {current_minute:02d}:{current_second:02d}")
        time.sleep(2)


if __name__ == "__main__":
    adjust_time()

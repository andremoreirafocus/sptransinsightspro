from pipeline_configurator.config import get_config
from pipeline_configurator.general_config import GeneralConfig
import logging
from logging.handlers import RotatingFileHandler


LOG_FILENAME = "gtfs.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILENAME, maxBytes=5 * 1024 * 1024, backupCount=5),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)

PIPELINE_NAME = "gtfs"


class StageExecutionError(ValueError):
    def __init__(self, stage: str, message: str, stage_result: dict):
        super().__init__(message)
        self.stage = stage
        self.stage_result = stage_result


def _load_pipeline_config():
    try:
        return get_config(
            PIPELINE_NAME,
            None,
            GeneralConfig,
            "gtfs_conn",
            "minio_conn",
            None,
        )
    except Exception as e:
        logger.error("Pipeline configuration validation failed: %s", e)
        raise ValueError(f"Pipeline configuration validation failed: {e}")


def extract_load_files_wrapper():
    pipeline_config = _load_pipeline_config()
    return stage_results


def transform_wrapper():
    pipeline_config = _load_pipeline_config()
    return stage_results


def create_trip_details_wrapper():
    pipeline_config = _load_pipeline_config()
    return stage_results


def build_quality_report_and_send_webhook_wrapper():
    pipeline_config = _load_pipeline_config()


def main():
    stage_results = extract_load_files_wrapper()
    stage_results = transform_wrapper(stage_results)
    stage_results = create_trip_details_wrapper(stage_results)
    build_quality_report_and_send_webhook_wrapper(stage_results)


if __name__ == "__main__":
    main()

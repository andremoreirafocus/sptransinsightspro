from datetime import datetime
from typing import Any, Callable, Dict

from sqlalchemy import create_engine, text

from observability.structured_event_logger import get_structured_logger

structured_logger = get_structured_logger(logger_name=__name__)


def measure_input_trips(
    logic_date: datetime,
    config: Dict[str, Any],
    engine_factory: Callable = create_engine,
) -> Dict[str, Any]:
    def get_config(config):
        general = config["general"]
        database = config["connections"]["database"]
        table_name = general["tables"]["finished_trips_table_name"]
        db_uri = (
            f"postgresql://{database['user']}:{database['password']}"
            f"@{database['host']}:{database['port']}/{database['database']}"
        )
        return table_name, db_uri

    table_name, db_uri = get_config(config)
    engine = engine_factory(db_uri)
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    f"SELECT COUNT(*) AS finished_trips_read "
                    f"FROM {table_name} "
                    f"WHERE logic_date = :logic_date"
                ),
                {"logic_date": logic_date},
            )
            count = result.fetchone()[0]
            return {"finished_trips_read": int(count)}
    except Exception as e:
        structured_logger.error(
            event="input_trips_measurement_failed",
            message=f"Failed to measure input trips for logic_date '{logic_date}' from '{table_name}': {e}",
            metadata={"logic_date": str(logic_date), "table_name": table_name},
        )
        raise ValueError(
            f"Failed to measure input trips for logic_date '{logic_date}' from '{table_name}': {e}"
        )
    finally:
        engine.dispose()

from src.services.create_latest_positions import (
    create_latest_positions_table,
    create_positions_table_by_minute,
)
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def update_latest_positions(config):
    # create_latest_positions_table(config)
    year = 2026
    month = 1
    day = 20
    minute = 40
    hour = 15
    create_positions_table_by_minute(config, year, month, day, hour, minute)
    
    

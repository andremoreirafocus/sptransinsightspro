from src.services.create_latest_positions import create_latest_positions_table
import logging

# This logger inherits the configuration from the root logger in main.py
logger = logging.getLogger(__name__)


def update_latest_positions(config):
    create_latest_positions_table(config)

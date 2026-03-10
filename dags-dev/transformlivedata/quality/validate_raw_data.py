"""
Raw Data Expectations: Pre-transformation validation using JSON Schema.

Validates raw API responses against raw_expectations.json schema before
any data transformation occurs.
"""

import json
import logging
from typing import Dict, Tuple, List

import jsonschema

logger = logging.getLogger(__name__)


def validate_raw_data(data: Dict, config_file: str) -> Tuple[bool, List[str]]:
    """Validate raw API response dict against JSON schema.

    Args:
        data: Raw API response dict to validate
        config_file: Path to raw_expectations.json schema file

    Returns:
        Tuple of (is_valid, error_messages)
        - is_valid: True if validation passed, False otherwise
        - error_messages: List of validation error messages (empty if valid)

    Raises:
        FileNotFoundError: If schema config_file does not exist
        json.JSONDecodeError: If schema config_file is not valid JSON
    """
    try:
        schema = load_raw_schema(config_file)
        jsonschema.validate(instance=data, schema=schema)
        logger.debug("Raw data validation passed")
        return True, []
    except jsonschema.ValidationError as e:
        error_msg = str(e)
        logger.warning(f"Raw data validation failed: {error_msg}")
        return False, [error_msg]
    except Exception as e:
        error_msg = f"Unexpected error during raw data validation: {str(e)}"
        logger.error(error_msg)
        return False, [error_msg]


def load_raw_schema(config_file: str) -> Dict:
    """Load raw_expectations.json JSON schema from file.

    Args:
        config_file: Path to raw_expectations.json file

    Returns:
        Parsed JSON schema dictionary

    Raises:
        FileNotFoundError: If config_file does not exist
        json.JSONDecodeError: If config_file is not valid JSON
    """
    with open(config_file) as f:
        schema = json.load(f)
    logger.info(f"Loaded raw data expectations schema from {config_file}")
    return schema

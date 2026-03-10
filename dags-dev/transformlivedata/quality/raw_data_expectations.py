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


class RawDataExpectations:
    """Pre-transformation validation on raw API response using JSON Schema (jsonschema-driven)"""

    def __init__(self, config_file: str) -> None:
        """Load raw_expectations.json JSON schema

        Args:
            config_file: Path to raw_expectations.json file
        """
        with open(config_file) as f:
            self.schema = json.load(f)
        logger.info(f"Loaded RawDataExpectations schema from {config_file}")

    def validate(self, data: Dict) -> Tuple[bool, List[str]]:
        """Validate raw API response dict against JSON schema

        Args:
            data: Raw API response dict to validate

        Returns:
            Tuple of (is_valid, error_messages)
            - is_valid: True if validation passed, False otherwise
            - error_messages: List of validation error messages (empty if valid)
        """
        try:
            jsonschema.validate(instance=data, schema=self.schema)
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

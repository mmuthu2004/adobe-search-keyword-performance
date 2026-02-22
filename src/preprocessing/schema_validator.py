"""
schema_validator.py
===================
Validates that the input file contains all required columns.
Raises SchemaValidationError if any required column is missing.
Config-driven: reads required_columns from config.yaml
"""
import logging
from typing import List

logger = logging.getLogger(__name__)


class SchemaValidationError(Exception):
    pass


class SchemaValidator:

    def __init__(self, cfg):
        self.required = cfg.require("input.required_columns")
        self.fail_hard = cfg.get("data_quality.fail_on_schema_error", default=True)

    def validate(self, actual_columns: List[str]) -> dict:
        """
        Compare actual columns against required columns.

        Returns:
            dict with keys: passed (bool), missing (list), extra (list)

        Raises:
            SchemaValidationError if fail_hard=True and columns are missing.
        """
        actual_set   = set(actual_columns)
        required_set = set(self.required)

        missing = sorted(required_set - actual_set)
        extra   = sorted(actual_set - required_set)

        passed = len(missing) == 0

        result = {
            "check":   "schema_validation",
            "passed":  passed,
            "missing": missing,
            "extra":   extra,
            "actual_columns": list(actual_columns),
        }

        if passed:
            logger.info("Schema validation PASSED. Columns: %s", actual_columns)
        else:
            msg = f"Schema validation FAILED. Missing columns: {missing}"
            logger.error(msg)
            if self.fail_hard:
                raise SchemaValidationError(msg)
            logger.warning("fail_on_schema_error=false — continuing with warnings")

        return result

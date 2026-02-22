"""
null_validator.py
=================
Checks null/empty values across all columns.
Critical columns (ip, referrer, hit_time_gmt) must never be null — pipeline halts.
Other columns emit warnings if null % exceeds configured threshold.
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class NullValidationError(Exception):
    pass


class NullValidator:

    def __init__(self, cfg):
        self.critical_cols  = cfg.require("data_quality.critical_columns")
        self.warn_threshold = cfg.get("data_quality.null_warning_threshold_pct", default=50)
        self.fail_hard      = cfg.get("data_quality.fail_on_null_critical", default=True)

    def validate(self, df: pd.DataFrame) -> dict:
        """
        Check null counts across all columns.

        Returns:
            dict with null stats per column and overall pass/fail.

        Raises:
            NullValidationError if a critical column contains nulls and fail_hard=True.
        """
        total_rows = len(df)
        null_stats = {}
        critical_failures = []

        for col in df.columns:
            # Treat empty strings as null too
            null_count = df[col].replace("", pd.NA).isna().sum()
            null_pct   = round((null_count / total_rows * 100), 2) if total_rows > 0 else 0

            null_stats[col] = {
                "null_count": int(null_count),
                "null_pct":   null_pct,
            }

            if col in self.critical_cols and null_count > 0:
                critical_failures.append(col)
                logger.error(
                    "CRITICAL column '%s' has %d null(s) (%s%%) — pipeline cannot continue.",
                    col, null_count, null_pct
                )
            elif null_pct > self.warn_threshold:
                logger.warning(
                    "Column '%s' has high null rate: %s%% (%d/%d rows).",
                    col, null_pct, null_count, total_rows
                )
            else:
                logger.debug("Column '%s': %d nulls (%s%%)", col, null_count, null_pct)

        passed = len(critical_failures) == 0

        result = {
            "check":              "null_validation",
            "passed":             passed,
            "total_rows":         total_rows,
            "critical_failures":  critical_failures,
            "column_null_stats":  null_stats,
        }

        if not passed and self.fail_hard:
            raise NullValidationError(
                f"Null validation FAILED. Critical columns with nulls: {critical_failures}"
            )

        if passed:
            logger.info("Null validation PASSED. No critical column nulls.")

        return result

"""
duplicate_checker.py
====================
Detects and removes duplicate rows using a composite key.
Key columns: hit_time_gmt + ip + page_url (config-driven).
Duplicates are logged and removed — pipeline continues.
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class DuplicateChecker:

    def __init__(self, cfg):
        self.key_cols = cfg.require("data_quality.duplicate_key_columns")

    def check_and_remove(self, df: pd.DataFrame) -> tuple:
        """
        Identify and remove duplicate rows.

        Returns:
            (deduplicated_df, result_dict)
        """
        # Only check key columns that actually exist in the df
        available_keys = [c for c in self.key_cols if c in df.columns]

        if not available_keys:
            logger.warning("No duplicate key columns found in DataFrame — skipping dedup")
            return df, {"check": "duplicate_check", "passed": True,
                        "duplicate_count": 0, "rows_before": len(df), "rows_after": len(df)}

        before = len(df)
        df_deduped = df.drop_duplicates(subset=available_keys, keep="first")
        after = len(df_deduped)
        dupes = before - after

        result = {
            "check":           "duplicate_check",
            "passed":          True,          # always passes — dupes are removed, not fatal
            "key_columns":     available_keys,
            "rows_before":     before,
            "rows_after":      after,
            "duplicate_count": dupes,
        }

        if dupes > 0:
            logger.warning(
                "Duplicate check: %d duplicate row(s) removed on key %s. "
                "Rows: %d → %d", dupes, available_keys, before, after
            )
        else:
            logger.info("Duplicate check PASSED. No duplicates found on key %s.", available_keys)

        return df_deduped, result

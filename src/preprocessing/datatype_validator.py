"""
datatype_validator.py
=====================
Validates and coerces column data types.
Bad rows are logged and dropped — pipeline continues with valid rows.
Config-driven: reads column_dtypes from config.yaml
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class DatatypeValidator:

    def __init__(self, cfg):
        self.dtypes = cfg.get("input.column_dtypes", default={})

    def validate_and_coerce(self, df: pd.DataFrame) -> tuple:
        """
        Attempt to coerce columns to their expected types.
        Rows that fail coercion are dropped.

        Returns:
            (coerced_df, result_dict)
        """
        bad_rows_total = 0
        coercion_log   = {}
        df_out = df.copy()

        for col, expected_type in self.dtypes.items():
            if col not in df_out.columns:
                logger.debug("Column '%s' not in DataFrame — skipping type check", col)
                continue

            before = len(df_out)

            if expected_type == "int":
                df_out[col] = pd.to_numeric(df_out[col], errors="coerce")
                bad = df_out[col].isna().sum()
                df_out = df_out.dropna(subset=[col])
                df_out[col] = df_out[col].astype(int)

            elif expected_type == "datetime":
                df_out[col] = pd.to_datetime(df_out[col], errors="coerce")
                bad = df_out[col].isna().sum()
                df_out = df_out.dropna(subset=[col])

            elif expected_type == "float":
                df_out[col] = pd.to_numeric(df_out[col], errors="coerce")
                bad = df_out[col].isna().sum()
                df_out = df_out.dropna(subset=[col])

            else:
                # string — just strip whitespace
                df_out[col] = df_out[col].astype(str).str.strip()
                bad = 0

            bad_rows_total += bad
            coercion_log[col] = {
                "expected_type": expected_type,
                "bad_rows_dropped": int(bad),
                "rows_before": before,
                "rows_after": len(df_out),
            }

            if bad > 0:
                logger.warning(
                    "Column '%s': %d row(s) failed type coercion to '%s' — rows dropped.",
                    col, bad, expected_type
                )
            else:
                logger.debug("Column '%s': all values valid as '%s'.", col, expected_type)

        result = {
            "check":            "datatype_validation",
            "passed":           True,
            "total_bad_rows":   bad_rows_total,
            "coercion_details": coercion_log,
            "rows_out":         len(df_out),
        }

        logger.info(
            "Datatype validation complete. %d bad rows dropped. %d rows remain.",
            bad_rows_total, len(df_out)
        )

        return df_out, result

"""
preprocessing_pipeline.py
==========================
Pure Python preprocessing — zero dependencies (no Pandas, no NumPy).
Uses only Python standard library: csv, io, logging.

Orchestrates 4 checks:
  1. Schema validation   — required columns present
  2. CRLF normalisation  — transparent auto-fix
  3. Null validation     — critical columns have no nulls
  4. Duplicate removal   — composite key deduplication

Returns: (clean_rows, error_rows, dq_report)
  clean_rows  — list of row dicts that passed all checks
  error_rows  — list of row dicts that failed, each with an added
                '_error_reason' column describing why it was rejected
  dq_report   — dict with check results and summary stats
"""
import csv
import io
import logging

logger = logging.getLogger(__name__)


class SchemaValidationError(Exception):
    pass


class NullValidationError(Exception):
    pass


class PreprocessingPipeline:

    def __init__(self, cfg):
        self.delimiter      = cfg.get("input.file_delimiter",              default="\t")
        self.strip_crlf     = cfg.get("input.strip_crlf",                  default=True)
        self.required_cols  = cfg.require("input.required_columns")
        self.critical_cols  = cfg.require("data_quality.critical_columns")
        self.dedup_keys     = cfg.require("data_quality.duplicate_key_columns")
        self.fail_schema    = cfg.get("data_quality.fail_on_schema_error",  default=True)
        self.fail_null      = cfg.get("data_quality.fail_on_null_critical", default=False)  # Continue by default
        self.warn_threshold = cfg.get("data_quality.null_warning_threshold_pct", default=50)

    def run(self, raw_content: str) -> tuple:
        """
        Run all preprocessing checks on raw file content string.

        Returns:
            (clean_rows, error_rows, dq_report)
            clean_rows  — list of dicts that passed all checks
            error_rows  — list of dicts that failed, with '_error_reason' column
            dq_report   — dict with check results and summary stats
        """
        logger.info("=== Preprocessing Pipeline Start ===")
        dq_report  = {"checks": []}
        error_rows = []

        # ── Step 0: CRLF normalisation ───────────────────────────────
        if self.strip_crlf:
            raw_content = raw_content.replace("\r\n", "\n").replace("\r", "\n")

        # ── Step 1: Parse CSV to list of dicts ───────────────────────
        reader = csv.DictReader(
            io.StringIO(raw_content),
            delimiter=self.delimiter
        )
        rows = [dict(row) for row in reader]
        cols = list(reader.fieldnames or [])
        total = len(rows)
        logger.info("Parsed %d rows x %d columns.", total, len(cols))

        # ── Step 2: Schema validation ────────────────────────────────
        # Schema is file-level — if columns are missing, tag ALL rows
        # as schema errors and return immediately (nothing to process).
        missing = sorted(set(self.required_cols) - set(cols))
        schema_result = {
            "check":          "schema_validation",
            "passed":         len(missing) == 0,
            "missing":        missing,
            "actual_columns": cols,
        }
        dq_report["checks"].append(schema_result)

        if missing:
            msg = f"Schema validation FAILED. Missing columns: {missing}"
            logger.error(msg)
            if self.fail_schema:
                # Tag all rows as schema errors before raising
                for row in rows:
                    row["_error_reason"] = f"schema_error: missing columns {missing}"
                    error_rows.append(row)
                dq_report["overall_passed"] = False
                dq_report["rows_in"]        = total
                dq_report["rows_out"]       = 0
                dq_report["error_count"]    = len(error_rows)
                raise SchemaValidationError(msg)
        else:
            logger.info(
                "Schema validation PASSED. All %d required columns present.",
                len(self.required_cols)
            )

        # ── Step 3: Null validation ──────────────────────────────────
        # Row-level check — tag individual rows with null critical columns.
        null_error_rows = set()  # track row indices already marked as errors

        for col in self.critical_cols:
            for idx, row in enumerate(rows):
                if not row.get(col, "").strip():
                    null_error_rows.add(idx)
                    # Append to error list with reason; preserve original row data
                    error_row = dict(row)
                    error_row["_error_reason"] = f"null_critical_column: {col}"
                    error_rows.append(error_row)
                    logger.debug("Row %d rejected — null in critical column '%s'.", idx, col)

        if null_error_rows:
            logger.error(
                "Null validation: %d row(s) rejected for null in critical columns.",
                len(null_error_rows)
            )
        else:
            logger.info("Null validation PASSED. No critical column nulls.")

        # Check non-critical columns for high null rates (warn only, no rejection)
        for col in cols:
            if col in self.critical_cols:
                continue
            null_count = sum(1 for r in rows if not r.get(col, "").strip())
            null_pct   = round(null_count / total * 100, 2) if total > 0 else 0
            if null_pct > self.warn_threshold:
                logger.warning(
                    "Column '%s' has high null rate: %s%% (%d/%d rows).",
                    col, null_pct, null_count, total
                )

        null_result = {
            "check":             "null_validation",
            "passed":            len(null_error_rows) == 0,
            "critical_failures": len(null_error_rows),
            "total_rows":        total,
        }
        dq_report["checks"].append(null_result)

        # Remove null error rows from clean set before dedup
        rows = [row for idx, row in enumerate(rows) if idx not in null_error_rows]

        # ── Step 4: Duplicate removal ────────────────────────────────
        # Tag duplicate rows as errors; keep first occurrence as clean.
        seen    = set()
        deduped = []
        for row in rows:
            key = tuple(row.get(k, "").strip() for k in self.dedup_keys if k in row)
            if key not in seen:
                seen.add(key)
                deduped.append(row)
            else:
                error_row = dict(row)
                error_row["_error_reason"] = f"duplicate_row: key={key}"
                error_rows.append(error_row)
                logger.debug("Row rejected — duplicate key %s.", key)

        dupes = len(rows) - len(deduped)
        dq_report["checks"].append({
            "check":           "duplicate_check",
            "passed":          True,   # duplicates are always non-fatal
            "duplicate_count": dupes,
            "rows_before":     len(rows),
            "rows_after":      len(deduped),
        })

        if dupes > 0:
            logger.warning("Duplicate check: %d duplicate row(s) removed.", dupes)
        else:
            logger.info("Duplicate check PASSED. No duplicates found.")

        rows = deduped

        # ── Summary ──────────────────────────────────────────────────
        dq_report["overall_passed"] = all(c.get("passed", True) for c in dq_report["checks"])
        dq_report["rows_in"]        = total
        dq_report["rows_out"]       = len(rows)
        dq_report["error_count"]    = len(error_rows)

        logger.info(
            "=== Preprocessing Complete. Rows in: %d | Clean: %d | "
            "Errors: %d | All checks passed: %s ===",
            total, len(rows), len(error_rows), dq_report["overall_passed"]
        )
        return rows, error_rows, dq_report

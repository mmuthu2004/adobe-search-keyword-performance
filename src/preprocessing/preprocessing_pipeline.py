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

Returns: (list of row dicts, dq_report dict)
Each row dict has column names as keys, values as strings.
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
        self.fail_null      = cfg.get("data_quality.fail_on_null_critical", default=True)
        self.warn_threshold = cfg.get("data_quality.null_warning_threshold_pct", default=50)

    def run(self, raw_content: str) -> tuple:
        """
        Run all preprocessing checks on raw file content string.

        Returns:
            (rows, dq_report)
            rows      — list of dicts, one per data row
            dq_report — dict with check results and summary stats
        """
        logger.info("=== Preprocessing Pipeline Start ===")
        dq_report = {"checks": []}

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
        missing = sorted(set(self.required_cols) - set(cols))
        schema_result = {
            "check":   "schema_validation",
            "passed":  len(missing) == 0,
            "missing": missing,
            "actual_columns": cols,
        }
        dq_report["checks"].append(schema_result)

        if missing:
            msg = f"Schema validation FAILED. Missing columns: {missing}"
            logger.error(msg)
            if self.fail_schema:
                raise SchemaValidationError(msg)
        else:
            logger.info("Schema validation PASSED. All %d required columns present.", len(self.required_cols))

        # ── Step 3: Null validation ──────────────────────────────────
        critical_failures = []
        for col in self.critical_cols:
            null_count = sum(1 for r in rows if not r.get(col, "").strip())
            null_pct   = round(null_count / total * 100, 2) if total > 0 else 0
            if null_count > 0:
                critical_failures.append(col)
                logger.error(
                    "CRITICAL column '%s' has %d null(s) (%s%%) — pipeline cannot continue.",
                    col, null_count, null_pct
                )

        # Check non-critical columns for high null rates
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
            "passed":            len(critical_failures) == 0,
            "critical_failures": critical_failures,
            "total_rows":        total,
        }
        dq_report["checks"].append(null_result)

        if critical_failures and self.fail_null:
            raise NullValidationError(
                f"Null validation FAILED. Critical columns with nulls: {critical_failures}"
            )
        logger.info("Null validation PASSED. No critical column nulls.")

        # ── Step 4: Duplicate removal ────────────────────────────────
        seen    = set()
        deduped = []
        for row in rows:
            key = tuple(row.get(k, "").strip() for k in self.dedup_keys if k in row)
            if key not in seen:
                seen.add(key)
                deduped.append(row)

        dupes = total - len(deduped)
        dq_report["checks"].append({
            "check":           "duplicate_check",
            "passed":          True,
            "duplicate_count": dupes,
            "rows_before":     total,
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

        logger.info(
            "=== Preprocessing Complete. Rows in: %d | Rows out: %d | All checks passed: %s ===",
            total, len(rows), dq_report["overall_passed"]
        )
        return rows, dq_report

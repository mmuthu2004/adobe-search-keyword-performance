"""
preprocessing_pipeline.py
==========================
Orchestrates all 4 preprocessing checks in sequence:
  1. Schema validation  (fail-fast if columns missing)
  2. CRLF normalization (transparent auto-fix)
  3. Null validation    (fail-fast on critical columns)
  4. Duplicate removal  (always continues)
  5. Datatype coercion  (always continues)

Returns the clean DataFrame and a consolidated DQ report dict.
"""
import logging
import io
import pandas as pd

from preprocessing.schema_validator   import SchemaValidator
from preprocessing.null_validator      import NullValidator
from preprocessing.duplicate_checker   import DuplicateChecker
from preprocessing.datatype_validator  import DatatypeValidator

logger = logging.getLogger(__name__)


class PreprocessingPipeline:

    def __init__(self, cfg):
        self.cfg        = cfg
        self.schema_v   = SchemaValidator(cfg)
        self.null_v     = NullValidator(cfg)
        self.dedup_v    = DuplicateChecker(cfg)
        self.dtype_v    = DatatypeValidator(cfg)

        self.delimiter  = cfg.get("input.file_delimiter", default="\t")
        self.encoding   = cfg.get("input.file_encoding",  default="utf-8")
        self.strip_crlf = cfg.get("input.strip_crlf",     default=True)

    def run(self, raw_content: str) -> tuple:
        """
        Run all preprocessing checks on the raw file content string.

        Args:
            raw_content: Raw file content as a string (read from S3 or local).

        Returns:
            (clean_df, dq_report)

        Raises:
            SchemaValidationError / NullValidationError on hard failures.
        """
        logger.info("=== Preprocessing Pipeline Start ===")
        dq_report = {"checks": []}

        # ── Step 0: CRLF normalisation ──────────────────────────────
        if self.strip_crlf:
            original_len = len(raw_content)
            raw_content  = raw_content.replace("\r\n", "\n").replace("\r", "\n")
            stripped     = original_len - len(raw_content)
            if stripped:
                logger.info("CRLF normalisation: stripped %d carriage-return characters.", stripped)

        # ── Step 1: Parse to DataFrame ───────────────────────────────
        df = pd.read_csv(
            io.StringIO(raw_content),
            sep=self.delimiter,
            dtype=str,          # read everything as string first
            keep_default_na=False,
        )
        logger.info("Parsed %d rows × %d columns from input.", len(df), len(df.columns))

        # ── Step 2: Schema validation ────────────────────────────────
        schema_result = self.schema_v.validate(list(df.columns))
        dq_report["checks"].append(schema_result)

        # ── Step 3: Null validation ──────────────────────────────────
        null_result = self.null_v.validate(df)
        dq_report["checks"].append(null_result)

        # ── Step 4: Duplicate removal ────────────────────────────────
        df, dedup_result = self.dedup_v.check_and_remove(df)
        dq_report["checks"].append(dedup_result)

        # ── Step 5: Datatype coercion ────────────────────────────────
        df, dtype_result = self.dtype_v.validate_and_coerce(df)
        dq_report["checks"].append(dtype_result)

        # ── Summary ─────────────────────────────────────────────────
        all_passed = all(c.get("passed", True) for c in dq_report["checks"])
        dq_report["overall_passed"] = all_passed
        dq_report["rows_in"]        = null_result["total_rows"]
        dq_report["rows_out"]       = len(df)

        logger.info(
            "=== Preprocessing Pipeline Complete. "
            "Rows in: %d | Rows out: %d | All checks passed: %s ===",
            dq_report["rows_in"], dq_report["rows_out"], all_passed
        )

        return df, dq_report

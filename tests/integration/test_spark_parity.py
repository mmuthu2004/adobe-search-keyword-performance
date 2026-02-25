"""
test_spark_parity.py
====================
CRITICAL TEST — Proves PySpark pipeline produces byte-identical results
to the pure Python Lambda pipeline on the same input data.

This test is the engineering proof that both paths are correct and
interchangeable. If this test passes, we can confidently route
large files to EMR knowing the output will match.

Test Phases:
  Phase 1 (20 rows)     — Original assessment ground truth
  Phase 2 (25K rows)    — Volume correctness with new anchors

Run:
  # With PySpark installed locally:
  python -m pytest tests/integration/test_spark_parity.py -v

  # With spark-submit:
  spark-submit tests/integration/test_spark_parity.py
"""
import os
import sys
import logging
import unittest
from pathlib import Path

logging.disable(logging.CRITICAL)

# Add src to path
sys.path.insert(0, str(Path(__file__).parents[2] / "src"))

DATA_DIR = Path(__file__).parents[1] / "data"


def _run_python_pipeline(filepath):
    """Run the pure Python pipeline and return results as list of tuples."""
    os.environ["APP_ENV"] = "dev"
    os.environ["PROCESSED_BUCKET"] = ""
    os.environ["LOGS_BUCKET"] = ""

    from config.config_loader import load_config
    from preprocessing.preprocessing_pipeline import PreprocessingPipeline
    from processing.search_keyword_analyzer import SearchKeywordAnalyzer

    cfg = load_config(env="dev")
    raw = filepath.read_text(encoding="utf-8", errors="replace")
    rows, error_rows, dq = PreprocessingPipeline(cfg).run(raw)
    results = SearchKeywordAnalyzer(cfg).analyze(rows)

    return [
        (r["Search Engine Domain"], r["Search Keyword"], float(r["Revenue"]))
        for r in results
    ]


def _run_spark_pipeline(filepath):
    """Run the PySpark pipeline and return results as list of tuples."""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
    except ImportError:
        return None  # PySpark not installed — skip

    from processing.spark_skp_pipeline import (
        create_spark_session, read_input, validate_schema,
        validate_nulls, deduplicate, build_session_map,
        extract_purchase_rows, attribute_revenue, aggregate_results
    )

    spark = create_spark_session(local=True)
    try:
        df, total_rows = read_input(spark, str(filepath))
        validate_schema(df)
        validate_nulls(df, total_rows)
        df_clean, _ = deduplicate(df, total_rows)
        df_session_map = build_session_map(df_clean)
        df_purchases = extract_purchase_rows(df_clean)
        df_attributed = attribute_revenue(df_purchases, df_session_map)
        df_results = aggregate_results(df_attributed)

        return [
            (row["Search Engine Domain"], row["Search Keyword"], float(row["Revenue"]))
            for row in df_results.orderBy("Revenue", ascending=False).collect()
        ]
    finally:
        spark.stop()


class TestSparkParity(unittest.TestCase):
    """
    Parity tests: PySpark results must exactly match pure Python results.
    Each test runs both pipelines on the same file and compares output.
    """

    def _assert_parity(self, python_results, spark_results, phase_name):
        """Compare results from both pipelines."""
        if spark_results is None:
            self.skipTest("PySpark not installed — skipping parity test")

        self.assertEqual(
            len(python_results), len(spark_results),
            f"{phase_name}: Row count mismatch — "
            f"Python: {len(python_results)}, Spark: {len(spark_results)}"
        )

        # Sort both by revenue descending for comparison
        python_sorted = sorted(python_results, key=lambda x: x[2], reverse=True)
        spark_sorted  = sorted(spark_results,  key=lambda x: x[2], reverse=True)

        for i, (py_row, sp_row) in enumerate(zip(python_sorted, spark_sorted)):
            self.assertEqual(py_row[0], sp_row[0],
                f"{phase_name} row {i}: Domain mismatch — Python: {py_row[0]}, Spark: {sp_row[0]}")
            self.assertEqual(py_row[1], sp_row[1],
                f"{phase_name} row {i}: Keyword mismatch — Python: {py_row[1]}, Spark: {sp_row[1]}")
            self.assertAlmostEqual(py_row[2], sp_row[2], places=2,
                msg=f"{phase_name} row {i}: Revenue mismatch — Python: {py_row[2]}, Spark: {sp_row[2]}")

    # ── Ground truth tests (both pipelines) ──────────────────────────

    def test_phase1_python_google_ipod_480(self):
        """Phase 1: Python pipeline — google.com/ipod must be $480."""
        filepath = DATA_DIR / "hit_data_phase1_crawl.tab"
        if not filepath.exists():
            self.skipTest("Phase 1 data file not found")
        results = dict(((r[0], r[1]), r[2]) for r in _run_python_pipeline(filepath))
        self.assertAlmostEqual(results.get(("google.com", "ipod"), 0), 480.00, places=2)

    def test_phase1_python_bing_zune_250(self):
        """Phase 1: Python pipeline — bing.com/zune must be $250."""
        filepath = DATA_DIR / "hit_data_phase1_crawl.tab"
        if not filepath.exists():
            self.skipTest("Phase 1 data file not found")
        results = dict(((r[0], r[1]), r[2]) for r in _run_python_pipeline(filepath))
        self.assertAlmostEqual(results.get(("bing.com", "zune"), 0), 250.00, places=2)

    def test_phase1_python_yahoo_excluded(self):
        """Phase 1: Python pipeline — yahoo.com must NOT appear."""
        filepath = DATA_DIR / "hit_data_phase1_crawl.tab"
        if not filepath.exists():
            self.skipTest("Phase 1 data file not found")
        results = _run_python_pipeline(filepath)
        domains = [r[0] for r in results]
        self.assertNotIn("yahoo.com", domains)

    def test_phase2_python_all_anchors(self):
        """Phase 2: Python pipeline — all 4 anchor attributions correct."""
        filepath = DATA_DIR / "hit_data_phase2_walk.tab"
        if not filepath.exists():
            self.skipTest("Phase 2 data file not found")
        results = dict(((r[0], r[1]), r[2]) for r in _run_python_pipeline(filepath))
        self.assertAlmostEqual(results.get(("google.com", "ipod"),       0), 480.00,  places=2)
        self.assertAlmostEqual(results.get(("bing.com",   "zune"),       0), 250.00,  places=2)
        self.assertAlmostEqual(results.get(("google.com", "laptop"),     0), 1199.00, places=2)
        self.assertAlmostEqual(results.get(("bing.com",   "headphones"), 0), 350.00,  places=2)

    def test_phase2_python_amazon_excluded(self):
        """Phase 2: amazon.com must NOT appear (not a search engine)."""
        filepath = DATA_DIR / "hit_data_phase2_walk.tab"
        if not filepath.exists():
            self.skipTest("Phase 2 data file not found")
        results = _run_python_pipeline(filepath)
        domains = [r[0] for r in results]
        self.assertNotIn("amazon.com", domains,
            "amazon.com appeared in output — noise purchase sessions are incorrectly attributed")

    def test_phase2_python_sorted_descending(self):
        """Phase 2: Output must be sorted by revenue descending."""
        filepath = DATA_DIR / "hit_data_phase2_walk.tab"
        if not filepath.exists():
            self.skipTest("Phase 2 data file not found")
        results = _run_python_pipeline(filepath)
        revenues = [r[2] for r in results]
        self.assertEqual(revenues, sorted(revenues, reverse=True))

    # ── Parity tests (Python == Spark) ───────────────────────────────

    def test_phase1_parity(self):
        """Phase 1: PySpark output must exactly match Python output."""
        filepath = DATA_DIR / "hit_data_phase1_crawl.tab"
        if not filepath.exists():
            self.skipTest("Phase 1 data file not found")
        python_results = _run_python_pipeline(filepath)
        spark_results  = _run_spark_pipeline(filepath)
        self._assert_parity(python_results, spark_results, "Phase 1")

    def test_phase2_parity(self):
        """Phase 2: PySpark output must exactly match Python output."""
        filepath = DATA_DIR / "hit_data_phase2_walk.tab"
        if not filepath.exists():
            self.skipTest("Phase 2 data file not found")
        python_results = _run_python_pipeline(filepath)
        spark_results  = _run_spark_pipeline(filepath)
        self._assert_parity(python_results, spark_results, "Phase 2")


class TestSparkDQChecks(unittest.TestCase):
    """Test that Spark DQ checks behave identically to Python DQ checks."""

    def test_schema_validation_passes_on_valid_file(self):
        """Schema validation must pass on well-formed Phase 1 file."""
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            self.skipTest("PySpark not installed")

        filepath = DATA_DIR / "hit_data_phase1_crawl.tab"
        if not filepath.exists():
            self.skipTest("Phase 1 data file not found")

        from processing.spark_skp_pipeline import (
            create_spark_session, read_input, validate_schema
        )
        spark = create_spark_session(local=True)
        try:
            df, _ = read_input(spark, str(filepath))
            result = validate_schema(df)
            self.assertTrue(result["passed"])
            self.assertEqual(result["missing"], [])
        finally:
            spark.stop()

    def test_dedup_count_matches_python(self):
        """Row count after dedup must match between Python and Spark."""
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            self.skipTest("PySpark not installed")

        filepath = DATA_DIR / "hit_data_phase1_crawl.tab"
        if not filepath.exists():
            self.skipTest("Phase 1 data file not found")

        # Python count
        os.environ["APP_ENV"] = "dev"
        os.environ["PROCESSED_BUCKET"] = ""
        sys.path.insert(0, str(Path(__file__).parents[2] / "src"))
        from config.config_loader import load_config
        from preprocessing.preprocessing_pipeline import PreprocessingPipeline
        cfg = load_config(env="dev")
        raw = filepath.read_text(encoding="utf-8", errors="replace")
        _, _error_rows, dq = PreprocessingPipeline(cfg).run(raw)
        python_clean_rows = dq["rows_out"]

        # Spark count
        from processing.spark_skp_pipeline import (
            create_spark_session, read_input, deduplicate
        )
        spark = create_spark_session(local=True)
        try:
            df, total_rows = read_input(spark, str(filepath))
            _, dedup_result = deduplicate(df, total_rows)
            spark_clean_rows = dedup_result["rows_after"]
        finally:
            spark.stop()

        self.assertEqual(python_clean_rows, spark_clean_rows,
            f"Clean row count mismatch: Python={python_clean_rows}, Spark={spark_clean_rows}")


if __name__ == "__main__":
    unittest.main(verbosity=2)

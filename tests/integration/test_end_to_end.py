"""
test_end_to_end.py
==================
Integration test — runs the full pipeline against the real sample file
and verifies output matches the documented ground truth exactly.

Ground truth (keyword_normalize_case=true):
  google.com  |  ipod  |  480.00   (Ipod $290 + ipod $190 combined)
  bing.com    |  zune  |  250.00

v2: analyzer returns list of dicts, not a Pandas DataFrame.

Run: python -m pytest tests/integration/test_end_to_end.py -v
"""
import os
import sys
import unittest
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parents[2] / "src"))

DATA_FILE = Path(__file__).parents[1] / "data" / "hit_data.tab"


class TestEndToEnd(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not DATA_FILE.exists():
            raise unittest.SkipTest(f"Sample data file not found: {DATA_FILE}")

        os.environ["APP_ENV"]          = "dev"
        os.environ["PROCESSED_BUCKET"] = ""
        os.environ["LOGS_BUCKET"]      = ""

        from config.config_loader import load_config
        cls.cfg = load_config(env="dev")

    def _run_pipeline(self):
        """Load the file and run preprocessing + analysis."""
        from preprocessing.preprocessing_pipeline import PreprocessingPipeline
        from processing.search_keyword_analyzer   import SearchKeywordAnalyzer

        raw      = DATA_FILE.read_text(encoding="utf-8", errors="replace")
        pipeline = PreprocessingPipeline(self.cfg)
        rows, error_rows, dq = pipeline.run(raw)

        analyzer = SearchKeywordAnalyzer(self.cfg)
        results  = analyzer.analyze(rows)
        return results, dq

    # ── Helper methods for list of dicts ────────────────────────────

    def _get_row(self, results, domain, keyword):
        """Find a specific row by domain and keyword."""
        for row in results:
            if row["Search Engine Domain"] == domain and row["Search Keyword"] == keyword:
                return row
        return None

    def _get_revenues(self, results):
        """Get list of revenue values."""
        return [float(r["Revenue"]) for r in results]

    def _get_domains(self, results):
        """Get list of domain values."""
        return [r["Search Engine Domain"] for r in results]

    # ── Ground truth tests ──────────────────────────────────────────

    def test_output_has_correct_number_of_rows(self):
        results, _ = self._run_pipeline()
        self.assertEqual(len(results), 2,
            f"Expected 2 output rows, got {len(results)}:\n{results}")

    def test_output_columns_are_correct(self):
        results, _ = self._run_pipeline()
        self.assertGreater(len(results), 0, "Results should not be empty")
        expected_keys = {"Search Engine Domain", "Search Keyword", "Revenue"}
        actual_keys   = set(results[0].keys())
        self.assertEqual(actual_keys, expected_keys)

    def test_google_ipod_revenue_is_480(self):
        """google.com/ipod = $290 + $190 = $480.00 when case normalized"""
        results, _ = self._run_pipeline()
        row = self._get_row(results, "google.com", "ipod")
        self.assertIsNotNone(row, "Expected google.com/ipod row not found")
        self.assertAlmostEqual(
            float(row["Revenue"]), 480.00, places=2,
            msg="google.com/ipod revenue should be $480.00"
        )

    def test_bing_zune_revenue_is_250(self):
        results, _ = self._run_pipeline()
        row = self._get_row(results, "bing.com", "zune")
        self.assertIsNotNone(row, "Expected bing.com/zune row not found")
        self.assertAlmostEqual(
            float(row["Revenue"]), 250.00, places=2,
            msg="bing.com/zune revenue should be $250.00"
        )

    def test_total_revenue_is_730(self):
        results, _ = self._run_pipeline()
        total = sum(float(r["Revenue"]) for r in results)
        self.assertAlmostEqual(total, 730.00, places=2,
            msg=f"Total revenue should be $730.00, got ${total:.2f}")

    def test_yahoo_cd_player_not_in_output(self):
        """yahoo.com had a session but no purchase — must NOT appear"""
        results, _ = self._run_pipeline()
        yahoo_rows = [r for r in results if r["Search Engine Domain"] == "yahoo.com"]
        self.assertEqual(len(yahoo_rows), 0,
            "yahoo.com should not appear — no purchase event")

    def test_output_sorted_by_revenue_descending(self):
        results, _ = self._run_pipeline()
        revenues = self._get_revenues(results)
        self.assertEqual(revenues, sorted(revenues, reverse=True),
            "Output must be sorted by Revenue descending")

    def test_google_is_first_row(self):
        """google.com/ipod at $480 should be the first row"""
        results, _ = self._run_pipeline()
        first = results[0]
        self.assertEqual(first["Search Engine Domain"], "google.com")
        self.assertEqual(first["Search Keyword"], "ipod")

    # ── DQ report tests ─────────────────────────────────────────────

    def test_dq_overall_passed(self):
        _, dq = self._run_pipeline()
        self.assertTrue(dq.get("overall_passed"),
            f"DQ checks should all pass. Report: {dq}")

    def test_dq_no_duplicates(self):
        _, dq = self._run_pipeline()
        dedup = next(c for c in dq["checks"] if c["check"] == "duplicate_check")
        self.assertEqual(dedup["duplicate_count"], 0)

    def test_dq_schema_passed(self):
        _, dq = self._run_pipeline()
        schema = next(c for c in dq["checks"] if c["check"] == "schema_validation")
        self.assertTrue(schema["passed"])
        self.assertEqual(schema["missing"], [])

    def test_dq_21_rows_parsed(self):
        _, dq = self._run_pipeline()
        self.assertEqual(dq["rows_in"], 21)

    def test_dq_21_rows_out(self):
        _, dq = self._run_pipeline()
        self.assertEqual(dq["rows_out"], 21)


if __name__ == "__main__":
    unittest.main(verbosity=2)

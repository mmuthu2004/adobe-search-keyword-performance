"""
test_end_to_end.py
==================
Integration test — runs the full pipeline against the real sample file
and verifies output matches the documented ground truth exactly.

Ground truth (keyword_normalize_case=true):
  google.com  |  ipod  |  480.00   (Ipod $290 + ipod $190 combined)
  bing.com    |  zune  |  250.00

Run: python -m pytest tests/integration/test_end_to_end.py -v
"""
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

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

        raw = DATA_FILE.read_text(encoding="utf-8", errors="replace")
        pipeline  = PreprocessingPipeline(self.cfg)
        df, dq    = pipeline.run(raw)

        analyzer  = SearchKeywordAnalyzer(self.cfg)
        result_df = analyzer.analyze(df)
        return result_df, dq

    # ── Ground truth tests ──────────────────────────────────────────

    def test_output_has_correct_number_of_rows(self):
        result_df, _ = self._run_pipeline()
        # With normalize_case=True: google/ipod ($480) + bing/zune ($250) = 2 rows
        self.assertEqual(len(result_df), 2,
            f"Expected 2 output rows, got {len(result_df)}:\n{result_df}")

    def test_output_columns_are_correct(self):
        result_df, _ = self._run_pipeline()
        expected_cols = ["Search Engine Domain", "Search Keyword", "Revenue"]
        self.assertEqual(list(result_df.columns), expected_cols)

    def test_google_ipod_revenue_is_480(self):
        """google.com/ipod = $290 (Ipod) + $190 (ipod) = $480.00 when normalized"""
        result_df, _ = self._run_pipeline()
        google_row = result_df[
            (result_df["Search Engine Domain"] == "google.com") &
            (result_df["Search Keyword"] == "ipod")
        ]
        self.assertEqual(len(google_row), 1, "Expected exactly one google.com/ipod row")
        self.assertAlmostEqual(
            float(google_row["Revenue"].iloc[0]), 480.00, places=2,
            msg="google.com/ipod revenue should be $480.00"
        )

    def test_bing_zune_revenue_is_250(self):
        result_df, _ = self._run_pipeline()
        bing_row = result_df[
            (result_df["Search Engine Domain"] == "bing.com") &
            (result_df["Search Keyword"] == "zune")
        ]
        self.assertEqual(len(bing_row), 1, "Expected exactly one bing.com/zune row")
        self.assertAlmostEqual(
            float(bing_row["Revenue"].iloc[0]), 250.00, places=2,
            msg="bing.com/zune revenue should be $250.00"
        )

    def test_total_revenue_is_730(self):
        result_df, _ = self._run_pipeline()
        total = result_df["Revenue"].astype(float).sum()
        self.assertAlmostEqual(total, 730.00, places=2,
            msg=f"Total revenue should be $730.00, got ${total:.2f}")

    def test_yahoo_cd_player_not_in_output(self):
        """yahoo.com/cd player had no purchase — must NOT appear in output"""
        result_df, _ = self._run_pipeline()
        yahoo_rows = result_df[result_df["Search Engine Domain"] == "yahoo.com"]
        self.assertEqual(len(yahoo_rows), 0,
            "yahoo.com should not appear in output — no purchase event")

    def test_output_sorted_by_revenue_descending(self):
        result_df, _ = self._run_pipeline()
        revenues = list(result_df["Revenue"].astype(float))
        self.assertEqual(revenues, sorted(revenues, reverse=True),
            "Output must be sorted by Revenue descending")

    def test_google_is_first_row(self):
        """google.com/ipod at $480 should be the first row"""
        result_df, _ = self._run_pipeline()
        first = result_df.iloc[0]
        self.assertEqual(first["Search Engine Domain"], "google.com")
        self.assertEqual(first["Search Keyword"], "ipod")

    # ── DQ report tests ─────────────────────────────────────────────

    def test_dq_overall_passed(self):
        _, dq = self._run_pipeline()
        self.assertTrue(dq.get("overall_passed"),
            f"DQ checks should all pass on the sample file. Report: {dq}")

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

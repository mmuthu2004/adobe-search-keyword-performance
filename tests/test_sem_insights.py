"""
Unit tests for src/sem_insights/sem_extractor.py.

Tests are self-contained — no filesystem I/O, no AWS, no external libs.
Each test class covers one logical boundary of the extractor.

Expected metrics from the hit_data.tab fixture (traced manually):

    IP             first-touch             purchases
    -----------    ---------------------   ----------------------------
    67.98.123.1    google.com / ipod       $290  (row 22)
    23.8.61.21     bing.com   / zune       $250  (row 16)
    112.33.98.231  yahoo.com  / cd player  —
    44.12.96.2     google.com / ipod       $190  (row 19)

    Keyword         clicks  unique_sessions  converting  revenue  cr     rpc    aov
    --------------- ------  ---------------  ----------  -------  -----  -----  ------
    google/ipod       2           2               2       480.0  100.0  240.0  240.0
    bing/zune         1           1               1       250.0  100.0  250.0  250.0
    yahoo/cd player   1           1               0         0.0    0.0    0.0    0.0
"""

import pytest

from src.sem_insights.sem_extractor import SemExtractor


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def extractor():
    """SemExtractor with a fixed run_id for deterministic assertions."""
    return SemExtractor(run_id="test-run-001")


# Minimal synthetic dataset that mirrors the hit_data.tab sessions above.
ROWS = [
    # --- google.com / ipod session A (67.98.123.1) ---
    {   # landing page — search referrer establishes first-touch
        "ip": "67.98.123.1",
        "referrer": "http://www.google.com/search?hl=en&q=Ipod",
        "event_list": "",
        "product_list": "",
    },
    {   # subsequent page — internal referrer, no SEM signal
        "ip": "67.98.123.1",
        "referrer": "http://www.esshopzilla.com/search/?k=Ipod",
        "event_list": "",
        "product_list": "",
    },
    {   # purchase row — attributed to google/ipod via first-touch
        "ip": "67.98.123.1",
        "referrer": "https://www.esshopzilla.com/checkout/?a=confirm",
        "event_list": "1",
        "product_list": "Electronics;Ipod - Touch - 32GB;1;290;",
    },
    # --- bing.com / zune session (23.8.61.21) ---
    {
        "ip": "23.8.61.21",
        "referrer": "http://www.bing.com/search?q=Zune&form=QBLH",
        "event_list": "",
        "product_list": "",
    },
    {
        "ip": "23.8.61.21",
        "referrer": "https://www.esshopzilla.com/checkout/?a=confirm",
        "event_list": "1",
        "product_list": "Electronics;Zune - 32GB;1;250;",
    },
    # --- yahoo.com / cd player session (112.33.98.231) — no purchase ---
    {
        "ip": "112.33.98.231",
        "referrer": "http://search.yahoo.com/search?p=cd+player&fr=yfp-t-701",
        "event_list": "",
        "product_list": "",
    },
    # --- google.com / ipod session B (44.12.96.2) ---
    {
        "ip": "44.12.96.2",
        "referrer": "http://www.google.com/search?hl=en&q=ipod",
        "event_list": "",
        "product_list": "",
    },
    {
        "ip": "44.12.96.2",
        "referrer": "https://www.esshopzilla.com/checkout/?a=confirm",
        "event_list": "1",
        "product_list": "Electronics;Ipod - Nano - 8GB;1;190;",
    },
]


# ---------------------------------------------------------------------------
# TestReferrerParsing
# ---------------------------------------------------------------------------

class TestReferrerParsing:
    """_parse_referrer must extract (domain, keyword) or return None."""

    def test_google_q_param(self, extractor):
        assert extractor._parse_referrer(
            "https://www.google.com/search?q=ipod"
        ) == ("google.com", "ipod")

    def test_bing_q_param(self, extractor):
        assert extractor._parse_referrer(
            "https://www.bing.com/search?q=zune&form=QBLH"
        ) == ("bing.com", "zune")

    def test_yahoo_p_param(self, extractor):
        assert extractor._parse_referrer(
            "http://search.yahoo.com/search?p=cd+player"
        ) == ("yahoo.com", "cd player")

    def test_keyword_case_normalised(self, extractor):
        # "Ipod" in URL → "ipod" in output
        assert extractor._parse_referrer(
            "http://www.google.com/search?q=Ipod"
        ) == ("google.com", "ipod")

    def test_non_search_referrer_returns_none(self, extractor):
        assert extractor._parse_referrer("http://www.esshopzilla.com/cart/") is None

    def test_search_engine_without_keyword_returns_none(self, extractor):
        assert extractor._parse_referrer("https://www.google.com/") is None

    def test_empty_referrer_returns_none(self, extractor):
        assert extractor._parse_referrer("") is None

    def test_unknown_domain_returns_none(self, extractor):
        assert extractor._parse_referrer(
            "http://duckduckgo.com/search?q=ipod"
        ) is None

    def test_malformed_url_returns_none(self, extractor):
        assert extractor._parse_referrer("not a url!!!") is None


# ---------------------------------------------------------------------------
# TestDomainNormalisation
# ---------------------------------------------------------------------------

class TestDomainNormalisation:
    """_normalize_domain must strip subdomains down to registrable domain."""

    def test_www_prefix_stripped(self, extractor):
        assert extractor._normalize_domain("www.google.com") == "google.com"

    def test_deep_subdomain_stripped(self, extractor):
        assert extractor._normalize_domain("search.yahoo.com") == "yahoo.com"

    def test_plain_domain_unchanged(self, extractor):
        assert extractor._normalize_domain("bing.com") == "bing.com"

    def test_normalised_to_lowercase(self, extractor):
        assert extractor._normalize_domain("WWW.GOOGLE.COM") == "google.com"


# ---------------------------------------------------------------------------
# TestRevenueExtraction
# ---------------------------------------------------------------------------

class TestRevenueExtraction:
    """_extract_revenue must parse the Adobe product_list revenue field."""

    def test_single_product(self, extractor):
        # category;name;qty;revenue;events → index 3 = 250
        assert extractor._extract_revenue("Electronics;Ipod;1;250;") == 250.0

    def test_multiple_products_summed(self, extractor):
        assert extractor._extract_revenue(
            "Electronics;Ipod;1;100;,Electronics;Case;1;50;"
        ) == 150.0

    def test_empty_string_returns_zero(self, extractor):
        assert extractor._extract_revenue("") == 0.0

    def test_missing_revenue_field_skipped(self, extractor):
        # Only 3 attrs — revenue index 3 does not exist; should not raise
        assert extractor._extract_revenue("Electronics;Zune - 328GB;1;;") == 0.0

    def test_non_numeric_revenue_skipped(self, extractor):
        assert extractor._extract_revenue("Electronics;Widget;1;N/A;") == 0.0

    def test_multiple_products_one_with_no_revenue(self, extractor):
        assert extractor._extract_revenue(
            "Electronics;Ipod;1;200;,Electronics;Case;1;;"
        ) == 200.0


# ---------------------------------------------------------------------------
# TestPurchaseEventDetection
# ---------------------------------------------------------------------------

class TestPurchaseEventDetection:
    """_has_purchase_event must detect event_id="1" in the comma-delimited list."""

    def test_sole_event(self, extractor):
        assert extractor._has_purchase_event("1") is True

    def test_mixed_events_contains_purchase(self, extractor):
        assert extractor._has_purchase_event("11,12,1") is True

    def test_no_purchase_event(self, extractor):
        assert extractor._has_purchase_event("11,12") is False

    def test_empty_string(self, extractor):
        assert extractor._has_purchase_event("") is False

    def test_prefix_match_rejected(self, extractor):
        # "11" contains "1" as a substring but must NOT match
        assert extractor._has_purchase_event("11") is False


# ---------------------------------------------------------------------------
# TestExtract — integration-level assertions on ROWS fixture
# ---------------------------------------------------------------------------

class TestExtract:
    """Full extract() pipeline against the synthetic ROWS fixture."""

    def test_returns_list_of_dicts(self, extractor):
        results = extractor.extract(ROWS)
        assert isinstance(results, list)
        assert all(isinstance(r, dict) for r in results)

    def test_schema_columns_exact(self, extractor):
        results = extractor.extract(ROWS)
        for row in results:
            assert set(row.keys()) == set(SemExtractor.SCHEMA_COLS), (
                f"Unexpected columns: {set(row.keys()) ^ set(SemExtractor.SCHEMA_COLS)}"
            )

    def test_three_keyword_pairs_produced(self, extractor):
        results = extractor.extract(ROWS)
        assert len(results) == 3

    def test_sorted_by_revenue_descending(self, extractor):
        results = extractor.extract(ROWS)
        revenues = [r["total_revenue"] for r in results]
        assert revenues == sorted(revenues, reverse=True)

    def test_google_ipod_clicks(self, extractor):
        results = extractor.extract(ROWS)
        row = next(r for r in results if r["search_engine_domain"] == "google.com")
        # Two rows in ROWS have a google/ipod referrer (one per session).
        assert row["total_clicks"] == 2

    def test_google_ipod_unique_sessions(self, extractor):
        results = extractor.extract(ROWS)
        row = next(r for r in results if r["search_engine_domain"] == "google.com")
        assert row["unique_sessions"] == 2

    def test_google_ipod_converting_sessions(self, extractor):
        results = extractor.extract(ROWS)
        row = next(r for r in results if r["search_engine_domain"] == "google.com")
        assert row["converting_sessions"] == 2

    def test_google_ipod_revenue(self, extractor):
        results = extractor.extract(ROWS)
        row = next(r for r in results if r["search_engine_domain"] == "google.com")
        assert row["total_revenue"] == 480.0   # 290 + 190

    def test_google_ipod_conversion_rate(self, extractor):
        results = extractor.extract(ROWS)
        row = next(r for r in results if r["search_engine_domain"] == "google.com")
        assert row["conversion_rate"] == 100.0  # 2/2 × 100

    def test_google_ipod_revenue_per_click(self, extractor):
        results = extractor.extract(ROWS)
        row = next(r for r in results if r["search_engine_domain"] == "google.com")
        assert row["revenue_per_click"] == 240.0  # 480 / 2 clicks

    def test_google_ipod_avg_order_value(self, extractor):
        results = extractor.extract(ROWS)
        row = next(r for r in results if r["search_engine_domain"] == "google.com")
        assert row["avg_order_value"] == 240.0   # 480 / 2 conversions

    def test_bing_zune_revenue(self, extractor):
        results = extractor.extract(ROWS)
        row = next(r for r in results if r["search_engine_domain"] == "bing.com")
        assert row["total_revenue"] == 250.0

    def test_zero_revenue_keyword_included(self, extractor):
        """yahoo/cd player drove a session but no purchase — must appear in output."""
        results = extractor.extract(ROWS)
        yahoo = next(
            (r for r in results if r["search_engine_domain"] == "yahoo.com"), None
        )
        assert yahoo is not None, "yahoo.com keyword missing from output"
        assert yahoo["search_keyword"] == "cd player"
        assert yahoo["unique_sessions"] == 1
        assert yahoo["converting_sessions"] == 0
        assert yahoo["total_revenue"] == 0.0

    def test_zero_division_guarded_conversion_rate(self, extractor):
        results = extractor.extract(ROWS)
        yahoo = next(r for r in results if r["search_engine_domain"] == "yahoo.com")
        assert yahoo["conversion_rate"] == 0.0

    def test_zero_division_guarded_revenue_per_click(self, extractor):
        results = extractor.extract(ROWS)
        yahoo = next(r for r in results if r["search_engine_domain"] == "yahoo.com")
        assert yahoo["revenue_per_click"] == 0.0

    def test_zero_division_guarded_avg_order_value(self, extractor):
        results = extractor.extract(ROWS)
        yahoo = next(r for r in results if r["search_engine_domain"] == "yahoo.com")
        assert yahoo["avg_order_value"] == 0.0

    def test_run_id_propagated_to_all_rows(self, extractor):
        results = extractor.extract(ROWS)
        assert all(r["run_id"] == "test-run-001" for r in results)

    def test_run_date_populated(self, extractor):
        results = extractor.extract(ROWS)
        assert all(r["run_date"].endswith("UTC") for r in results)

    def test_empty_input_returns_empty_list(self, extractor):
        assert extractor.extract([]) == []

    def test_rows_with_no_search_referrers_returns_empty(self, extractor):
        no_sem = [
            {"ip": "1.2.3.4", "referrer": "http://example.com/", "event_list": "", "product_list": ""},
        ]
        assert extractor.extract(no_sem) == []

    def test_first_touch_not_overwritten(self, extractor):
        """
        If an IP visits via google/ipod and then via bing/zune, revenue must
        be attributed to google/ipod (first-touch), not bing/zune.
        """
        rows = [
            {"ip": "9.9.9.9", "referrer": "http://www.google.com/search?q=ipod",
             "event_list": "", "product_list": ""},
            {"ip": "9.9.9.9", "referrer": "http://www.bing.com/search?q=zune",
             "event_list": "", "product_list": ""},
            {"ip": "9.9.9.9", "referrer": "http://www.esshopzilla.com/",
             "event_list": "1", "product_list": "Electronics;Widget;1;100;"},
        ]
        results = extractor.extract(rows)
        google_row = next(
            (r for r in results if r["search_engine_domain"] == "google.com"), None
        )
        bing_row = next(
            (r for r in results if r["search_engine_domain"] == "bing.com"), None
        )
        # Revenue must land on google/ipod, not bing/zune
        assert google_row is not None
        assert google_row["total_revenue"] == 100.0
        # bing/zune was seen (click counted) but gets zero revenue
        assert bing_row is not None
        assert bing_row["total_revenue"] == 0.0
        assert bing_row["converting_sessions"] == 0

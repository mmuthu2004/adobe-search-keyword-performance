"""
search_keyword_analyzer.py
==========================
Core business logic — Pandas processing path (files < 1GB).

Algorithm (Session-Based First-Touch Attribution):
  Pass 1 — Build session map:
    For each row where referrer is a known search engine:
      session_map[ip] = {domain, keyword}   (first occurrence per IP wins)

  Pass 2 — Attribute revenue:
    For each row where purchase event ('1') is in event_list AND revenue > 0:
      Look up session_map[ip] → credit revenue to that domain+keyword

  Aggregate → group by (domain, keyword), sum revenue
  Sort by Revenue DESC
  Return DataFrame with 3 columns: Search Engine Domain, Search Keyword, Revenue

All rules are config-driven. Zero hardcoding.
"""

import logging
import re
from urllib.parse import urlparse, parse_qs
from typing import Dict, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class SearchKeywordAnalyzer:

    def __init__(self, cfg):
        # Business rules — all from config
        self.search_domains   = cfg.require("business_rules.search_engine_domains")
        self.keyword_params   = cfg.require("business_rules.keyword_query_params")
        self.purchase_event   = cfg.require("business_rules.purchase_event_id")
        self.session_key_col  = cfg.get("business_rules.session_key_column",      default="ip")
        self.revenue_idx      = cfg.get("business_rules.revenue_column_index",     default=3)
        self.prod_delim       = cfg.get("business_rules.product_delimiter",        default=",")
        self.prod_attr_delim  = cfg.get("business_rules.product_attr_delimiter",   default=";")
        self.event_delim      = cfg.get("business_rules.event_delimiter",          default=",")
        self.normalize_case   = cfg.get("business_rules.keyword_normalize_case",   default=True)
        self.min_revenue      = cfg.get("business_rules.min_revenue_threshold",    default=0.0)

        # Output settings
        self.out_cols         = cfg.get("output.columns",
                                        default=["Search Engine Domain","Search Keyword","Revenue"])
        self.sort_col         = cfg.get("output.sort_column",      default="Revenue")
        self.sort_asc         = cfg.get("output.sort_ascending",   default=False)
        self.revenue_dp       = cfg.get("output.revenue_decimal_places", default=2)

        # Pre-compile search domain set for O(1) lookup
        self._domain_set = set(self.search_domains)

    # ---------------------------------------------------------------- #
    # Public entry point
    # ---------------------------------------------------------------- #

    def analyze(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Run session-based attribution on a preprocessed DataFrame.

        Args:
            df: Clean DataFrame from PreprocessingPipeline.

        Returns:
            Aggregated results DataFrame with columns:
            [Search Engine Domain, Search Keyword, Revenue]
            Sorted by Revenue descending.
        """
        logger.info("=== Search Keyword Analyzer Start. Input rows: %d ===", len(df))

        # Pass 1: build session map
        session_map = self._build_session_map(df)
        logger.info("Session map built. %d unique search sessions found.", len(session_map))

        # Pass 2: attribute revenue
        attributions = self._attribute_revenue(df, session_map)
        logger.info("Revenue attributed to %d purchase event(s).", len(attributions))

        if not attributions:
            logger.warning("No qualified revenue attributions found. Output will be empty.")
            return self._empty_result()

        # Aggregate
        result_df = self._aggregate(attributions)

        logger.info(
            "=== Analyzer Complete. Output rows: %d | Total revenue: $%.2f ===",
            len(result_df),
            result_df[self.out_cols[2]].sum()
        )

        return result_df

    # ---------------------------------------------------------------- #
    # Pass 1: Build session map
    # ---------------------------------------------------------------- #

    def _build_session_map(self, df: pd.DataFrame) -> Dict[str, dict]:
        """
        Scan rows in hit_time order and record the FIRST search engine
        referrer seen per IP address (first-touch attribution).

        Returns:
            {ip_address: {"domain": str, "keyword": str}}
        """
        session_map: Dict[str, dict] = {}

        # Sort by hit_time_gmt ascending to ensure first-touch is correct
        if "hit_time_gmt" in df.columns:
            df = df.sort_values("hit_time_gmt", ascending=True)

        for _, row in df.iterrows():
            ip       = str(row.get(self.session_key_col, "")).strip()
            referrer = str(row.get("referrer", "")).strip()

            # Skip if IP already has a session entry (first-touch wins)
            if ip in session_map:
                continue

            # Parse referrer — only care about search engine domains
            domain, keyword = self._extract_search_info(referrer)
            if domain is None:
                continue

            session_map[ip] = {"domain": domain, "keyword": keyword}
            logger.debug(
                "Session map: IP %s...  →  domain=%s keyword=%s",
                ip[:8], domain, keyword
            )

        return session_map

    # ---------------------------------------------------------------- #
    # Pass 2: Attribute revenue to sessions
    # ---------------------------------------------------------------- #

    def _attribute_revenue(self, df: pd.DataFrame, session_map: Dict[str, dict]) -> list:
        """
        Scan rows for purchase events. For each qualifying purchase,
        look up the session map to find the originating search engine entry.

        Returns:
            List of dicts: [{domain, keyword, revenue}, ...]
        """
        attributions = []

        for _, row in df.iterrows():
            event_list   = str(row.get("event_list",   "")).strip()
            product_list = str(row.get("product_list", "")).strip()
            ip           = str(row.get(self.session_key_col, "")).strip()

            # Condition 1: purchase event must be present
            if not self._has_purchase_event(event_list):
                continue

            # Condition 2: revenue must be > threshold
            revenue = self._extract_revenue(product_list)
            if revenue <= self.min_revenue:
                continue

            # Condition 3: IP must have a search session
            session = session_map.get(ip)
            if session is None:
                logger.debug(
                    "Purchase found for IP %s... but no search engine session — skipping.",
                    ip[:8]
                )
                continue

            attributions.append({
                "domain":  session["domain"],
                "keyword": session["keyword"],
                "revenue": revenue,
            })
            logger.info(
                "Attribution: %s / %s  →  $%.2f",
                session["domain"], session["keyword"], revenue
            )

        return attributions

    # ---------------------------------------------------------------- #
    # Aggregate and sort
    # ---------------------------------------------------------------- #

    def _aggregate(self, attributions: list) -> pd.DataFrame:
        """Group by domain+keyword, sum revenue, sort descending."""
        agg = {}
        for a in attributions:
            key = (a["domain"], a["keyword"])
            agg[key] = agg.get(key, 0.0) + a["revenue"]

        rows = [
            {
                self.out_cols[0]: domain,
                self.out_cols[1]: keyword,
                self.out_cols[2]: round(revenue, self.revenue_dp),
            }
            for (domain, keyword), revenue in agg.items()
        ]

        result_df = pd.DataFrame(rows, columns=self.out_cols)
        result_df = result_df.sort_values(
            by=self.sort_col,
            ascending=self.sort_asc
        ).reset_index(drop=True)

        return result_df

    # ---------------------------------------------------------------- #
    # Helpers
    # ---------------------------------------------------------------- #

    def _extract_search_info(self, referrer: str) -> tuple:
        """
        Parse a referrer URL and extract (normalized_domain, keyword).
        Returns (None, None) if referrer is not a tracked search engine.
        """
        if not referrer or referrer in ("", "nan", "None"):
            return None, None

        try:
            parsed = urlparse(referrer)
            hostname = parsed.hostname or ""

            # Normalize: strip www. and subdomain variations
            # e.g.  search.yahoo.com → yahoo.com
            normalized = self._normalize_domain(hostname)

            if normalized not in self._domain_set:
                return None, None

            # Extract keyword from query string
            query_params = parse_qs(parsed.query)
            keyword = None
            for param in self.keyword_params:
                if param in query_params:
                    keyword = query_params[param][0]
                    break

            if keyword is None:
                logger.debug("Search engine referrer %s has no recognizable keyword param.", referrer)
                return None, None

            if self.normalize_case:
                keyword = keyword.lower().strip()
            else:
                keyword = keyword.strip()

            return normalized, keyword

        except Exception as exc:
            logger.debug("Could not parse referrer '%s': %s", referrer[:80], exc)
            return None, None

    def _normalize_domain(self, hostname: str) -> str:
        """
        Normalize domain to base form for matching against search_engine_domains.
        e.g.  search.yahoo.com → yahoo.com
              www.google.com   → google.com
        """
        if not hostname:
            return ""
        parts = hostname.lower().split(".")
        # Return last two parts: yahoo.com, google.com, bing.com
        if len(parts) >= 2:
            return ".".join(parts[-2:])
        return hostname.lower()

    def _has_purchase_event(self, event_list: str) -> bool:
        """Return True if the purchase event ID is in the event_list string."""
        if not event_list or event_list in ("", "nan", "None"):
            return False
        events = [e.strip() for e in event_list.split(self.event_delim)]
        return self.purchase_event in events

    def _extract_revenue(self, product_list: str) -> float:
        """
        Parse product_list and sum revenue across all products.

        product_list format (comma-separated products, semicolons within):
          category;name;qty;revenue;events;evar, category;name;qty;revenue;...

        Revenue is at self.revenue_idx (0-based) within each semicolon-delimited product.
        """
        if not product_list or product_list in ("", "nan", "None"):
            return 0.0

        total_revenue = 0.0
        products = product_list.split(self.prod_delim)

        for product in products:
            attrs = product.split(self.prod_attr_delim)
            if len(attrs) > self.revenue_idx:
                try:
                    rev = float(attrs[self.revenue_idx].strip())
                    total_revenue += rev
                except (ValueError, TypeError):
                    pass

        return total_revenue

    def _empty_result(self) -> pd.DataFrame:
        """Return an empty DataFrame with the correct output columns."""
        return pd.DataFrame(columns=self.out_cols)

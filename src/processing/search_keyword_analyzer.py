"""
search_keyword_analyzer.py
==========================
Core business logic — Pure Python processing path.
Zero external dependencies. Standard library only.

Algorithm (Session-Based First-Touch Attribution):
  Pass 1 — Build session map:
    For each row where referrer is a known search engine:
      session_map[ip] = {domain, keyword}  (first occurrence per IP wins)

  Pass 2 — Attribute revenue:
    For each row where purchase event in event_list AND revenue > 0:
      Look up session_map[ip] → credit revenue to that domain+keyword

  Aggregate → group by (domain, keyword), sum revenue
  Sort by Revenue DESC
"""
import logging
from collections import defaultdict
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)


class SearchKeywordAnalyzer:
    """
    Core business logic engine — pure Python, zero external dependencies.

    Implements session-based first-touch attribution:
      Pass 1 — Build session map: for each IP, record the FIRST search engine
               referrer seen (domain + keyword). O(n) scan in chronological order.
      Pass 2 — Attribute revenue: for each purchase row, look up the IP's session.
               Only purchases from IPs with a prior search engine visit are counted.
      Aggregate — group by (domain, keyword), sum revenue, sort descending.

    All business rules (domains, keyword params, revenue parsing, attribution model)
    are config-driven — see config.yaml business_rules section.
    """

    def __init__(self, cfg):
        self.search_domains  = cfg.require("business_rules.search_engine_domains")
        self.keyword_params  = cfg.require("business_rules.keyword_query_params")
        self.purchase_event  = cfg.require("business_rules.purchase_event_id")
        self.session_key_col = cfg.get("business_rules.session_key_column",     default="ip")
        self.revenue_idx     = cfg.get("business_rules.revenue_column_index",   default=3)
        self.prod_delim      = cfg.get("business_rules.product_delimiter",      default=",")
        self.prod_attr_delim = cfg.get("business_rules.product_attr_delimiter", default=";")
        self.event_delim     = cfg.get("business_rules.event_delimiter",        default=",")
        self.normalize_case  = cfg.get("business_rules.keyword_normalize_case", default=True)
        self.min_revenue     = cfg.get("business_rules.min_revenue_threshold",  default=0.0)
        self.out_cols        = cfg.get("output.columns",
                                       default=["Search Engine Domain","Search Keyword","Revenue"])
        self.sort_asc        = cfg.get("output.sort_ascending",        default=False)
        self.revenue_dp      = cfg.get("output.revenue_decimal_places", default=2)

        # O(1) domain lookup
        self._domain_set = set(self.search_domains)

    # ---------------------------------------------------------------- #
    # Public entry point
    # ---------------------------------------------------------------- #

    def analyze(self, rows: list) -> list:
        """
        Run session-based attribution on preprocessed rows.

        Args:
            rows: List of dicts from PreprocessingPipeline.

        Returns:
            List of result dicts:
            [{"Search Engine Domain": ..., "Search Keyword": ..., "Revenue": ...}, ...]
            Sorted by Revenue descending.
        """
        logger.info("=== Search Keyword Analyzer Start. Input rows: %d ===", len(rows))

        # Sort by hit_time_gmt ascending for correct first-touch attribution
        rows_sorted = sorted(rows, key=lambda r: r.get("hit_time_gmt", ""))

        # Pass 1: build session map
        session_map = self._build_session_map(rows_sorted)
        logger.info("Session map built. %d unique search sessions found.", len(session_map))

        # Pass 2: attribute revenue
        attributions = self._attribute_revenue(rows_sorted, session_map)
        logger.info("Revenue attributed to %d purchase event(s).", len(attributions))

        if not attributions:
            logger.warning("No qualified revenue attributions found. Output will be empty.")
            return []

        # Aggregate and sort
        results = self._aggregate(attributions)

        logger.info(
            "=== Analyzer Complete. Output rows: %d | Total revenue: $%.2f ===",
            len(results),
            sum(r[self.out_cols[2]] for r in results)
        )
        return results

    # ---------------------------------------------------------------- #
    # Pass 1: Build session map
    # ---------------------------------------------------------------- #

    def _build_session_map(self, rows: list) -> dict:
        """
        Record the FIRST search engine referrer per IP (first-touch attribution).
        Returns: {ip: {"domain": str, "keyword": str}}
        """
        session_map = {}

        for row in rows:
            ip       = row.get(self.session_key_col, "").strip()
            referrer = row.get("referrer", "").strip()

            if not ip or ip in session_map:
                continue

            domain, keyword = self._extract_search_info(referrer)
            if domain is None:
                continue

            session_map[ip] = {"domain": domain, "keyword": keyword}
            logger.debug("Session: IP %s... → %s / %s", ip[:8], domain, keyword)

        return session_map

    # ---------------------------------------------------------------- #
    # Pass 2: Attribute revenue
    # ---------------------------------------------------------------- #

    def _attribute_revenue(self, rows: list, session_map: dict) -> list:
        """
        Find purchase rows and attribute revenue to search sessions.
        Returns: [{"domain": str, "keyword": str, "revenue": float}, ...]
        """
        attributions = []

        for row in rows:
            event_list   = row.get("event_list",   "").strip()
            product_list = row.get("product_list", "").strip()
            ip           = row.get(self.session_key_col, "").strip()

            if not self._has_purchase_event(event_list):
                continue

            revenue = self._extract_revenue(product_list)
            if revenue <= self.min_revenue:
                continue

            session = session_map.get(ip)
            if session is None:
                logger.debug("Purchase for IP %s... has no search session — skipping.", ip[:8])
                continue

            attributions.append({
                "domain":  session["domain"],
                "keyword": session["keyword"],
                "revenue": revenue,
            })
            # DEBUG — not INFO: this fires once per purchase row and would flood
            # CloudWatch logs for files with thousands of purchase events.
            logger.debug(
                "Attribution: %s / %s  →  $%.2f",
                session["domain"], session["keyword"], revenue
            )

        return attributions

    # ---------------------------------------------------------------- #
    # Aggregate and sort
    # ---------------------------------------------------------------- #

    def _aggregate(self, attributions: list) -> list:
        """Group by domain+keyword, sum revenue, sort descending."""
        agg = defaultdict(float)
        for a in attributions:
            agg[(a["domain"], a["keyword"])] += a["revenue"]

        results = [
            {
                self.out_cols[0]: domain,
                self.out_cols[1]: keyword,
                self.out_cols[2]: round(revenue, self.revenue_dp),
            }
            for (domain, keyword), revenue in agg.items()
        ]

        results.sort(key=lambda r: r[self.out_cols[2]], reverse=not self.sort_asc)
        return results

    # ---------------------------------------------------------------- #
    # Helpers
    # ---------------------------------------------------------------- #

    def _extract_search_info(self, referrer: str) -> tuple:
        """Parse referrer URL → (normalized_domain, keyword) or (None, None)."""
        if not referrer or referrer in ("", "nan", "None"):
            return None, None
        try:
            parsed   = urlparse(referrer)
            hostname = parsed.hostname or ""
            domain   = self._normalize_domain(hostname)

            if domain not in self._domain_set:
                return None, None

            query_params = parse_qs(parsed.query)
            keyword = None
            for param in self.keyword_params:
                if param in query_params:
                    keyword = query_params[param][0]
                    break

            if keyword is None:
                return None, None

            keyword = keyword.lower().strip() if self.normalize_case else keyword.strip()
            return domain, keyword

        except Exception as exc:
            logger.debug("Could not parse referrer '%s': %s", referrer[:80], exc)
            return None, None

    def _normalize_domain(self, hostname: str) -> str:
        """search.yahoo.com → yahoo.com, www.google.com → google.com"""
        if not hostname:
            return ""
        parts = hostname.lower().split(".")
        return ".".join(parts[-2:]) if len(parts) >= 2 else hostname.lower()

    def _has_purchase_event(self, event_list: str) -> bool:
        """Return True if purchase event ID is in the event_list string."""
        if not event_list or event_list in ("", "nan", "None"):
            return False
        return self.purchase_event in [e.strip() for e in event_list.split(self.event_delim)]

    def _extract_revenue(self, product_list: str) -> float:
        """
        Parse product_list and sum revenue across all products.
        Format: category;name;qty;revenue;events;evar, category;...
        Revenue is at index self.revenue_idx within each semicolon product.
        """
        if not product_list or product_list in ("", "nan", "None"):
            return 0.0

        total = 0.0
        for product in product_list.split(self.prod_delim):
            attrs = product.split(self.prod_attr_delim)
            if len(attrs) > self.revenue_idx:
                try:
                    total += float(attrs[self.revenue_idx].strip())
                except (ValueError, TypeError):
                    pass
        return total

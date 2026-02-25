"""
SEM Insights Extractor — standalone proof of concept.

Computes keyword-level SEM performance metrics from raw Adobe Analytics
hit-level data using the same first-touch attribution model as the main
pipeline, but extended with click, session, conversion, and efficiency
metrics useful for paid/organic search analysis.

Zero external dependencies — stdlib only (csv, io, uuid, datetime,
urllib.parse, collections, logging).

Output schema (one row per search_engine_domain + search_keyword pair):

    search_engine_domain  — google.com, bing.com, etc.
    search_keyword        — normalized keyword string
    total_clicks          — rows where this keyword appears in the referrer
    unique_sessions       — distinct IPs whose first-touch is this keyword
    converting_sessions   — subset of unique_sessions that triggered a purchase
    total_revenue         — sum of revenue attributed to this keyword
    conversion_rate       — converting_sessions / unique_sessions × 100
    revenue_per_click     — total_revenue / total_clicks
    avg_order_value       — total_revenue / converting_sessions
    run_date              — UTC timestamp of this extraction run
    run_id                — opaque ID linking this report to a pipeline run

All division operations are guarded against zero — keywords with no purchases
produce 0.0 for conversion_rate, revenue_per_click, and avg_order_value and
are included in the output (they are valid SEM signal: keyword drove traffic
but did not convert).
"""

import io
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)


class SemExtractor:
    """
    Extract SEM performance metrics from a list of hit-level row dicts.

    Usage:
        extractor = SemExtractor()
        results = extractor.extract(rows)   # rows from csv.DictReader

    The extractor is intentionally stateless across calls to extract() —
    each call produces an independent report. Pass a stable run_id to link
    the report back to the triggering pipeline run.
    """

    SCHEMA_COLS = [
        "search_engine_domain",
        "search_keyword",
        "total_clicks",
        "unique_sessions",
        "converting_sessions",
        "total_revenue",
        "conversion_rate",
        "revenue_per_click",
        "avg_order_value",
        "run_date",
        "run_id",
    ]

    # Default search engine domains recognised as SEM sources.
    # Matches business_rules.search_engine_domains in config.yaml.
    _DEFAULT_DOMAINS = frozenset([
        "google.com",
        "bing.com",
        "yahoo.com",
        "msn.com",
        "ask.com",
        "aol.com",
    ])

    # Default query parameters to probe for the keyword.
    # Matches business_rules.keyword_query_params in config.yaml.
    _DEFAULT_KW_PARAMS = ("q", "p", "query", "text", "s", "qs")

    def __init__(
        self,
        search_domains=None,
        keyword_params=None,
        purchase_event_id="1",
        revenue_col_idx=3,
        prod_delim=",",
        prod_attr_delim=";",
        event_delim=",",
        normalize_case=True,
        run_id=None,
    ):
        """
        Parameters
        ----------
        search_domains : iterable[str] | None
            Recognised SEM domains (e.g. {"google.com", "bing.com"}).
            Defaults to _DEFAULT_DOMAINS when None.
        keyword_params : iterable[str] | None
            URL query-string params to probe for the keyword.
            Defaults to _DEFAULT_KW_PARAMS when None.
        purchase_event_id : str
            Event ID that signals a completed purchase.  Must match
            business_rules.purchase_event_id in config.yaml (default "1").
        revenue_col_idx : int
            0-based index of the revenue field inside each semicolon-delimited
            product attribute block (default 3, matches config.yaml).
        prod_delim : str
            Delimiter between multiple products in product_list (default ",").
        prod_attr_delim : str
            Delimiter between attribute fields within one product (default ";").
        event_delim : str
            Delimiter between event IDs in event_list (default ",").
        normalize_case : bool
            Lowercase keywords before grouping (default True).
        run_id : str | None
            Caller-supplied audit ID.  A UUID4 is generated when None.
        """
        self.search_domains = (
            frozenset(search_domains) if search_domains is not None
            else self._DEFAULT_DOMAINS
        )
        self.keyword_params = (
            tuple(keyword_params) if keyword_params is not None
            else self._DEFAULT_KW_PARAMS
        )
        self.purchase_event_id = str(purchase_event_id)
        self.revenue_col_idx = revenue_col_idx
        self.prod_delim = prod_delim
        self.prod_attr_delim = prod_attr_delim
        self.event_delim = event_delim
        self.normalize_case = normalize_case
        self.run_id = run_id or str(uuid.uuid4())
        self.run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(self, rows: list) -> list:
        """
        Compute SEM metrics from hit-level rows.

        Parameters
        ----------
        rows : list[dict]
            Rows produced by csv.DictReader over a hit-level .tab file.
            Must contain at minimum the keys: ip, referrer, event_list,
            product_list.

        Returns
        -------
        list[dict]
            One dict per (search_engine_domain, search_keyword) pair,
            matching SCHEMA_COLS, sorted by total_revenue descending.
            Includes pairs with zero revenue.
        """
        if not rows:
            return []

        clicks, first_touch, unique_ips = self._pass1_sessions(rows)
        converting_ips, revenue = self._pass2_revenue(rows, first_touch)
        return self._aggregate(clicks, unique_ips, converting_ips, revenue)

    # ------------------------------------------------------------------
    # Pass 1: session map + click counting
    # ------------------------------------------------------------------

    def _pass1_sessions(self, rows: list):
        """
        Single pass over all rows to build three data structures:

        clicks[(domain, kw)]      — total rows where this referrer was seen
        first_touch[ip]           — (domain, kw) of the first search engine
                                    referrer seen for this IP
        unique_ips[(domain, kw)]  — set of IPs whose first-touch is this pair

        first_touch implements first-touch attribution: only the first search
        engine referrer encountered for a given IP is recorded; later rows for
        the same IP with a different (or the same) search referrer do not
        overwrite the original attribution.
        """
        clicks = defaultdict(int)
        first_touch = {}
        unique_ips = defaultdict(set)

        for row in rows:
            referrer = (row.get("referrer") or "").strip()
            ip = (row.get("ip") or "").strip()
            if not referrer or not ip:
                continue

            parsed = self._parse_referrer(referrer)
            if parsed is None:
                continue

            domain, keyword = parsed
            key = (domain, keyword)

            # Count every row that carries this search referrer.
            clicks[key] += 1

            # Record first-touch only once per IP.
            if ip not in first_touch:
                first_touch[ip] = key
                unique_ips[key].add(ip)

        logger.debug(
            "Pass 1 complete — %d unique (domain, keyword) pairs, %d IPs",
            len(clicks), len(first_touch),
        )
        return clicks, first_touch, unique_ips

    # ------------------------------------------------------------------
    # Pass 2: revenue attribution
    # ------------------------------------------------------------------

    def _pass2_revenue(self, rows: list, first_touch: dict):
        """
        Second pass: attribute purchase revenue to each keyword via
        first-touch session map built in Pass 1.

        Returns
        -------
        converting_ips : defaultdict(set)
            Set of converting IPs per (domain, kw) key.
        revenue : defaultdict(float)
            Summed revenue per (domain, kw) key.
        """
        converting_ips = defaultdict(set)
        revenue = defaultdict(float)

        for row in rows:
            ip = (row.get("ip") or "").strip()
            event_list = (row.get("event_list") or "").strip()
            product_list = (row.get("product_list") or "").strip()

            if not ip or ip not in first_touch:
                continue
            if not self._has_purchase_event(event_list):
                continue

            key = first_touch[ip]
            converting_ips[key].add(ip)
            row_revenue = self._extract_revenue(product_list)
            revenue[key] += row_revenue
            logger.debug(
                "Pass 2: attributed $%.2f to %s / %s (ip=%s)",
                row_revenue, key[0], key[1], ip,
            )

        return converting_ips, revenue

    # ------------------------------------------------------------------
    # Aggregation
    # ------------------------------------------------------------------

    def _aggregate(self, clicks, unique_ips, converting_ips, revenue) -> list:
        """
        Combine Pass 1 and Pass 2 structures into the final output rows.
        All division operations are guarded against zero.
        Sorted by total_revenue descending.
        """
        results = []
        for key in clicks:
            domain, keyword = key
            tc = clicks[key]                           # total_clicks
            us = len(unique_ips[key])                  # unique_sessions
            cs = len(converting_ips[key])              # converting_sessions
            rev = round(revenue[key], 2)               # total_revenue

            conversion_rate = round(cs / us * 100, 2) if us > 0 else 0.0
            revenue_per_click = round(rev / tc, 2) if tc > 0 else 0.0
            avg_order_value = round(rev / cs, 2) if cs > 0 else 0.0

            results.append({
                "search_engine_domain": domain,
                "search_keyword":       keyword,
                "total_clicks":         tc,
                "unique_sessions":      us,
                "converting_sessions":  cs,
                "total_revenue":        rev,
                "conversion_rate":      conversion_rate,
                "revenue_per_click":    revenue_per_click,
                "avg_order_value":      avg_order_value,
                "run_date":             self.run_date,
                "run_id":               self.run_id,
            })

        results.sort(key=lambda r: r["total_revenue"], reverse=True)
        logger.debug("Aggregation complete — %d output rows", len(results))
        return results

    # ------------------------------------------------------------------
    # Referrer helpers
    # ------------------------------------------------------------------

    def _parse_referrer(self, referrer: str):
        """
        Extract (normalized_domain, keyword) from a referrer URL.

        Returns None if the referrer is not from a recognised search engine
        or if no keyword query parameter is found.
        """
        try:
            parsed = urlparse(referrer)
            hostname = parsed.hostname or ""
        except Exception:
            return None

        domain = self._normalize_domain(hostname)
        if domain not in self.search_domains:
            return None

        qs = parse_qs(parsed.query)
        for param in self.keyword_params:
            values = qs.get(param)
            if values:
                keyword = values[0].strip()
                if keyword:
                    if self.normalize_case:
                        keyword = keyword.lower()
                    return domain, keyword

        return None

    def _normalize_domain(self, hostname: str) -> str:
        """
        Strip subdomains: www.google.com → google.com,
        search.yahoo.com → yahoo.com.
        """
        parts = hostname.lower().split(".")
        return ".".join(parts[-2:]) if len(parts) >= 2 else hostname.lower()

    # ------------------------------------------------------------------
    # Event / product helpers
    # ------------------------------------------------------------------

    def _has_purchase_event(self, event_list: str) -> bool:
        """Return True if purchase_event_id appears in event_list."""
        if not event_list:
            return False
        return self.purchase_event_id in [
            e.strip() for e in event_list.split(self.event_delim)
        ]

    def _extract_revenue(self, product_list: str) -> float:
        """
        Parse product_list and return the sum of revenue across all products.

        Adobe product_list format:
            category;name;quantity;revenue;events;evars,...

        Revenue sits at index self.revenue_col_idx (default 3, 0-based).
        Multiple products are separated by prod_delim (default ",").
        Missing or non-numeric revenue fields are silently skipped.
        """
        if not product_list:
            return 0.0
        total = 0.0
        for product in product_list.split(self.prod_delim):
            attrs = product.split(self.prod_attr_delim)
            if len(attrs) > self.revenue_col_idx:
                raw = attrs[self.revenue_col_idx].strip()
                try:
                    total += float(raw)
                except (ValueError, TypeError):
                    pass
        return total

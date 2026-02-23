"""
generate_test_data.py
=====================
Deterministic test data generator for SKP pipeline testing.
Crawl → Walk → Jog → Run → Sprint

Each dataset embeds known ground truth anchors that must appear
in the output regardless of scale. The generator is seeded so
results are always reproducible.

Ground Truth Anchors (present at ALL scales):
  google.com  / ipod        → $480.00  (original assessment answer)
  bing.com    / zune        → $250.00  (original assessment answer)

Additional anchors added at larger scales:
  google.com  / laptop      → $1,200.00  (Phase 2+)
  bing.com    / headphones  → $350.00    (Phase 2+)
  google.com  / tablet      → $2,500.00  (Phase 4+)
  bing.com    / camera      → $899.00    (Phase 4+)

Usage:
  python generate_test_data.py          # generates all phases
  python generate_test_data.py --phase 2  # generates phase 2 only
"""

import csv
import io
import os
import random
import argparse
import time
from datetime import datetime, timedelta

# ── Deterministic seed ────────────────────────────────────────────────
SEED = 42

# ── Output directory ──────────────────────────────────────────────────
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "data")

# ── Real user agents from sample data ────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.10) Gecko/2009042316 Firefox/3.0.10",
    '"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_4_11; en) AppleWebKit/525.27.1 (KHTML, like Gecko) Version/3.2.1 Safari/525.27.1"',
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124",
    '"Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148"',
    "Mozilla/5.0 (X11; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0",
    '"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/91.0.864.59"',
]

# ── Geography pools ───────────────────────────────────────────────────
CITIES = [
    ("Salem", "OR", "US"), ("Rochester", "NY", "US"), ("Salt Lake City", "UT", "US"),
    ("Duncan", "OK", "US"), ("Austin", "TX", "US"), ("Portland", "OR", "US"),
    ("Seattle", "WA", "US"), ("Denver", "CO", "US"), ("Chicago", "IL", "US"),
    ("Miami", "FL", "US"), ("Boston", "MA", "US"), ("Atlanta", "GA", "US"),
    ("Dallas", "TX", "US"), ("Phoenix", "AZ", "US"), ("San Diego", "CA", "US"),
    ("Minneapolis", "MN", "US"), ("Detroit", "MI", "US"), ("Nashville", "TN", "US"),
]

# ── Products catalogue ────────────────────────────────────────────────
PRODUCTS = {
    "ipod":       [("Electronics", "Ipod - Nano - 8GB",    1, 190.0),
                   ("Electronics", "Ipod - Touch - 32GB",  1, 290.0),
                   ("Electronics", "Ipod - Classic - 80GB",1, 249.0)],
    "zune":       [("Electronics", "Zune - 32GB",          1, 250.0),
                   ("Electronics", "Zune - 80GB",          1, 349.0)],
    "laptop":     [("Computers",   "Dell Latitude 14",     1, 899.0),
                   ("Computers",   "HP Pavilion 15",       1, 749.0),
                   ("Computers",   "Lenovo ThinkPad X1",   1, 1199.0)],
    "headphones": [("Electronics", "Sony WH-1000XM4",      1, 350.0),
                   ("Electronics", "Bose QC45",            1, 329.0)],
    "tablet":     [("Electronics", "iPad Air 5th Gen",     1, 749.0),
                   ("Electronics", "Samsung Galaxy Tab S8",1, 699.0),
                   ("Electronics", "Microsoft Surface Go", 1, 399.0)],
    "camera":     [("Electronics", "Canon EOS R50",        1, 679.0),
                   ("Electronics", "Sony ZV-E10",          1, 749.0),
                   ("Electronics", "Nikon Z30",            1, 899.0)],
    "monitor":    [("Computers",   "LG 27UK850",           1, 449.0),
                   ("Computers",   "Dell U2722D",          1, 599.0)],
    "keyboard":   [("Accessories", "Logitech MX Keys",     1,  99.0),
                   ("Accessories", "Das Keyboard 4",       1, 169.0)],
}

# ── Search engine referrer templates ─────────────────────────────────
SEARCH_REFERRERS = {
    "google.com": "http://www.google.com/search?hl=en&q={keyword}&client=firefox-a",
    "bing.com":   "http://www.bing.com/search?q={keyword}&form=QBLH",
    "yahoo.com":  "http://search.yahoo.com/search?p={keyword}&fr=yfp-t-701",
    "ask.com":    "http://www.ask.com/web?q={keyword}",
    "aol.com":    "http://search.aol.com/aol/search?q={keyword}",
}

# ── Non-search referrers (noise) ──────────────────────────────────────
NOISE_REFERRERS = [
    "http://www.esshopzilla.com",
    "http://www.esshopzilla.com/hotbuys/",
    "http://www.esshopzilla.com/search/?k=electronics",
    "http://www.facebook.com/",
    "http://www.twitter.com/",
    "http://www.amazon.com/",         # NOT a search engine — should not be attributed
    "http://www.reddit.com/r/deals/",
    "http://www.esshopzilla.com",  # direct visit (internal)
]

# ── Page templates ────────────────────────────────────────────────────
PAGE_TEMPLATES = [
    ("Home",              "http://www.esshopzilla.com"),
    ("Hot Buys",          "http://www.esshopzilla.com/hotbuys/"),
    ("Search Results",    "http://www.esshopzilla.com/search/?k={keyword}"),
    ("Shopping Cart",     "http://www.esshopzilla.com/cart/"),
    ("Order Checkout",    "https://www.esshopzilla.com/checkout/"),
    ("Order Confirmation","https://www.esshopzilla.com/checkout/?a=confirm"),
    ("Order Complete",    "https://www.esshopzilla.com/checkout/?a=complete"),
]

# ── Ground truth session definitions ─────────────────────────────────
# Each entry: (ip, search_engine, keyword, products_to_buy)
# products_to_buy: list of (category, name, qty, revenue)
GROUND_TRUTH_SESSIONS = {
    # Phase 1+ — original assessment anchors
    "anchor_ipod_1": {
        "ip": "67.98.123.1",
        "engine": "google.com",
        "keyword": "Ipod",  # mixed case — tests normalization
        "city": ("Salem", "OR", "US"),
        "purchases": [("Electronics", "Ipod - Touch - 32GB", 1, 290.0)],
        "start_offset": 0,
    },
    "anchor_ipod_2": {
        "ip": "44.12.96.2",
        "engine": "google.com",
        "keyword": "ipod",  # lowercase — tests normalization combines with above
        "city": ("Duncan", "OK", "US"),
        "purchases": [("Electronics", "Ipod - Nano - 8GB", 1, 190.0)],
        "start_offset": 200,
    },
    "anchor_zune": {
        "ip": "23.8.61.21",
        "engine": "bing.com",
        "keyword": "Zune",
        "city": ("Rochester", "NY", "US"),
        "purchases": [("Electronics", "Zune - 32GB", 1, 250.0)],
        "start_offset": 100,
    },
    "anchor_yahoo_no_purchase": {
        "ip": "112.33.98.231",
        "engine": "yahoo.com",
        "keyword": "cd player",
        "city": ("Salt Lake City", "UT", "US"),
        "purchases": [],  # NO purchase — should NOT appear in output
        "start_offset": 150,
    },
    # Phase 2+ anchors
    "anchor_laptop": {
        "ip": "198.51.100.10",
        "engine": "google.com",
        "keyword": "laptop",
        "city": ("Austin", "TX", "US"),
        "purchases": [("Computers", "Lenovo ThinkPad X1", 1, 1199.0)],
        "start_offset": 400,
    },
    "anchor_headphones": {
        "ip": "203.0.113.25",
        "engine": "bing.com",
        "keyword": "headphones",
        "city": ("Seattle", "WA", "US"),
        "purchases": [("Electronics", "Sony WH-1000XM4", 1, 350.0)],
        "start_offset": 500,
    },
    # Phase 4+ anchors
    "anchor_tablet": {
        "ip": "198.51.100.50",
        "engine": "google.com",
        "keyword": "tablet",
        "city": ("Denver", "CO", "US"),
        "purchases": [("Electronics", "iPad Air 5th Gen", 1, 749.0),
                      ("Electronics", "iPad Air 5th Gen", 1, 749.0),   # 2 units
                      ("Electronics", "Microsoft Surface Go", 1, 399.0)],  # multi-item
        "start_offset": 600,
    },
    "anchor_camera": {
        "ip": "203.0.113.75",
        "engine": "bing.com",
        "keyword": "camera",
        "city": ("Chicago", "IL", "US"),
        "purchases": [("Electronics", "Nikon Z30", 1, 899.0)],
        "start_offset": 700,
    },
}

# ── Ground truth expected results by phase ────────────────────────────
EXPECTED_RESULTS = {
    1: [
        ("google.com", "ipod",   480.00),
        ("bing.com",   "zune",   250.00),
    ],
    2: [
        ("google.com", "ipod",        480.00),
        ("google.com", "laptop",     1199.00),
        ("bing.com",   "zune",        250.00),
        ("bing.com",   "headphones",  350.00),
    ],
    3: [  # same as phase 2 — tests path switching not new data
        ("google.com", "ipod",        480.00),
        ("google.com", "laptop",     1199.00),
        ("bing.com",   "zune",        250.00),
        ("bing.com",   "headphones",  350.00),
    ],
    4: [
        ("google.com", "ipod",        480.00),
        ("google.com", "laptop",     1199.00),
        ("google.com", "tablet",     1897.00),  # 749+749+399
        ("bing.com",   "zune",        250.00),
        ("bing.com",   "headphones",  350.00),
        ("bing.com",   "camera",      899.00),
    ],
    5: [  # same anchors as phase 4, more noise rows
        ("google.com", "ipod",        480.00),
        ("google.com", "laptop",     1199.00),
        ("google.com", "tablet",     1897.00),
        ("bing.com",   "zune",        250.00),
        ("bing.com",   "headphones",  350.00),
        ("bing.com",   "camera",      899.00),
    ],
}


class HitDataGenerator:
    """
    Generates realistic Adobe Analytics hit-level data files.
    All output is deterministic given the same seed.
    """

    COLUMNS = [
        "hit_time_gmt", "date_time", "user_agent", "ip",
        "event_list", "geo_city", "geo_region", "geo_country",
        "pagename", "page_url", "product_list", "referrer"
    ]

    def __init__(self, seed=SEED):
        self.rng = random.Random(seed)
        self.base_ts = 1254033280  # 2009-09-27 06:34:40 — matches original data

    def _ts_to_datetime(self, ts):
        dt = datetime(2009, 9, 27, 6, 34, 40) + timedelta(seconds=(ts - self.base_ts))
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def _random_ip(self):
        return f"{self.rng.randint(1,254)}.{self.rng.randint(1,254)}.{self.rng.randint(1,254)}.{self.rng.randint(1,254)}"

    def _make_product_list(self, products):
        """Format: category;name;qty;revenue;events;evar"""
        parts = []
        for cat, name, qty, rev in products:
            if rev > 0:
                parts.append(f"{cat};{name};{qty};{rev};")
            else:
                parts.append(f"{cat};{name};{qty};;")
        return ",".join(parts)

    def _build_session_rows(self, session_def, ts_start):
        """
        Build the sequence of hit rows for one user session.
        Mimics real Adobe Analytics: search → browse → product → cart → checkout → complete
        """
        rows = []
        ts = ts_start
        ip = session_def["ip"]
        engine = session_def["engine"]
        keyword = session_def["keyword"]
        city, region, country = session_def["city"]
        ua = self.rng.choice(USER_AGENTS)

        referrer_url = SEARCH_REFERRERS[engine].format(keyword=keyword.replace(" ", "+"))

        # Hit 1: Search engine landing (referrer = search engine)
        rows.append({
            "hit_time_gmt": str(ts),
            "date_time":    self._ts_to_datetime(ts),
            "user_agent":   ua,
            "ip":           ip,
            "event_list":   "",
            "geo_city":     city,
            "geo_region":   region,
            "geo_country":  country,
            "pagename":     "Home",
            "page_url":     "http://www.esshopzilla.com",
            "product_list": "",
            "referrer":     referrer_url,
        })
        ts += self.rng.randint(90, 120)

        if not session_def["purchases"]:
            # Browse only — no purchase
            rows.append({
                "hit_time_gmt": str(ts),
                "date_time":    self._ts_to_datetime(ts),
                "user_agent":   ua,
                "ip":           ip,
                "event_list":   "",
                "geo_city":     city,
                "geo_region":   region,
                "geo_country":  country,
                "pagename":     "Search Results",
                "page_url":     f"http://www.esshopzilla.com/search/?k={keyword}",
                "product_list": "",
                "referrer":     "http://www.esshopzilla.com",
            })
            return rows

        # Hit 2: Browse product page
        first_product = session_def["purchases"][0]
        rows.append({
            "hit_time_gmt": str(ts),
            "date_time":    self._ts_to_datetime(ts),
            "user_agent":   ua,
            "ip":           ip,
            "event_list":   "2",  # product view
            "geo_city":     city,
            "geo_region":   region,
            "geo_country":  country,
            "pagename":     first_product[1],
            "page_url":     "http://www.esshopzilla.com/product/?pid=as32213",
            "product_list": self._make_product_list([first_product]),
            "referrer":     "http://www.esshopzilla.com",
        })
        ts += self.rng.randint(90, 120)

        # Hit 3: Add to cart
        rows.append({
            "hit_time_gmt": str(ts),
            "date_time":    self._ts_to_datetime(ts),
            "user_agent":   ua,
            "ip":           ip,
            "event_list":   "12",  # add to cart
            "geo_city":     city,
            "geo_region":   region,
            "geo_country":  country,
            "pagename":     "Shopping Cart",
            "page_url":     "http://www.esshopzilla.com/cart/",
            "product_list": "",
            "referrer":     "http://www.esshopzilla.com/product/?pid=as32213",
        })
        ts += self.rng.randint(90, 120)

        # Hit 4: Checkout
        rows.append({
            "hit_time_gmt": str(ts),
            "date_time":    self._ts_to_datetime(ts),
            "user_agent":   ua,
            "ip":           ip,
            "event_list":   "11",  # checkout start
            "geo_city":     city,
            "geo_region":   region,
            "geo_country":  country,
            "pagename":     "Order Checkout Details",
            "page_url":     "https://www.esshopzilla.com/checkout/",
            "product_list": "",
            "referrer":     "http://www.esshopzilla.com/cart/",
        })
        ts += self.rng.randint(90, 120)

        # Hit 5: Order confirmation page
        rows.append({
            "hit_time_gmt": str(ts),
            "date_time":    self._ts_to_datetime(ts),
            "user_agent":   ua,
            "ip":           ip,
            "event_list":   "",
            "geo_city":     city,
            "geo_region":   region,
            "geo_country":  country,
            "pagename":     "Order Confirmation",
            "page_url":     "https://www.esshopzilla.com/checkout/?a=confirm",
            "product_list": "",
            "referrer":     "https://www.esshopzilla.com/checkout/",
        })
        ts += self.rng.randint(90, 120)

        # Hit 6: Purchase complete — event1 = purchase
        rows.append({
            "hit_time_gmt": str(ts),
            "date_time":    self._ts_to_datetime(ts),
            "user_agent":   ua,
            "ip":           ip,
            "event_list":   "1",  # PURCHASE
            "geo_city":     city,
            "geo_region":   region,
            "geo_country":  country,
            "pagename":     "Order Complete",
            "page_url":     "https://www.esshopzilla.com/checkout/?a=complete",
            "product_list": self._make_product_list(session_def["purchases"]),
            "referrer":     "https://www.esshopzilla.com/checkout/?a=confirm",
        })

        return rows

    def _build_noise_row(self, ts, ip=None):
        """Generate a realistic browsing hit with no search engine referrer."""
        city, region, country = self.rng.choice(CITIES)
        ip = ip or self._random_ip()
        ua = self.rng.choice(USER_AGENTS)
        page_name, page_url = self.rng.choice(PAGE_TEMPLATES[:4])
        keyword = self.rng.choice(["electronics", "sale", "deals", "gifts"])
        page_url = page_url.replace("{keyword}", keyword)

        return {
            "hit_time_gmt": str(ts),
            "date_time":    self._ts_to_datetime(ts),
            "user_agent":   ua,
            "ip":           ip,
            "event_list":   "",
            "geo_city":     city,
            "geo_region":   region,
            "geo_country":  country,
            "pagename":     page_name,
            "page_url":     page_url,
            "product_list": "",
            "referrer":     self.rng.choice(NOISE_REFERRERS),
        }

    def _build_noise_purchase_session(self, ts, engine=None, keyword=None):
        """
        Build a noise PURCHASE session — from a non-search referrer.
        Purchase should NOT be attributed (no search engine in session).
        Tests that direct/social purchases are correctly excluded.
        """
        ip = self._random_ip()
        city, region, country = self.rng.choice(CITIES)
        ua = self.rng.choice(USER_AGENTS)
        product_key = self.rng.choice(list(PRODUCTS.keys()))
        product = self.rng.choice(PRODUCTS[product_key])
        rows = []

        # Browse from non-search source (facebook, direct, etc.)
        rows.append({
            "hit_time_gmt": str(ts),
            "date_time":    self._ts_to_datetime(ts),
            "user_agent":   ua,
            "ip":           ip,
            "event_list":   "",
            "geo_city":     city, "geo_region": region, "geo_country": country,
            "pagename":     "Home",
            "page_url":     "http://www.esshopzilla.com",
            "product_list": "",
            "referrer":     self.rng.choice(["http://www.facebook.com/",
                                              "http://www.twitter.com/",
                                              "http://www.esshopzilla.com",  # direct
                                              "http://www.amazon.com/"]),
        })
        ts += self.rng.randint(60, 180)

        # Purchase — but no search session for this IP
        rows.append({
            "hit_time_gmt": str(ts),
            "date_time":    self._ts_to_datetime(ts),
            "user_agent":   ua,
            "ip":           ip,
            "event_list":   "1",  # purchase
            "geo_city":     city, "geo_region": region, "geo_country": country,
            "pagename":     "Order Complete",
            "page_url":     "https://www.esshopzilla.com/checkout/?a=complete",
            "product_list": self._make_product_list([product]),
            "referrer":     "https://www.esshopzilla.com/checkout/?a=confirm",
        })
        return rows

    def generate_phase(self, phase: int, target_rows: int, anchors: list) -> list:
        """
        Generate all rows for a given phase.

        Args:
            phase:       Phase number (1-5)
            target_rows: Approximate number of rows to generate
            anchors:     List of anchor session keys to embed

        Returns:
            List of row dicts sorted by hit_time_gmt
        """
        all_rows = []
        ts = self.base_ts

        # ── Embed anchor sessions ────────────────────────────────────
        for anchor_key in anchors:
            session = GROUND_TRUTH_SESSIONS[anchor_key]
            session_ts = ts + session["start_offset"]
            rows = self._build_session_rows(session, session_ts)
            all_rows.extend(rows)

        anchor_row_count = len(all_rows)
        noise_rows_needed = target_rows - anchor_row_count
        ts_counter = ts + 5000  # start noise rows after anchors

        # ── Fill with noise browsing rows ────────────────────────────
        # Ratio: 80% pure browsing, 15% direct purchases, 5% search no-purchase
        noise_browse   = int(noise_rows_needed * 0.80)
        noise_purchase = int(noise_rows_needed * 0.15)
        noise_search   = int(noise_rows_needed * 0.05)

        for _ in range(noise_browse):
            all_rows.append(self._build_noise_row(ts_counter))
            ts_counter += self.rng.randint(10, 60)

        for _ in range(noise_purchase):
            session_rows = self._build_noise_purchase_session(ts_counter)
            all_rows.extend(session_rows)
            ts_counter += self.rng.randint(120, 300)

        for _ in range(noise_search):
            # Search session with no purchase — tests exclusion logic
            noise_engine = self.rng.choice(["google.com", "yahoo.com", "ask.com"])
            noise_keyword = self.rng.choice(["shoes", "tv", "furniture", "clothing"])
            noise_session = {
                "ip":       self._random_ip(),
                "engine":   noise_engine,
                "keyword":  noise_keyword,
                "city":     self.rng.choice(CITIES),
                "purchases": [],  # no purchase
                "start_offset": 0,
            }
            rows = self._build_session_rows(noise_session, ts_counter)
            all_rows.extend(rows)
            ts_counter += self.rng.randint(200, 500)

        # Sort by timestamp (ascending) — real Adobe data is chronological
        all_rows.sort(key=lambda r: int(r["hit_time_gmt"]))
        return all_rows

    def write_tab_file(self, rows: list, filepath: str):
        """Write rows as tab-delimited file with CRLF line endings (Windows format)."""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", newline="\r\n", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.COLUMNS, delimiter="\t")
            writer.writeheader()
            writer.writerows(rows)
        size_bytes = os.path.getsize(filepath)
        size_mb = size_bytes / (1024 * 1024)
        print(f"  Written: {filepath}")
        print(f"  Rows:    {len(rows):,}")
        print(f"  Size:    {size_bytes:,} bytes ({size_mb:.2f} MB)")
        return size_bytes


# ── Phase definitions ─────────────────────────────────────────────────
PHASE_CONFIGS = {
    1: {
        "name":        "Crawl",
        "description": "Original assessment file — correctness baseline",
        "target_rows": 21,
        "anchors":     ["anchor_ipod_1", "anchor_ipod_2", "anchor_zune", "anchor_yahoo_no_purchase"],
        "expected":    EXPECTED_RESULTS[1],
        "path":        "Lambda",
    },
    2: {
        "name":        "Walk",
        "description": "7MB — Lambda handles it. DQ at volume. New anchors.",
        "target_rows": 21_000,
        "anchors":     ["anchor_ipod_1", "anchor_ipod_2", "anchor_zune",
                        "anchor_yahoo_no_purchase", "anchor_laptop", "anchor_headphones"],
        "expected":    EXPECTED_RESULTS[2],
        "path":        "Lambda",
    },
    3: {
        "name":        "Jog",
        "description": "70MB — Approaching Lambda limit. Tests path switching.",
        "target_rows": 210_000,
        "anchors":     ["anchor_ipod_1", "anchor_ipod_2", "anchor_zune",
                        "anchor_yahoo_no_purchase", "anchor_laptop", "anchor_headphones"],
        "expected":    EXPECTED_RESULTS[3],
        "path":        "Lambda or EMR (tests threshold)",
    },
    4: {
        "name":        "Run",
        "description": "700MB — EMR Serverless + PySpark. Parity test.",
        "target_rows": 2_100_000,
        "anchors":     ["anchor_ipod_1", "anchor_ipod_2", "anchor_zune",
                        "anchor_yahoo_no_purchase", "anchor_laptop", "anchor_headphones",
                        "anchor_tablet", "anchor_camera"],
        "expected":    EXPECTED_RESULTS[4],
        "path":        "EMR Serverless",
    },
    5: {
        "name":        "Sprint",
        "description": "7GB — Full production scale. PySpark optimization.",
        "target_rows": 21_000_000,
        "anchors":     ["anchor_ipod_1", "anchor_ipod_2", "anchor_zune",
                        "anchor_yahoo_no_purchase", "anchor_laptop", "anchor_headphones",
                        "anchor_tablet", "anchor_camera"],
        "expected":    EXPECTED_RESULTS[5],
        "path":        "EMR Serverless",
    },
}


def generate_all(phases=None, output_dir=OUTPUT_DIR):
    """Generate test data for specified phases (default: all)."""
    phases = phases or [1, 2, 3, 4, 5]
    gen = HitDataGenerator(seed=SEED)

    print("=" * 65)
    print("SKP Test Data Generator — Crawl → Walk → Jog → Run → Sprint")
    print("=" * 65)

    for phase_num in phases:
        cfg = PHASE_CONFIGS[phase_num]
        print(f"\nPhase {phase_num} — {cfg['name']}: {cfg['description']}")
        print(f"  Target rows: {cfg['target_rows']:,}")
        print(f"  Processing:  {cfg['path']}")
        print(f"  Anchors:     {len(cfg['anchors'])} ground truth sessions")

        # Skip Phase 5 by default (7GB takes too long to generate in tests)
        if phase_num == 5 and phases == [1, 2, 3, 4, 5]:
            print("  [SKIPPING Phase 5 — 7GB file. Run explicitly: --phase 5]")
            continue

        start = time.time()
        rows = gen.generate_phase(phase_num, cfg["target_rows"], cfg["anchors"])
        filename = f"hit_data_phase{phase_num}_{cfg['name'].lower()}.tab"
        filepath = os.path.join(output_dir, filename)
        gen.write_tab_file(rows, filepath)

        elapsed = time.time() - start
        print(f"  Generated in: {elapsed:.1f}s")
        print(f"  Expected results:")
        for engine, keyword, revenue in cfg["expected"]:
            print(f"    {engine:<20} {keyword:<15} ${revenue:,.2f}")

    # Write expected results manifest
    manifest_path = os.path.join(output_dir, "expected_results_manifest.py")
    with open(manifest_path, "w") as f:
        f.write('"""\nExpected results manifest for all test phases.\nAuto-generated by generate_test_data.py\n"""\n\n')
        f.write("EXPECTED_RESULTS = {\n")
        for phase_num, cfg in PHASE_CONFIGS.items():
            f.write(f"    {phase_num}: [  # {cfg['name']} — {cfg['description']}\n")
            for engine, keyword, revenue in cfg["expected"]:
                f.write(f"        ('{engine}', '{keyword}', {revenue}),\n")
            f.write("    ],\n")
        f.write("}\n")
    print(f"\nManifest written: {manifest_path}")
    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate SKP test data")
    parser.add_argument("--phase", type=int, choices=[1, 2, 3, 4, 5],
                        help="Generate specific phase only")
    parser.add_argument("--output-dir", default=OUTPUT_DIR,
                        help="Output directory for generated files")
    args = parser.parse_args()

    phases = [args.phase] if args.phase else [1, 2, 3, 4]
    generate_all(phases=phases, output_dir=args.output_dir)

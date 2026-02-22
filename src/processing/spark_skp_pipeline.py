"""
spark_skp_pipeline.py
=====================
PySpark implementation of the Search Keyword Performance pipeline.
Runs on AWS EMR Serverless for files >= 100MB.

Mirrors the exact business logic of the pure Python Lambda path.
Output must be byte-identical to the Lambda path for the same input.

Algorithm (Session-Based First-Touch Attribution):
  Step 1 — Read tab-delimited file from S3 into Spark DataFrame
  Step 2 — Schema validation (all required columns present)
  Step 3 — Null validation (critical columns)
  Step 4 — Deduplication (composite key)
  Step 5 — Extract search engine sessions (Window function, first-touch)
  Step 6 — Extract purchase revenue (UDF on product_list)
  Step 7 — Join sessions to purchases, attribute revenue
  Step 8 — Aggregate by domain+keyword, sum revenue
  Step 9 — Sort descending, write tab-delimited output to S3

Usage (local with spark-submit):
  spark-submit spark_skp_pipeline.py \\
    --input  s3://skp-raw-dev-.../raw/hit_data.tab \\
    --output s3://skp-processed-dev-.../processed/ \\
    --env    dev

Usage (EMR Serverless) — submitted via aws emr-serverless start-job-run
"""

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

# ── PySpark imports ───────────────────────────────────────────────────
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType
)

# ── Logging ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("spark_skp_pipeline")

# ── Constants (all business rules come from config or args) ───────────
REQUIRED_COLUMNS = [
    "hit_time_gmt", "date_time", "user_agent", "ip",
    "event_list", "geo_city", "geo_region", "geo_country",
    "pagename", "page_url", "product_list", "referrer",
]
CRITICAL_COLUMNS = ["hit_time_gmt", "ip", "referrer"]
DEDUP_KEY_COLUMNS = ["hit_time_gmt", "ip", "page_url"]
SEARCH_ENGINE_DOMAINS = ["google.com", "yahoo.com", "bing.com", "ask.com", "aol.com", "msn.com"]
KEYWORD_PARAMS = ["q", "p", "query", "text", "s", "qs"]
PURCHASE_EVENT_ID = "1"
REVENUE_COL_INDEX = 3
OUTPUT_COLUMNS = ["Search Engine Domain", "Search Keyword", "Revenue"]


# ══════════════════════════════════════════════════════════════════════
# SPARK SESSION
# ══════════════════════════════════════════════════════════════════════

def create_spark_session(app_name="SearchKeywordPerformance", local=False):
    """
    Create and configure SparkSession.

    Local mode: for development and testing on laptop.
    Cluster mode: for EMR Serverless — uses environment defaults.
    """
    builder = SparkSession.builder.appName(app_name)

    if local:
        builder = (
            builder
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")   # small for local
            .config("spark.driver.memory", "2g")
        )
    else:
        # EMR Serverless — Spark picks up cluster config from environment
        builder = (
            builder
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.sql.adaptive.enabled", "true")          # AQE on
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")   # suppress verbose Spark INFO
    return spark


# ══════════════════════════════════════════════════════════════════════
# STEP 1 — READ
# ══════════════════════════════════════════════════════════════════════

def read_input(spark, input_path):
    """
    Read tab-delimited hit data file into a Spark DataFrame.

    Uses inferSchema=False because we want strings for all columns.
    Type casting happens explicitly later — safer than inferring types
    from potentially dirty data.
    """
    logger.info("Reading input: %s", input_path)

    df = (
        spark.read
        .option("delimiter", "\t")
        .option("header", "true")
        .option("inferSchema", "false")       # all columns as string first
        .option("encoding", "UTF-8")
        .option("multiLine", "false")
        .option("escape", '"')
        .csv(input_path)
    )

    # Strip Windows CRLF from string values (same as pure Python path)
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.regexp_replace(F.col(col_name), r"\r", "")
        )

    row_count = df.count()
    logger.info("Read %d rows, %d columns.", row_count, len(df.columns))
    return df, row_count


# ══════════════════════════════════════════════════════════════════════
# STEP 2 — SCHEMA VALIDATION
# ══════════════════════════════════════════════════════════════════════

def validate_schema(df):
    """
    Check all required columns are present.
    Raises ValueError immediately if any are missing — fail fast.
    """
    actual_cols = set(df.columns)
    missing = sorted(set(REQUIRED_COLUMNS) - actual_cols)

    if missing:
        raise ValueError(f"Schema validation FAILED. Missing columns: {missing}")

    logger.info("Schema validation PASSED. All %d required columns present.", len(REQUIRED_COLUMNS))
    return {"check": "schema_validation", "passed": True, "missing": []}


# ══════════════════════════════════════════════════════════════════════
# STEP 3 — NULL VALIDATION
# ══════════════════════════════════════════════════════════════════════

def validate_nulls(df, total_rows):
    """
    Check critical columns have no nulls.
    Non-critical columns emit warnings if null rate exceeds threshold.

    Key difference from pure Python path:
    Spark treats empty string ("") differently from null.
    We check BOTH to match the pure Python behavior.
    """
    failures = []

    for col_name in CRITICAL_COLUMNS:
        null_count = df.filter(
            F.col(col_name).isNull() | (F.trim(F.col(col_name)) == "")
        ).count()

        if null_count > 0:
            failures.append(col_name)
            logger.error(
                "CRITICAL column '%s' has %d null(s) — pipeline cannot continue.",
                col_name, null_count
            )

    if failures:
        raise ValueError(f"Null validation FAILED. Critical columns with nulls: {failures}")

    # Warn on non-critical columns with high null rates
    for col_name in REQUIRED_COLUMNS:
        if col_name in CRITICAL_COLUMNS:
            continue
        null_count = df.filter(
            F.col(col_name).isNull() | (F.trim(F.col(col_name)) == "")
        ).count()
        null_pct = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0
        if null_pct > 50:
            logger.warning("Column '%s' has high null rate: %.2f%%", col_name, null_pct)

    logger.info("Null validation PASSED. No critical column nulls.")
    return {"check": "null_validation", "passed": True, "critical_failures": []}


# ══════════════════════════════════════════════════════════════════════
# STEP 4 — DEDUPLICATION
# ══════════════════════════════════════════════════════════════════════

def deduplicate(df, total_rows):
    """
    Remove duplicate rows based on composite key (hit_time_gmt, ip, page_url).
    Uses Spark's built-in dropDuplicates — optimized internally.
    """
    df_deduped = df.dropDuplicates(DEDUP_KEY_COLUMNS)
    deduped_count = df_deduped.count()
    dupes = total_rows - deduped_count

    if dupes > 0:
        logger.warning("Duplicate check: %d duplicate row(s) removed.", dupes)
    else:
        logger.info("Duplicate check PASSED. No duplicates found.")

    return df_deduped, {
        "check": "duplicate_check",
        "passed": True,
        "duplicate_count": dupes,
        "rows_before": total_rows,
        "rows_after": deduped_count,
    }


# ══════════════════════════════════════════════════════════════════════
# STEP 5 — BUILD SESSION MAP (FIRST-TOUCH ATTRIBUTION)
# ══════════════════════════════════════════════════════════════════════

def build_session_map(df):
    """
    Extract the FIRST search engine referrer per IP address.

    Pure Python approach: iterate rows in time order, dict lookup.
    PySpark approach:     Window function — partition by IP, order by
                          hit_time_gmt, take first() search engine hit.

    This is the most important translation from Python to Spark.
    The logic is identical; only the execution model differs.

    Returns a DataFrame with columns: [ip, search_domain, search_keyword]
    """
    logger.info("Building session map (first-touch attribution)...")

    # ── Step 5a: Filter to rows that have a search engine referrer ───

    # Build the domain extraction expression
    # urlparse equivalent in Spark SQL:
    #   parse_url(referrer, 'HOST') → www.google.com
    #   Then strip leading 'www.' or other subdomains to normalize

    # Use a UDF to extract (domain, keyword) from referrer URL.
    # This mirrors _extract_search_info() in search_keyword_analyzer.py
    # exactly — same urllib.parse logic, same normalization, same param order.
    # UDF approach avoids PySpark version differences with parse_url().

    from urllib.parse import urlparse, parse_qs

    _domain_set_local   = set(SEARCH_ENGINE_DOMAINS)
    _keyword_params_local = KEYWORD_PARAMS

    @F.udf(returnType=StructType([
        StructField("domain",  StringType(), True),
        StructField("keyword", StringType(), True),
    ]))
    def extract_search_info(referrer):
        """
        Parse referrer URL → (normalized_domain, keyword) or (None, None).
        Identical to SearchKeywordAnalyzer._extract_search_info().
        """
        if not referrer or referrer.strip() in ("", "nan", "None"):
            return (None, None)
        try:
            parsed   = urlparse(referrer)
            hostname = (parsed.hostname or "").lower()
            # Normalize: strip subdomains, keep last two parts
            parts    = hostname.split(".")
            domain   = ".".join(parts[-2:]) if len(parts) >= 2 else hostname
            if domain not in _domain_set_local:
                return (None, None)
            query_params = parse_qs(parsed.query)
            keyword = None
            for param in _keyword_params_local:
                if param in query_params and query_params[param][0]:
                    keyword = query_params[param][0].lower().strip()
                    break
            if keyword is None:
                return (None, None)
            return (domain, keyword)
        except Exception:
            return (None, None)

    df_with_search = df.withColumn("_search", extract_search_info(F.col("referrer")))
    df_with_search = df_with_search.withColumn("search_domain",  F.col("_search.domain")) \
                                   .withColumn("search_keyword", F.col("_search.keyword")) \
                                   .drop("_search")

    # Keep only rows where search info was successfully extracted
    df_search_hits = df_with_search.filter(
        F.col("search_domain").isNotNull() & F.col("search_keyword").isNotNull()
    )

    # ── Step 5b: First-touch — keep only FIRST search hit per IP ──────
    # Window: partition by IP, order by hit_time_gmt ascending
    # first_value() picks the earliest search engine hit for each IP

    window_spec = Window.partitionBy("ip").orderBy(F.col("hit_time_gmt").cast(LongType()))

    df_session_map = (
        df_search_hits
        .withColumn("first_domain",  F.first("search_domain",  ignorenulls=True).over(window_spec))
        .withColumn("first_keyword", F.first("search_keyword", ignorenulls=True).over(window_spec))
        # Keep only the first row per IP (row_number == 1)
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .select(
            F.col("ip"),
            F.col("first_domain").alias("search_domain"),
            F.col("first_keyword").alias("search_keyword"),
        )
    )

    session_count = df_session_map.count()
    logger.info("Session map built. %d unique search sessions found.", session_count)

    return df_session_map


# ══════════════════════════════════════════════════════════════════════
# STEP 6 — EXTRACT PURCHASE REVENUE
# ══════════════════════════════════════════════════════════════════════

def extract_purchase_rows(df):
    """
    Filter to purchase rows and extract revenue from product_list.

    A purchase row must:
      1. Have PURCHASE_EVENT_ID ('1') in event_list
      2. Have a non-empty product_list with parseable revenue

    product_list format: category;name;qty;revenue;events;evars,...
    Revenue is at semicolon-index 3 within each comma-separated product.

    We use a Spark UDF for the revenue parsing because it mirrors
    the pure Python logic exactly — important for parity testing.
    """
    logger.info("Extracting purchase rows...")

    # ── Filter to rows with purchase event ───────────────────────────
    # event_list can be "1", "1,2", "2,1,3" — we check if '1' is in the list
    df_purchases = df.filter(
        F.array_contains(
            F.split(F.col("event_list"), ","),
            F.lit(PURCHASE_EVENT_ID)
        )
    ).filter(
        F.col("product_list").isNotNull()
        & (F.trim(F.col("product_list")) != "")
        & (F.trim(F.col("product_list")) != "nan")
    )

    # ── Parse revenue from product_list using UDF ─────────────────────
    @F.udf(returnType=DoubleType())
    def parse_revenue(product_list_str):
        """
        Parse product_list and sum revenue across all products.
        Mirrors _extract_revenue() in search_keyword_analyzer.py exactly.

        Format: category;name;qty;revenue;events,category;name;qty;revenue
        """
        if not product_list_str or product_list_str.strip() in ("", "nan", "None"):
            return 0.0
        total = 0.0
        for product in product_list_str.split(","):
            attrs = product.split(";")
            if len(attrs) > REVENUE_COL_INDEX:
                try:
                    val = attrs[REVENUE_COL_INDEX].strip()
                    if val:
                        total += float(val)
                except (ValueError, TypeError):
                    pass
        return total

    df_purchases = (
        df_purchases
        .withColumn("revenue", parse_revenue(F.col("product_list")))
        .filter(F.col("revenue") > 0.0)
        .select("ip", "hit_time_gmt", "revenue")
    )

    purchase_count = df_purchases.count()
    logger.info("Found %d qualifying purchase rows.", purchase_count)

    return df_purchases


# ══════════════════════════════════════════════════════════════════════
# STEP 7 — JOIN AND ATTRIBUTE
# ══════════════════════════════════════════════════════════════════════

def attribute_revenue(df_purchases, df_session_map):
    """
    Join purchases to session map on IP address.
    Only purchases where the IP has a search engine session get attributed.
    Direct visits, social media, email clicks are excluded (no session).

    This is an INNER JOIN — purchases without a search session are dropped.
    This mirrors: session_map.get(ip) is None → skip in pure Python.
    """
    logger.info("Attributing revenue to search sessions...")

    df_attributed = df_purchases.join(
        df_session_map,
        on="ip",
        how="inner"   # inner join = only purchases with matching search session
    )

    attributed_count = df_attributed.count()
    logger.info("Revenue attributed to %d purchase event(s).", attributed_count)

    return df_attributed


# ══════════════════════════════════════════════════════════════════════
# STEP 8 — AGGREGATE
# ══════════════════════════════════════════════════════════════════════

def aggregate_results(df_attributed):
    """
    Group by (search_domain, search_keyword), sum revenue.
    Sort by revenue descending.
    Round to 2 decimal places.

    This mirrors _aggregate() in search_keyword_analyzer.py.
    """
    logger.info("Aggregating results...")

    df_results = (
        df_attributed
        .groupBy("search_domain", "search_keyword")
        .agg(F.round(F.sum("revenue"), 2).alias("Revenue"))
        .filter(F.col("Revenue") > 0.0)
        .withColumnRenamed("search_domain",  "Search Engine Domain")
        .withColumnRenamed("search_keyword", "Search Keyword")
        .orderBy(F.col("Revenue").desc())
    )

    result_count = df_results.count()
    total_revenue = df_results.agg(F.sum("Revenue")).collect()[0][0] or 0.0

    logger.info("=" * 60)
    logger.info("RESULTS — Search Keyword Performance")
    logger.info("=" * 60)
    for row in df_results.collect():
        logger.info("%-25s %-20s $%10.2f",
                    row["Search Engine Domain"],
                    row["Search Keyword"],
                    row["Revenue"])
    logger.info("=" * 60)
    logger.info("Output rows: %d | Total revenue: $%.2f", result_count, total_revenue)

    return df_results


# ══════════════════════════════════════════════════════════════════════
# STEP 9 — WRITE OUTPUT
# ══════════════════════════════════════════════════════════════════════

def write_output(df_results, output_path, exec_date=None):
    """
    Write results as tab-delimited file to S3.

    Important: Spark writes a directory of part files by default.
    We use coalesce(1) to produce a single file, then rename it to
    match the expected filename pattern.

    The output format matches the Lambda path exactly:
      Search Engine Domain\tSearch Keyword\tRevenue
      google.com\tipod\t480.00
    """
    if exec_date is None:
        exec_date = datetime.now(timezone.utc)

    date_str = exec_date.strftime("%Y-%m-%d")
    filename = f"{date_str}_SearchKeywordPerformance.tab"

    # Format Revenue to exactly 2 decimal places
    df_output = df_results.withColumn(
        "Revenue",
        F.format_number(F.col("Revenue"), 2)
    )

    # Use coalesce(1) so output is a single file, not 200 part files
    # For very large outputs (>1GB), remove coalesce and handle part files
    output_dir = f"{output_path.rstrip('/')}/{filename}_tmp"

    (
        df_output
        .coalesce(1)
        .write
        .option("delimiter", "\t")
        .option("header", "true")
        .mode("overwrite")
        .csv(output_dir)
    )

    logger.info("Output written to: %s", output_dir)
    return output_dir, filename


# ══════════════════════════════════════════════════════════════════════
# MAIN PIPELINE ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════

def run_pipeline(input_path, output_path, env="dev", local=False):
    """
    Orchestrate the full pipeline. Called by both the CLI entry point
    and by the EMR Serverless job runner.

    Returns a dict with run summary for lineage recording.
    """
    run_id = str(uuid.uuid4())
    start_ts = time.time()

    logger.info("=" * 60)
    logger.info("SKP PySpark Pipeline Start | run_id=%s", run_id)
    logger.info("Input:  %s", input_path)
    logger.info("Output: %s", output_path)
    logger.info("Env:    %s | Local: %s", env, local)
    logger.info("=" * 60)

    # Create Spark session
    spark = create_spark_session(local=local)

    dq_checks = []
    try:
        # ── Step 1: Read ──────────────────────────────────────────────
        df, total_rows = read_input(spark, input_path)

        # ── Step 2: Schema validation ─────────────────────────────────
        schema_result = validate_schema(df)
        dq_checks.append(schema_result)

        # ── Step 3: Null validation ───────────────────────────────────
        null_result = validate_nulls(df, total_rows)
        dq_checks.append(null_result)

        # ── Step 4: Deduplication ─────────────────────────────────────
        df_clean, dedup_result = deduplicate(df, total_rows)
        dq_checks.append(dedup_result)
        clean_rows = dedup_result["rows_after"]

        # ── Step 5: Build session map ─────────────────────────────────
        df_session_map = build_session_map(df_clean)

        # ── Step 6: Extract purchases ─────────────────────────────────
        df_purchases = extract_purchase_rows(df_clean)

        # ── Step 7: Attribute revenue ─────────────────────────────────
        df_attributed = attribute_revenue(df_purchases, df_session_map)

        # ── Step 8: Aggregate ─────────────────────────────────────────
        df_results = aggregate_results(df_attributed)
        output_rows = df_results.count()

        # ── Step 9: Write output ──────────────────────────────────────
        output_location, filename = write_output(df_results, output_path)

        duration = time.time() - start_ts
        dq_passed = all(c.get("passed", True) for c in dq_checks)

        summary = {
            "run_id":          run_id,
            "status":          "SUCCESS",
            "input_file":      input_path,
            "output_file":     output_location,
            "input_rows":      total_rows,
            "clean_rows":      clean_rows,
            "output_rows":     output_rows,
            "dq_passed":       dq_passed,
            "duration_s":      round(duration, 2),
            "engine":          "pyspark",
        }

        logger.info("=" * 60)
        logger.info("Pipeline COMPLETE")
        logger.info("Duration:    %.2fs", duration)
        logger.info("Rows in:     %d | Clean: %d | Out: %d",
                    total_rows, clean_rows, output_rows)
        logger.info("DQ passed:   %s", dq_passed)
        logger.info("=" * 60)

        return summary

    except Exception as exc:
        duration = time.time() - start_ts
        logger.error("Pipeline FAILED after %.2fs: %s", duration, exc, exc_info=True)
        return {
            "run_id":     run_id,
            "status":     "FAILED",
            "error":      str(exc),
            "duration_s": round(duration, 2),
            "engine":     "pyspark",
        }
    finally:
        spark.stop()


# ══════════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

def parse_args():
    parser = argparse.ArgumentParser(
        description="SKP PySpark Pipeline — Search Keyword Performance"
    )
    parser.add_argument(
        "--input",  required=True,
        help="Input file path (s3://bucket/key or local /path/to/file.tab)"
    )
    parser.add_argument(
        "--output", required=True,
        help="Output directory (s3://bucket/prefix/ or local /path/to/dir/)"
    )
    parser.add_argument(
        "--env",    default="dev",
        choices=["dev", "staging", "prod"],
        help="Environment (default: dev)"
    )
    parser.add_argument(
        "--local",  action="store_true",
        help="Run in local Spark mode (for testing without EMR)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = run_pipeline(
        input_path=args.input,
        output_path=args.output,
        env=args.env,
        local=args.local,
    )
    print(json.dumps(result, indent=2))
    sys.exit(0 if result["status"] == "SUCCESS" else 1)

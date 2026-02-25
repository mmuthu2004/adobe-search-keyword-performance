"""
spark_skp_pipeline.py
=====================
PySpark implementation of the Search Keyword Performance pipeline.
Runs on AWS EMR Serverless for files >= threshold_mb
(50 MB in dev / 1024 MB in prod — config-driven via processing.small_file_threshold_mb).

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
from urllib.parse import parse_qs, urlparse

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

# ── Default constants (used when no config-json is passed, e.g. in tests) ──
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


def load_pipeline_config(config_json_str=None):
    """
    Build a config dict for the Spark pipeline.
    When config_json_str is supplied (from Lambda via --config-json), its values
    override the module-level defaults so Spark always mirrors config.yaml.
    When called without args (e.g. from tests), the defaults above are used.
    """
    cfg = {
        "required_columns":           REQUIRED_COLUMNS,
        "critical_columns":           CRITICAL_COLUMNS,
        "dedup_key_columns":          DEDUP_KEY_COLUMNS,
        "search_engine_domains":      SEARCH_ENGINE_DOMAINS,
        "keyword_params":             KEYWORD_PARAMS,
        "purchase_event_id":          PURCHASE_EVENT_ID,
        "revenue_col_index":          REVENUE_COL_INDEX,
        "output_columns":             OUTPUT_COLUMNS,
        "product_delimiter":          ",",
        "product_attr_delimiter":     ";",
        "event_delimiter":            ",",
        "keyword_normalize_case":     True,
        "min_revenue_threshold":      0.0,
        "null_warning_threshold_pct": 50,
    }
    if config_json_str:
        overrides = json.loads(config_json_str)
        cfg.update(overrides)
    return cfg


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

def validate_schema(df, cfg=None):
    """
    Check all required columns are present.
    Raises ValueError immediately if any are missing — fail fast.
    """
    required = (cfg or {}).get("required_columns", REQUIRED_COLUMNS)
    actual_cols = set(df.columns)
    missing = sorted(set(required) - actual_cols)

    if missing:
        raise ValueError(f"Schema validation FAILED. Missing columns: {missing}")

    logger.info("Schema validation PASSED. All %d required columns present.", len(required))
    return {"check": "schema_validation", "passed": True, "missing": []}


# ══════════════════════════════════════════════════════════════════════
# STEP 3 — NULL VALIDATION
# ══════════════════════════════════════════════════════════════════════

def validate_nulls(df, total_rows, cfg=None):
    """
    Check critical columns have no nulls.
    Non-critical columns emit warnings if null rate exceeds threshold.

    Key difference from pure Python path:
    Spark treats empty string ("") differently from null.
    We check BOTH to match the pure Python behavior.
    """
    critical  = (cfg or {}).get("critical_columns",           CRITICAL_COLUMNS)
    required  = (cfg or {}).get("required_columns",           REQUIRED_COLUMNS)
    warn_pct  = (cfg or {}).get("null_warning_threshold_pct", 50)
    failures  = []

    for col_name in critical:
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
    critical_set = set(critical)
    for col_name in required:
        if col_name in critical_set:
            continue
        null_count = df.filter(
            F.col(col_name).isNull() | (F.trim(F.col(col_name)) == "")
        ).count()
        null_pct = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0
        if null_pct > warn_pct:
            logger.warning("Column '%s' has high null rate: %.2f%%", col_name, null_pct)

    logger.info("Null validation PASSED. No critical column nulls.")
    return {"check": "null_validation", "passed": True, "critical_failures": []}


# ══════════════════════════════════════════════════════════════════════
# STEP 4 — DEDUPLICATION
# ══════════════════════════════════════════════════════════════════════

def deduplicate(df, total_rows, cfg=None):
    """
    Remove duplicate rows based on composite key (hit_time_gmt, ip, page_url).
    Uses Spark's built-in dropDuplicates — optimized internally.
    """
    dedup_keys = (cfg or {}).get("dedup_key_columns", DEDUP_KEY_COLUMNS)
    df_deduped = df.dropDuplicates(dedup_keys)
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

def build_session_map(df, cfg=None):
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

    # Capture config values into local vars so the UDF closure picks them up
    # (urlparse / parse_qs imported at module level)
    _domain_set_local     = set((cfg or {}).get("search_engine_domains", SEARCH_ENGINE_DOMAINS))
    _keyword_params_local = (cfg or {}).get("keyword_params", KEYWORD_PARAMS)
    _normalize_case       = (cfg or {}).get("keyword_normalize_case", True)

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
                    kw = query_params[param][0].strip()
                    keyword = kw.lower() if _normalize_case else kw
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

def extract_purchase_rows(df, cfg=None):
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

    # Capture config values into local vars for UDF closures
    _purchase_event_id = (cfg or {}).get("purchase_event_id",     PURCHASE_EVENT_ID)
    _revenue_col_index = (cfg or {}).get("revenue_col_index",     REVENUE_COL_INDEX)
    _prod_delim        = (cfg or {}).get("product_delimiter",     ",")
    _prod_attr_delim   = (cfg or {}).get("product_attr_delimiter", ";")
    _min_revenue       = (cfg or {}).get("min_revenue_threshold",  0.0)

    # ── Filter to rows with purchase event ───────────────────────────
    # event_list can be "1", "1,2", "2,1,3" — we check if the event id is present
    df_purchases = df.filter(
        F.array_contains(
            F.split(F.col("event_list"), ","),
            F.lit(_purchase_event_id)
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
        for product in product_list_str.split(_prod_delim):
            attrs = product.split(_prod_attr_delim)
            if len(attrs) > _revenue_col_index:
                try:
                    val = attrs[_revenue_col_index].strip()
                    if val:
                        total += float(val)
                except (ValueError, TypeError):
                    pass
        return total

    df_purchases = (
        df_purchases
        .withColumn("revenue", parse_revenue(F.col("product_list")))
        .filter(F.col("revenue") > _min_revenue)
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

def aggregate_results(df_attributed, cfg=None):
    """
    Group by (search_domain, search_keyword), sum revenue.
    Sort by revenue descending.
    Round to 2 decimal places.

    This mirrors _aggregate() in search_keyword_analyzer.py.
    """
    logger.info("Aggregating results...")

    out_cols    = (cfg or {}).get("output_columns",          OUTPUT_COLUMNS)
    min_revenue = (cfg or {}).get("min_revenue_threshold",   0.0)

    df_results = (
        df_attributed
        .groupBy("search_domain", "search_keyword")
        .agg(F.round(F.sum("revenue"), 2).alias("Revenue"))
        .filter(F.col("Revenue") > min_revenue)
        .withColumnRenamed("search_domain",  out_cols[0])
        .withColumnRenamed("search_keyword", out_cols[1])
        .withColumnRenamed("Revenue",        out_cols[2])
        .orderBy(F.col(out_cols[2]).desc())
    )

    result_count  = df_results.count()
    rev_col       = out_cols[2]
    total_revenue = df_results.agg(F.sum(rev_col)).collect()[0][0] or 0.0

    logger.info("=" * 60)
    logger.info("RESULTS — Search Keyword Performance")
    logger.info("=" * 60)
    for row in df_results.collect():
        logger.info("%-25s %-20s $%10.2f",
                    row[out_cols[0]], row[out_cols[1]], row[rev_col])
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

    # Use coalesce(1) so output is a single part file, not 200.
    # Spark always writes a directory; we rename the single part file
    # to the final filename and delete the _tmp staging directory.
    output_dir = f"{output_path.rstrip('/')}/{filename}_tmp"
    final_key_path = f"{output_path.rstrip('/')}/{filename}"

    (
        df_output
        .coalesce(1)
        .write
        .option("delimiter", "\t")
        .option("header", "true")
        .mode("overwrite")
        .csv(output_dir)
    )

    # Rename: copy the single part file to the clean filename, then
    # delete the entire _tmp staging directory (part file + _SUCCESS).
    if output_dir.startswith("s3://"):
        import boto3

        parsed   = urlparse(output_dir)
        bucket   = parsed.netloc
        prefix   = parsed.path.lstrip("/")
        s3       = boto3.client("s3")

        # Find the part file (there is exactly one due to coalesce(1)).
        objects  = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        part_key = next(
            o["Key"] for o in objects.get("Contents", [])
            if "/part-" in o["Key"] and o["Key"].endswith(".csv")
        )

        # Copy part file → final clean filename.
        final_parsed = urlparse(final_key_path)
        final_key    = final_parsed.path.lstrip("/")
        s3.copy_object(
            Bucket=bucket, CopySource={"Bucket": bucket, "Key": part_key},
            Key=final_key,
        )
        logger.info("Output renamed: s3://%s/%s → s3://%s/%s",
                    bucket, part_key, bucket, final_key)

        # Clean up _tmp staging directory.
        for obj in objects.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
        logger.info("Staging directory removed: %s", output_dir)
    else:
        logger.info("Output written to: %s", output_dir)

    return final_key_path, filename


# ══════════════════════════════════════════════════════════════════════
# S3 HELPERS (archive + DQ report)
# ══════════════════════════════════════════════════════════════════════

def _archive_input_s3(input_path):
    """
    Copy input file to archive/ prefix and delete the original.
    Only called on successful pipeline completion — never on failure.

    Preserves the phase sub-folder from the input key so the archive mirrors
    the raw/ layout:
      raw/phase3/hit_data_phase3_20260225_143000.tab
      →  archive/phase3/hit_data_phase3_20260225_143000.tab

    This makes the archive queryable by phase and matches the Lambda path behaviour.
    """
    if not input_path or not input_path.startswith("s3://"):
        logger.info("Skipping archive — input is not an S3 URI: %s", input_path)
        return
    try:
        import boto3
        s3_part        = input_path[len("s3://"):]
        bucket, key    = s3_part.split("/", 1)

        # Reconstruct archive key by replacing the leading "raw/" segment with "archive/".
        # Input key format:  raw/{phase}/{filename}   (3+ path segments)
        # Archive key format: archive/{phase}/{filename}
        # If the key doesn't start with "raw/", fall back to a flat archive/ prefix.
        parts       = key.split("/")
        if parts[0] == "raw" and len(parts) >= 2:
            archive_key = "archive/" + "/".join(parts[1:])   # raw/phase3/f.tab → archive/phase3/f.tab
        else:
            archive_key = f"archive/{parts[-1]}"             # unexpected layout — best-effort

        s3 = boto3.client("s3")
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": key},
            Key=archive_key,
        )
        s3.delete_object(Bucket=bucket, Key=key)
        logger.info("Input archived: s3://%s/%s → s3://%s/%s",
                    bucket, key, bucket, archive_key)
    except Exception as exc:
        logger.warning("Archive failed (non-fatal): %s", exc)


def _write_dq_report_s3(run_id, dq_checks, input_path):
    """
    Write DQ check results as JSON to dq-reports/ in the raw (input) bucket.
    Mirrors what lambda_handler._write_dq_report() does on the Lambda path.
    """
    if not input_path or not input_path.startswith("s3://"):
        return
    try:
        import boto3
        s3_part = input_path[len("s3://"):]
        bucket  = s3_part.split("/")[0]
        orig    = s3_part.split("/")[-1].rsplit(".", 1)[0]
        now     = datetime.now(timezone.utc)
        key     = (
            f"dq-reports/{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{orig}_{run_id[:8]}_dq_report.json"
        )
        report = {
            "run_id":          run_id,
            "execution_date":  now.isoformat(),
            "input_file":      input_path,
            "dq_checks":       dq_checks,
            "overall_passed":  all(c.get("passed", True) for c in dq_checks),
            "engine":          "pyspark",
        }
        boto3.client("s3").put_object(
            Bucket=bucket, Key=key,
            Body=json.dumps(report, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("DQ report written: s3://%s/%s", bucket, key)
    except Exception as exc:
        logger.warning("DQ report write failed (non-fatal): %s", exc)


# ══════════════════════════════════════════════════════════════════════
# MAIN PIPELINE ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════

def run_pipeline(input_path, output_path, env="dev", local=False,
                 config_json=None, archive_input=None):
    """
    Orchestrate the full pipeline. Called by both the CLI entry point
    and by the EMR Serverless job runner.

    Args:
        input_path:   S3 URI or local path to the input tab file.
        output_path:  S3 URI or local path for the output directory.
        env:          Environment name (dev/staging/prod).
        local:        True to run Spark in local mode.
        config_json:  JSON string of config overrides from Lambda (business rules).
        archive_input: S3 URI to archive on success (same as input_path when set).

    Returns a dict with run summary for lineage recording.
    """
    run_id   = str(uuid.uuid4())
    start_ts = time.time()
    cfg      = load_pipeline_config(config_json)

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
        schema_result = validate_schema(df, cfg)
        dq_checks.append(schema_result)

        # ── Step 3: Null validation ───────────────────────────────────
        null_result = validate_nulls(df, total_rows, cfg)
        dq_checks.append(null_result)

        # ── Step 4: Deduplication ─────────────────────────────────────
        df_clean, dedup_result = deduplicate(df, total_rows, cfg)
        dq_checks.append(dedup_result)
        clean_rows = dedup_result["rows_after"]

        # ── Step 5: Build session map ─────────────────────────────────
        df_session_map = build_session_map(df_clean, cfg)

        # ── Step 6: Extract purchases ─────────────────────────────────
        df_purchases = extract_purchase_rows(df_clean, cfg)

        # ── Step 7: Attribute revenue ─────────────────────────────────
        df_attributed = attribute_revenue(df_purchases, df_session_map)

        # ── Step 8: Aggregate ─────────────────────────────────────────
        df_results = aggregate_results(df_attributed, cfg)
        output_rows = df_results.count()

        # ── Step 9: Write output ──────────────────────────────────────
        output_location, filename = write_output(df_results, output_path)

        duration  = time.time() - start_ts
        dq_passed = all(c.get("passed", True) for c in dq_checks)

        # ── Step 10: Archive input (only on success) ──────────────────
        if archive_input:
            _archive_input_s3(archive_input)

        # ── Step 11: Write DQ report ──────────────────────────────────
        _write_dq_report_s3(run_id, dq_checks, input_path)

        summary = {
            "run_id":      run_id,
            "status":      "SUCCESS",
            "input_file":  input_path,
            "output_file": output_location,
            "input_rows":  total_rows,
            "clean_rows":  clean_rows,
            "output_rows": output_rows,
            "dq_passed":   dq_passed,
            "duration_s":  round(duration, 2),
            "engine":      "pyspark",
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
        # Do NOT archive on failure — keep input file recoverable
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
    parser.add_argument(
        "--config-json", default=None,
        help="JSON string of business rule overrides from Lambda config.yaml"
    )
    parser.add_argument(
        "--archive-input", default=None,
        help="S3 URI to archive (copy+delete) on successful completion"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = run_pipeline(
        input_path=args.input,
        output_path=args.output,
        env=args.env,
        local=args.local,
        config_json=args.config_json,
        archive_input=args.archive_input,
    )
    print(json.dumps(result, indent=2))
    sys.exit(0 if result["status"] == "SUCCESS" else 1)

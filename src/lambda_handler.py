"""
lambda_handler.py
=================
AWS Lambda entry point for the Adobe Analytics Search Keyword Performance pipeline.

Architecture — Dual-Path Routing
─────────────────────────────────
Every S3 ObjectCreated event in raw/{phase}/ triggers this handler.

  File < threshold_mb  ──►  Pure Python path  ──►  Process in-Lambda  ──►  S3 processed/
  File >= threshold_mb ──►  EMR Serverless     ──►  PySpark job        ──►  S3 processed/

Thresholds (config-driven, never hardcoded):
  dev  : 50 MB   (config.dev.yaml  → processing.small_file_threshold_mb)
  prod : 1024 MB (config.yaml      → processing.small_file_threshold_mb)

S3 Bucket Layout (raw bucket)
──────────────────────────────
  raw/{phase}/          ← drop zone; S3 event notification fires Lambda here
  archive/{phase}/      ← post-processing; moved here on successful completion
  rejected/{phase}/     ← dead-letter; files that fail naming/duplicate checks
  dq-errors/{Y/M/D}/   ← rows that failed data quality, PII masked before write
  dq-reports/{Y/M/D}/  ← per-run DQ summary JSON (if emit_quality_report=true)
  scripts/              ← PySpark script read by EMR Serverless at runtime
  lambda/               ← deployment package (.zip) uploaded by CI/CD

Output bucket layout (processed bucket)
────────────────────────────────────────
  processed/{phase}/year=YYYY/month=MM/day=DD/{date}_SearchKeywordPerformance.tab
  Hive-style partitioning → Athena/Glue can auto-discover partitions.

Idempotency / Duplicate Detection
───────────────────────────────────
  Filenames MUST carry a full timestamp: <name>_YYYYMMDD_HHMMSS.tab
  Enforcement is configurable (on in prod, off in dev for test files).
  If archive/{phase}/{filename} already exists → duplicate → moved to rejected/.

Author  : Data Engineering Team
Version : 2.0
"""

# ── Standard library ──────────────────────────────────────────────────────────
import csv
import io
import json
import logging
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone

# ── Third-party ───────────────────────────────────────────────────────────────
import boto3

# ── Internal modules ──────────────────────────────────────────────────────────
# sys.path insert ensures Lambda can find sibling packages regardless of how
# the handler entry point is resolved by the Lambda runtime.
sys.path.insert(0, os.path.dirname(__file__))

from config.config_loader                  import load_config
from preprocessing.preprocessing_pipeline  import PreprocessingPipeline
from processing.search_keyword_analyzer    import SearchKeywordAnalyzer
from output.writer                         import OutputWriter
from governance.lineage_tracker            import LineageTracker
from utils.pii_masker                      import mask as mask_pii


# ── Logging ───────────────────────────────────────────────────────────────────
# basicConfig is called once at module load.  Lambda re-uses the execution
# environment across warm invocations, so this runs only on cold start.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("lambda_handler")


# ── Module-level singletons ───────────────────────────────────────────────────
# Config and notifier are loaded once per Lambda container (cold start) and
# reused across warm invocations. This avoids repeated S3/SSM calls per event.
_cfg      = None   # Loaded config object (see _get_config)
_notifier = None   # SNS notifier stub (see _get_notifier)


# ─────────────────────────────────────────────────────────────────────────────
# SNS Notifier stub
# ─────────────────────────────────────────────────────────────────────────────

class SNSNotifier:
    """
    Lightweight alert notifier.

    The real SNS alerts module is not bundled in this Lambda package, so this
    stub fulfils the interface by logging to CloudWatch instead.  When the
    alerts module is available, replace this class with the real import.

    All pipeline stages call notifier.alert_failure() on error so that the
    interface is uniform whether SNS is wired up or not.
    """

    def __init__(self, cfg):
        # Read SNS topic ARN from config; None means alerts are log-only.
        self._topic = cfg.get("alerts.sns_topic_arn", default=None)

    def alert_failure(self, run_id: str, stage: str, error: str, input_file: str):
        """Log a pipeline failure. Replace body with real SNS publish when ready."""
        logger.warning(
            "ALERT (SNS stub) run_id=%s stage=%s input=%s error=%s",
            run_id, stage, input_file, error,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Singleton helpers
# ─────────────────────────────────────────────────────────────────────────────

def _get_config():
    """
    Return the singleton config object, loading it on the first call.

    APP_ENV controls which config overlay is applied:
      dev     → config.dev.yaml   (low thresholds, enforcement off)
      staging → config.staging.yaml
      prod    → config.yaml        (full thresholds, all checks on)
    """
    global _cfg
    if _cfg is None:
        _cfg = load_config(env=os.environ.get("APP_ENV", "dev"))
    return _cfg


def _get_notifier(cfg):
    """Return the singleton SNSNotifier, constructing it on first call."""
    global _notifier
    if _notifier is None:
        _notifier = SNSNotifier(cfg)
    return _notifier


# ─────────────────────────────────────────────────────────────────────────────
# Lambda entry point
# ─────────────────────────────────────────────────────────────────────────────

def handler(event, context=None):
    """
    Main Lambda handler — called by the Lambda runtime for every S3 event.

    Execution flow:
      1. Parse S3 event  →  extract bucket + key
      2. Guard checks    →  file type, naming convention, duplicate detection
      3. Route decision  →  EMR (large) or Pure Python (small)
      4. Pure Python path:
           a. Download from S3
           b. Preprocess + DQ validation
           c. Analyze (search keyword aggregation)
           d. Write output to S3 (Hive-partitioned)
           e. Archive input
           f. Record lineage
      5. Return JSON response  →  200 (complete), 202 (EMR submitted),
                                   400 (bad filename), 409 (duplicate), 500 (error)

    Parameters
    ----------
    event   : dict   AWS event payload (S3 ObjectCreated or direct invocation dict)
    context : object Lambda context object; may be None in unit tests
    """
    run_id   = str(uuid.uuid4())   # Unique ID for this pipeline run (used in all logs)
    start_ts = time.time()          # Wall-clock start for duration tracking
    cfg      = _get_config()
    lineage  = LineageTracker(cfg)
    notifier = _get_notifier(cfg)

    logger.info("=== SKP Pipeline Start | run_id=%s ===", run_id)

    # ── Step 1: Parse the incoming S3 event ───────────────────────────────────
    try:
        bucket, key = _parse_event(event)
        logger.info("Input: s3://%s/%s", bucket, key)
    except Exception as exc:
        logger.error("Event parsing failed: %s", exc)
        notifier.alert_failure(run_id=run_id, stage="event_parsing",
                               error=str(exc), input_file="unknown")
        return _error_response(500, f"Event parsing failed: {exc}")

    # ── Step 2a: File-type guard ───────────────────────────────────────────────
    # Only .tab files carry pipeline data.  .keep files (S3 folder markers) and
    # any other objects that slip through the S3 notification suffix filter are
    # silently skipped here as a second line of defence.
    if not key.lower().endswith(".tab"):
        logger.info("Skipping non-.tab object: %s", key)
        return {"statusCode": 200,
                "body": json.dumps({"message": "Skipped non-tab file", "key": key})}

    # Extract the bare filename once; reused in all subsequent checks and logs.
    input_filename = key.split("/")[-1]

    # Derive the phase label (e.g. "phase1") from the S3 key path.
    # Used as the partition folder in processed/, archive/, and rejected/.
    phase_folder = _get_phase_folder(key)

    s3_client = boto3.client("s3")
    input_uri = f"s3://{bucket}/{key}"

    # ── Step 2b: Naming convention enforcement ────────────────────────────────
    # Production requires a full timestamp in every filename so that:
    #   • Daily feeds, hourly feeds, and ad-hoc deliveries all produce unique names
    #   • The archive is a clean, time-ordered ledger with no silent overwrites
    #   • Duplicate detection (below) can rely on filename equality
    # This check is OFF in dev (config.dev.yaml enforce_filename_date: false) so
    # test files without timestamps still work during development.
    enforce_ts = cfg.get("data_quality.enforce_filename_date", default=False)
    if enforce_ts and not _has_date_in_filename(input_filename):
        msg = (f"Rejected: '{input_filename}' lacks a full timestamp. "
               f"Source must deliver files as <name>_YYYYMMDD_HHMMSS.tab "
               f"e.g. hit_data_phase1_20260225_143000.tab")
        logger.warning(msg)
        _reject_input(s3_client, bucket, key, phase_folder,
                      reason="missing_timestamp_in_filename")
        return {"statusCode": 400, "body": json.dumps({"message": msg, "key": key})}

    # ── Step 2c: Duplicate detection ──────────────────────────────────────────
    # If the exact filename already exists in archive/{phase}/, this event is a
    # duplicate delivery (S3 at-least-once, operator re-drop, or upstream retry).
    # The file is moved to rejected/ so the operator can audit it; it is never
    # silently discarded.
    if _is_already_processed(s3_client, bucket, key, phase_folder):
        msg = (f"Duplicate rejected: '{input_filename}' already exists in "
               f"archive/{phase_folder}/. Re-deliver with a new timestamp to reprocess.")
        logger.warning(msg)
        _reject_input(s3_client, bucket, key, phase_folder, reason="duplicate")
        return {"statusCode": 409, "body": json.dumps({"message": msg, "key": key})}

    # ── Step 3: Routing decision ───────────────────────────────────────────────
    # Fetch content length from S3 metadata (no download required at this point).
    threshold_mb = cfg.get("processing.small_file_threshold_mb", default=50)
    try:
        head         = s3_client.head_object(Bucket=bucket, Key=key)
        file_size_b  = head["ContentLength"]                   # bytes
        file_size_mb = file_size_b / (1024 * 1024)             # megabytes (display)
        logger.info("File size: %.1f MB | Routing threshold: %d MB",
                    file_size_mb, threshold_mb)
    except Exception as exc:
        logger.error("S3 head_object failed: %s", exc)
        notifier.alert_failure(run_id=run_id, stage="s3_head",
                               error=str(exc), input_file=input_uri)
        return _error_response(500, f"Failed to read file metadata: {exc}")

    # Large file → hand off to EMR Serverless immediately.
    # The Spark job owns archiving on its side (archive on success, leave in raw on failure).
    if file_size_b >= threshold_mb * 1024 * 1024:
        logger.info("ROUTE → EMR Serverless (%.1f MB >= %d MB threshold)",
                    file_size_mb, threshold_mb)
        return _handoff_to_emr(input_uri, run_id, cfg, lineage, notifier,
                                start_ts, s3_client=s3_client, bucket=bucket, key=key,
                                reason="file_size_threshold", phase_folder=phase_folder)

    # Safety check: if Lambda is close to its timeout, offload to EMR rather
    # than risk a mid-processing timeout that would leave data in a partial state.
    if _is_near_timeout(context):
        logger.warning("ROUTE → EMR Serverless (Lambda timeout safety pre-download)")
        return _handoff_to_emr(input_uri, run_id, cfg, lineage, notifier,
                                start_ts, s3_client=s3_client, bucket=bucket, key=key,
                                reason="timeout_safety", phase_folder=phase_folder)

    # ── Step 4a: Download ──────────────────────────────────────────────────────
    logger.info("ROUTE → Pure Python (%.1f MB < %d MB threshold)",
                file_size_mb, threshold_mb)
    local_input = f"/tmp/{run_id}_input.tab"   # /tmp is the only writable path in Lambda

    try:
        s3_client.download_file(bucket, key, local_input)
        logger.info("Downloaded %.1f MB from S3 to %s", file_size_mb, local_input)
    except Exception as exc:
        logger.error("S3 download failed: %s", exc)
        notifier.alert_failure(run_id=run_id, stage="s3_download",
                               error=str(exc), input_file=input_uri)
        lineage.record(run_id=run_id, input_file=input_uri,
                       dq_passed=False, error=str(exc),
                       duration_seconds=time.time() - start_ts)
        return _error_response(500, f"S3 download failed: {exc}")

    # Post-download timeout safety — download itself may have consumed significant time.
    if _is_near_timeout(context):
        logger.warning("ROUTE → EMR Serverless (Lambda timeout safety post-download)")
        return _handoff_to_emr(input_uri, run_id, cfg, lineage, notifier,
                                start_ts, s3_client=s3_client, bucket=bucket, key=key,
                                reason="timeout_safety_post_download",
                                phase_folder=phase_folder)

    # ── Step 4b: Preprocess + Data Quality ────────────────────────────────────
    try:
        with open(local_input, encoding="utf-8", errors="replace") as fh:
            raw_content = fh.read()

        # PreprocessingPipeline returns a 3-tuple:
        #   rows       — clean rows ready for analysis (list of dicts)
        #   error_rows — rows that failed DQ checks   (list of dicts with _error_reason)
        #   dq_report  — summary dict: overall_passed, rows_in, error_count, …
        pipeline                    = PreprocessingPipeline(cfg)
        rows, error_rows, dq_report = pipeline.run(raw_content)

        dq_passed   = dq_report.get("overall_passed", True)
        input_rows  = dq_report.get("rows_in", 0)
        error_count = dq_report.get("error_count", 0)
        logger.info("Preprocessing complete — clean: %d rows | errors: %d rows",
                    len(rows), error_count)
    except Exception as exc:
        logger.error("Preprocessing failed: %s", exc)
        notifier.alert_failure(run_id=run_id, stage="preprocessing",
                               error=str(exc), input_file=input_uri)
        lineage.record(run_id=run_id, input_file=input_uri,
                       dq_passed=False, error=str(exc),
                       duration_seconds=time.time() - start_ts)
        return _error_response(500, f"Preprocessing failed: {exc}")

    # Write DQ artefacts to S3 (both are non-fatal — pipeline continues either way).
    dq_error_s3_uri = None
    if error_rows:
        # PII is masked inside _write_dq_errors before any data reaches S3.
        dq_error_s3_uri = _write_dq_errors(s3_client, bucket, key, error_rows,
                                            run_id, cfg=cfg)
    if cfg.get("data_quality.emit_quality_report", default=False):
        # Full DQ report JSON: row counts, null rates, duplicate stats, schema diffs.
        _write_dq_report(s3_client, bucket, key, run_id, dq_report)

    # Post-preprocessing timeout safety check.
    if _is_near_timeout(context):
        logger.warning("ROUTE → EMR Serverless (Lambda timeout safety post-preprocessing)")
        return _handoff_to_emr(input_uri, run_id, cfg, lineage, notifier,
                                start_ts, s3_client=s3_client, bucket=bucket, key=key,
                                reason="timeout_safety_post_preprocessing",
                                phase_folder=phase_folder)

    # ── Step 4c: Search keyword analysis ──────────────────────────────────────
    try:
        analyzer = SearchKeywordAnalyzer(cfg)
        results  = analyzer.analyze(rows)
        logger.info("Analysis complete — %d revenue rows produced", len(results))
    except Exception as exc:
        logger.error("Analysis failed: %s", exc)
        notifier.alert_failure(run_id=run_id, stage="analysis",
                               error=str(exc), input_file=input_uri)
        lineage.record(run_id=run_id, input_file=input_uri, input_rows=input_rows,
                       dq_passed=dq_passed, error=str(exc),
                       duration_seconds=time.time() - start_ts)
        return _error_response(500, f"Analysis failed: {exc}")

    # ── Step 4d: Write output to S3 ───────────────────────────────────────────
    try:
        writer        = OutputWriter(cfg)
        exec_date     = datetime.now(timezone.utc)
        output_fname  = writer.get_filename(exec_date)       # e.g. 2026-02-25_SearchKeywordPerformance.tab
        local_output  = f"/tmp/{run_id}_{output_fname}"
        writer.write_local(results, local_output)

        # Hive-style partitioning: phase-first so Athena/Glue can partition by phase.
        # Path: processed/{phase}/year=YYYY/month=MM/day=DD/{filename}
        s3_output_key = (
            f"processed/{phase_folder}"
            f"/year={exec_date.year}"
            f"/month={exec_date.month:02d}"
            f"/day={exec_date.day:02d}"
            f"/{output_fname}"
        )
        s3_output_uri = writer.upload_to_s3(local_output, s3_output_key)
        logger.info("Output written: %s", s3_output_uri)
    except Exception as exc:
        logger.error("Output write failed: %s", exc)
        notifier.alert_failure(run_id=run_id, stage="output_write",
                               error=str(exc), input_file=input_uri)
        lineage.record(run_id=run_id, input_file=input_uri, input_rows=input_rows,
                       dq_passed=dq_passed, error=str(exc),
                       duration_seconds=time.time() - start_ts)
        return _error_response(500, f"Output write failed: {exc}")

    # ── Step 4e: Archive input ─────────────────────────────────────────────────
    # Move the processed input to archive/{phase}/ so:
    #   • The raw/ folder stays clean (only unprocessed files live there)
    #   • The archive acts as the "processed file registry" for duplicate detection
    #   • Files are retained for replay / audit without cluttering the drop zone
    _archive_input(s3_client, bucket, key, phase_folder=phase_folder)

    # ── Step 4f: Record lineage ────────────────────────────────────────────────
    duration = time.time() - start_ts
    lineage.record(
        run_id=run_id,        input_file=input_uri,
        input_rows=input_rows, output_file=s3_output_uri or local_output,
        output_rows=len(results), engine_used="pure_python",
        dq_passed=dq_passed,  duration_seconds=duration,
    )

    # ── Final result log ───────────────────────────────────────────────────────
    col0, col1, col2 = cfg.get(
        "output.columns",
        default=["Search Engine Domain", "Search Keyword", "Revenue"],
    )
    logger.info("=" * 60)
    logger.info("RESULTS — Search Keyword Performance")
    logger.info("=" * 60)
    if not results:
        logger.info("No qualifying search engine revenue found.")
    else:
        logger.info("%-25s %-25s %10s", col0, col1, col2)
        logger.info("-" * 60)
        for row in results:
            logger.info("%-25s %-25s %10s",
                        row[col0], row[col1], f"${row[col2]:.2f}")
    logger.info("=" * 60)
    logger.info("Engine: pure_python | Duration: %.2fs | Run ID: %s", duration, run_id)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message":       "Pipeline complete",
            "run_id":        run_id,
            "input_file":    input_uri,
            "output_file":   s3_output_uri,
            "rows_in":       input_rows,
            "rows_out":      len(results),
            "error_rows":    error_count,
            "dq_error_file": dq_error_s3_uri,
            "dq_passed":     dq_passed,
            "duration_s":    round(duration, 2),
            "engine":        "pure_python",
        }),
    }


# ─────────────────────────────────────────────────────────────────────────────
# EMR Serverless handoff
# ─────────────────────────────────────────────────────────────────────────────

def _handoff_to_emr(input_uri, run_id, cfg, lineage, notifier, start_ts,
                    s3_client=None, bucket=None, key=None,
                    reason="", phase_folder="other"):
    """
    Submit a PySpark job to EMR Serverless and return immediately (async).

    Intentionally does NOT archive the input file here. The Spark job archives
    on successful completion via --archive-input. This design ensures:
      • No data loss if the EMR job fails (file stays in raw/ for retry)
      • Archive is atomic with successful processing (no orphaned archives)

    Returns a 202 Accepted response if submission succeeds, 500 on failure.
    """
    # Log the routing reason before the call so it appears in logs even if
    # the submission blocks or raises an exception.
    if reason:
        logger.info("EMR handoff reason: %s", reason)

    result = _trigger_emr_job(input_uri, run_id, cfg, notifier,
                              phase_folder=phase_folder)

    # Record lineage immediately on handoff (job outcome tracked separately in EMR logs).
    lineage.record(
        run_id=run_id, input_file=input_uri,
        engine_used="pyspark_emr", dq_passed=True,
        duration_seconds=time.time() - start_ts,
    )
    return result


def _trigger_emr_job(input_s3_uri, run_id, cfg, notifier, phase_folder="other"):
    """
    Construct and submit the EMR Serverless StartJobRun API call.

    All business rules are serialised to JSON and passed as --config-json so
    the Spark script reads from the same config as Lambda, not hardcoded constants.
    The Spark script also receives --archive-input so it can archive the input
    file after successful processing.

    Environment variables required:
      EMR_SERVERLESS_APP_ID   — EMR Serverless application ID
      EMR_EXECUTION_ROLE_ARN  — IAM role ARN the Spark job runs as
      RAW_BUCKET              — S3 bucket containing scripts/ and raw/ prefixes
      PROCESSED_BUCKET        — S3 bucket for output
      LOGS_BUCKET             — S3 bucket for EMR driver/executor logs
    """
    app_id           = os.environ.get("EMR_SERVERLESS_APP_ID")
    exec_role_arn    = os.environ.get("EMR_EXECUTION_ROLE_ARN")
    raw_bucket       = os.environ.get("RAW_BUCKET", "")
    processed_bucket = os.environ.get("PROCESSED_BUCKET", "")
    logs_bucket      = os.environ.get("LOGS_BUCKET", "")
    region           = os.environ.get("AWS_REGION", "us-east-1")

    # Both variables must be set for EMR submission to proceed.
    if not app_id or not exec_role_arn:
        msg = "EMR environment variables not set (EMR_SERVERLESS_APP_ID / EMR_EXECUTION_ROLE_ARN)."
        logger.error(msg)
        notifier.alert_failure(run_id=run_id, stage="emr_submission",
                               error=msg, input_file=input_s3_uri)
        return _error_response(500, msg)

    now = datetime.now(timezone.utc)

    # Output path mirrors the Lambda Pure Python path: phase-first Hive partitioning.
    output_path = (
        f"s3://{processed_bucket}/processed/{phase_folder}"
        f"/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
    )
    script_uri = f"s3://{raw_bucket}/scripts/spark_skp_pipeline.py"
    job_name   = f"skp-auto-{run_id[:8]}"

    # Serialise all business rules from config so Spark uses the same values as
    # Lambda — single source of truth, no drift between engines.
    config_dict = {
        "search_engine_domains":      cfg.get("business_rules.search_engine_domains",
                                              default=["google.com", "yahoo.com",
                                                       "bing.com", "ask.com",
                                                       "aol.com", "msn.com"]),
        "keyword_params":             cfg.get("business_rules.keyword_query_params",
                                              default=["q", "p", "query", "text", "s", "qs"]),
        "purchase_event_id":          cfg.get("business_rules.purchase_event_id",      default="1"),
        "revenue_col_index":          cfg.get("business_rules.revenue_column_index",    default=3),
        "product_delimiter":          cfg.get("business_rules.product_delimiter",       default=","),
        "product_attr_delimiter":     cfg.get("business_rules.product_attr_delimiter",  default=";"),
        "event_delimiter":            cfg.get("business_rules.event_delimiter",         default=","),
        "keyword_normalize_case":     cfg.get("business_rules.keyword_normalize_case",  default=True),
        "min_revenue_threshold":      cfg.get("business_rules.min_revenue_threshold",   default=0.0),
        "required_columns":           cfg.get("input.required_columns",                 default=[]),
        "critical_columns":           cfg.get("data_quality.critical_columns",
                                              default=["hit_time_gmt", "ip", "referrer"]),
        "dedup_key_columns":          cfg.get("data_quality.duplicate_key_columns",
                                              default=["hit_time_gmt", "ip", "page_url"]),
        "null_warning_threshold_pct": cfg.get("data_quality.null_warning_threshold_pct", default=50),
        "output_columns":             cfg.get("output.columns",
                                              default=["Search Engine Domain",
                                                       "Search Keyword", "Revenue"]),
    }

    logger.info("Submitting EMR job '%s' | Input: %s | Output: %s",
                job_name, input_s3_uri, output_path)

    try:
        emr_client = boto3.client("emr-serverless", region_name=region)
        response   = emr_client.start_job_run(
            applicationId=app_id,
            executionRoleArn=exec_role_arn,
            name=job_name,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": script_uri,
                    "entryPointArguments": [
                        "--input",         input_s3_uri,
                        "--output",        output_path,
                        "--env",           os.environ.get("APP_ENV", "dev"),
                        "--config-json",   json.dumps(config_dict),
                        "--archive-input", input_s3_uri,  # Spark archives raw file on success
                    ],
                    # Spark resource settings — tuned for 50MB–1GB files.
                    # Adjust executor.cores and memory for larger datasets.
                    "sparkSubmitParameters": (
                        "--conf spark.executor.cores=4 "
                        "--conf spark.executor.memory=8g "
                        "--conf spark.driver.cores=2 "
                        "--conf spark.driver.memory=4g "
                        "--conf spark.sql.adaptive.enabled=true"
                    ),
                }
            },
            # Attach EMR logs to S3 if a logs bucket is configured.
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{logs_bucket}/emr-logs/"
                    }
                }
            } if logs_bucket else {},
            # Tags enable cost allocation and resource filtering in AWS Cost Explorer.
            tags={"RunId": run_id, "ManagedBy": "skp-lambda"},
        )
        job_run_id = response["jobRunId"]
        logger.info("EMR job submitted successfully. JobRunId: %s", job_run_id)
        return {
            "statusCode": 202,
            "body": json.dumps({
                "message":    "Large file routed to EMR Serverless",
                "run_id":     run_id,
                "job_run_id": job_run_id,
                "input_file": input_s3_uri,
                "engine":     "pyspark_emr",
                "status":     "SUBMITTED",
            }),
        }
    except Exception as exc:
        logger.error("EMR submission failed: %s", exc)
        notifier.alert_failure(run_id=run_id, stage="emr_submission",
                               error=str(exc), input_file=input_s3_uri)
        return _error_response(500, f"EMR submission failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Data Quality artefact writers
# ─────────────────────────────────────────────────────────────────────────────

def _write_dq_errors(s3_client, bucket, key, error_rows, run_id, cfg=None):
    """
    Write DQ error rows to S3 as a CSV, with PII columns masked before upload.

    PII masking is mandatory — ip and user_agent must never appear in plain
    text outside the Lambda execution environment. The masker applies SHA-256
    hashing so the values are auditable but not reversible.

    S3 path: dq-errors/YYYY/MM/DD/<stem>_<run_id[:8]>_errors.csv

    Returns the S3 URI of the written file, or None if the write fails.
    Failure is non-fatal — the pipeline continues and the error is logged.
    """
    try:
        now       = datetime.now(timezone.utc)
        stem      = key.split("/")[-1].rsplit(".", 1)[0]    # filename without extension
        error_key = (
            f"dq-errors/{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{stem}_{run_id[:8]}_errors.csv"
        )

        # Determine which columns contain PII (configurable; defaults to ip + user_agent).
        pii_cols = set(
            cfg.get("data_quality.pii_columns", default=["ip", "user_agent"])
            if cfg else ["ip", "user_agent"]
        )

        # Mask PII in-place on a copy — never mutate the original rows.
        masked_rows = []
        for row in error_rows:
            masked = dict(row)
            for col in pii_cols:
                if masked.get(col):
                    masked[col] = mask_pii(str(masked[col]))
            masked_rows.append(masked)

        # Ensure the _error_reason column (added by preprocessing) is present
        # in the header even if the first row happens to omit it.
        fieldnames = list(masked_rows[0].keys()) if masked_rows else []
        if "_error_reason" not in fieldnames:
            fieldnames.append("_error_reason")

        buf = io.StringIO()
        csv.DictWriter(buf, fieldnames=fieldnames,
                       extrasaction="ignore").writeheader()
        csv.DictWriter(buf, fieldnames=fieldnames,
                       extrasaction="ignore").writerows(masked_rows)

        s3_client.put_object(
            Bucket=bucket, Key=error_key,
            Body=buf.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )
        uri = f"s3://{bucket}/{error_key}"
        logger.info("DQ errors written: %s (%d rows)", uri, len(masked_rows))
        return uri
    except Exception as exc:
        logger.warning("Failed to write DQ errors (non-fatal): %s", exc)
        return None


def _write_dq_report(s3_client, bucket, key, run_id, dq_report):
    """
    Write the per-run DQ summary report as JSON to S3.

    The report contains aggregate statistics (row counts, null rates, schema
    diffs, duplicate counts, overall pass/fail) and is useful for dashboards
    and trend analysis without accessing the full error CSV.

    S3 path: dq-reports/YYYY/MM/DD/<stem>_<run_id[:8]>_dq_report.json

    Failure is non-fatal — the pipeline continues and the error is logged.
    """
    try:
        now        = datetime.now(timezone.utc)
        stem       = key.split("/")[-1].rsplit(".", 1)[0]
        report_key = (
            f"dq-reports/{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{stem}_{run_id[:8]}_dq_report.json"
        )
        s3_client.put_object(
            Bucket=bucket, Key=report_key,
            Body=json.dumps(dq_report, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("DQ report written: s3://%s/%s", bucket, report_key)
    except Exception as exc:
        logger.warning("Failed to write DQ report (non-fatal): %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# S3 object lifecycle helpers
# ─────────────────────────────────────────────────────────────────────────────

def _move_s3_object(s3_client, bucket, src_key, dst_key, metadata=None):
    """
    Atomically move an S3 object by copying to the destination then deleting
    the source.  S3 does not have a native rename/move operation.

    metadata : dict | None
        If provided, the destination object is written with this metadata and
        MetadataDirective=REPLACE (overwrites any metadata on the source).

    Raises any S3 exception so callers can decide whether the failure is fatal.
    """
    copy_kwargs = {
        "Bucket":     bucket,
        "CopySource": {"Bucket": bucket, "Key": src_key},
        "Key":        dst_key,
    }
    if metadata:
        copy_kwargs["Metadata"]          = metadata
        copy_kwargs["MetadataDirective"] = "REPLACE"

    s3_client.copy_object(**copy_kwargs)
    s3_client.delete_object(Bucket=bucket, Key=src_key)


def _archive_input(s3_client, bucket, key, phase_folder="other"):
    """
    Move the successfully processed input file to archive/{phase_folder}/.

    The archive serves a dual purpose:
      1. Keeps raw/ clean — only unprocessed files live there
      2. Acts as the processed-file registry for duplicate detection
         (_is_already_processed checks for the file's presence here)

    Failure is non-fatal — a warning is logged and the pipeline response is
    still returned to the caller.  The file remains in raw/ in this case and
    will be caught as a duplicate on the next delivery.
    """
    filename    = key.split("/")[-1]
    archive_key = f"archive/{phase_folder}/{filename}"
    try:
        _move_s3_object(s3_client, bucket, key, archive_key)
        logger.info("Archived: s3://%s/%s → s3://%s/%s",
                    bucket, key, bucket, archive_key)
    except Exception as exc:
        logger.warning("Archive failed (non-fatal): %s", exc)


def _reject_input(s3_client, bucket, key, phase_folder, reason="duplicate"):
    """
    Move a rejected file to the dead-letter prefix rejected/{phase}/year=…/month=…/day=…/

    Hive-style partitioning is used (matching processed/) so rejections can be
    queried by Athena alongside processed outputs for auditing.

    The rejection reason is stored as S3 object metadata so operators can
    understand why a file was rejected without opening it.

    Reasons used by this pipeline:
      duplicate                    — same filename already in archive/
      missing_timestamp_in_filename — source violated naming convention

    Failure is non-fatal — if the reject move fails, the file stays in raw/
    and will be re-evaluated on the next S3 event (at-least-once delivery).
    """
    now        = datetime.now(timezone.utc)
    filename   = key.split("/")[-1]
    reject_key = (
        f"rejected/{phase_folder}"
        f"/year={now.year}/month={now.month:02d}/day={now.day:02d}"
        f"/{filename}"
    )
    try:
        _move_s3_object(
            s3_client, bucket, key, reject_key,
            metadata={"rejection_reason": reason, "original_key": key},
        )
        logger.warning("Rejected (%s): s3://%s/%s → s3://%s/%s",
                       reason, bucket, key, bucket, reject_key)
    except Exception as exc:
        logger.warning("Reject move failed (non-fatal): %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Validation helpers
# ─────────────────────────────────────────────────────────────────────────────

def _has_date_in_filename(filename: str) -> bool:
    """
    Return True if the filename contains a full timestamp in YYYYMMDD_HHMMSS format.

    A full timestamp (not just a date) is required so the naming convention is
    feed-frequency agnostic — it works for daily, hourly, or any other cadence
    without needing a convention change later.

    Valid   : hit_data_phase1_20260225_143000.tab   (_YYYYMMDD_HHMMSS)
    Invalid : hit_data_phase1_20260225.tab           (date-only — unsafe for hourly)
    Invalid : hit_data_phase1_crawl.tab              (no timestamp at all)
    """
    return bool(re.search(r"_\d{8}_\d{6}\.", filename))


def _is_already_processed(s3_client, bucket, key, phase_folder: str) -> bool:
    """
    Return True if the exact filename already exists in archive/{phase_folder}/.

    With timestamps enforced in filenames, an identical filename means the same
    logical delivery (same phase, same timestamp) has already been processed.
    This guards against:
      • S3 at-least-once delivery firing the same event twice
      • Operators accidentally re-dropping a file that was already processed

    Returns False for any unexpected S3 error other than 404/NoSuchKey so as
    not to silently block legitimate deliveries.
    """
    archive_key = f"archive/{phase_folder}/{key.split('/')[-1]}"
    try:
        s3_client.head_object(Bucket=bucket, Key=archive_key)
        return True   # Object exists → already processed
    except s3_client.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False   # Object not found → new delivery, proceed
        raise             # Unexpected error — re-raise for caller to handle


def _get_phase_folder(key: str) -> str:
    """
    Derive the phase partition label (e.g. "phase1") from the S3 object key.

    Detection order:
      1. Path structure (preferred) — raw/phase1/filename.tab → "phase1"
         This is the canonical production structure where the source deposits
         files into the correct phase subfolder.
      2. Filename keyword fallback — used only for legacy/test files that were
         dropped at the root raw/ prefix before phase subfolders were introduced.

    Returns "other" if no phase can be determined.
    """
    # Path-based detection: iterate key segments and match phaseN pattern.
    for part in key.split("/"):
        if part.startswith("phase") and part[5:].isdigit():
            return part

    # Filename keyword fallback (test/legacy files only).
    k = key.lower()
    if   "phase1" in k or "crawl"  in k: return "phase1"
    elif "phase2" in k or "walk"   in k: return "phase2"
    elif "phase3" in k or "jog"    in k: return "phase3"
    elif "phase4" in k or "run"    in k: return "phase4"
    elif "phase5" in k or "sprint" in k: return "phase5"
    return "other"


def _is_near_timeout(context, threshold_ms: int = 60_000) -> bool:
    """
    Return True if the Lambda execution has less than threshold_ms milliseconds
    remaining before the configured timeout.

    Used at multiple checkpoints in the Pure Python path to catch cases where
    a large download or slow preprocessing would otherwise cause Lambda to be
    killed mid-execution, leaving data in a partial state.

    Default threshold: 60 seconds — enough time for EMR submission API call.
    """
    if context and hasattr(context, "get_remaining_time_in_millis"):
        return context.get_remaining_time_in_millis() < threshold_ms
    return False   # No context (unit tests / direct invocation) — never near timeout


def _parse_event(event: dict) -> tuple:
    """
    Extract the S3 bucket name and object key from the Lambda event payload.

    Supports two formats:
      1. S3 ObjectCreated notification (standard production trigger):
           {"Records": [{"s3": {"bucket": {"name": "…"}, "object": {"key": "…"}}}]}
      2. Direct invocation dict (used in smoke tests and local __main__ runs):
           {"bucket": "…", "key": "…"}

    Raises ValueError if neither format is recognised.
    """
    if "Records" in event and event["Records"]:
        rec = event["Records"][0]
        return rec["s3"]["bucket"]["name"], rec["s3"]["object"]["key"]
    if "bucket" in event and "key" in event:
        return event["bucket"], event["key"]
    raise ValueError(
        f"Unrecognised event format. Expected S3 Records or {{bucket, key}} dict. "
        f"Got keys: {list(event.keys())}"
    )


def _error_response(status_code: int, message: str) -> dict:
    """
    Build a standardised error response dict for Lambda return.

    All error paths return this shape so callers (Step Functions, API Gateway,
    smoke tests) can reliably check statusCode and parse the error field.
    """
    logger.error("Pipeline error [%d]: %s", status_code, message)
    return {"statusCode": status_code, "body": json.dumps({"error": message})}


# ─────────────────────────────────────────────────────────────────────────────
# Local development runner
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    """
    Run the pipeline locally against a file on disk (no real AWS calls).

    Usage:
        python lambda_handler.py [path/to/hit_data.tab]

    All S3 calls are intercepted by _LocalS3 which reads/writes local files.
    Useful for rapid iteration without deploying to Lambda.
    """
    import shutil
    from unittest.mock import patch

    input_path = sys.argv[1] if len(sys.argv) > 1 else "tests/data/hit_data.tab"

    class _LocalS3:
        """Minimal S3 stub for local execution — replaces boto3.client("s3")."""

        # Simulate the botocore ClientError class hierarchy so _is_already_processed
        # can catch s3_client.exceptions.ClientError without a real boto3 session.
        class exceptions:
            class ClientError(Exception):
                def __init__(self, msg="", code="404"):
                    super().__init__(msg)
                    self.response = {"Error": {"Code": code}}

        def head_object(self, Bucket, Key):
            # Return real file size for the routing threshold check.
            if os.path.exists(Key):
                return {"ContentLength": os.path.getsize(Key)}
            # Simulate a 404 for archive/duplicate checks.
            raise self.exceptions.ClientError(f"{Key} not found", code="404")

        def download_file(self, bucket, key, dest):
            shutil.copy(key, dest)

        def put_object(self, **kwargs):    pass   # DQ errors / reports: no-op locally
        def copy_object(self, **kwargs):   pass   # Archive / reject: no-op locally
        def delete_object(self, **kwargs): pass   # Delete after copy: no-op locally

    os.environ.setdefault("PROCESSED_BUCKET", "")
    os.environ.setdefault("LOGS_BUCKET",      "")
    os.environ.setdefault("APP_ENV",          "dev")

    with patch("boto3.client", return_value=_LocalS3()):
        result = handler({"bucket": "local", "key": input_path})
        print("\n" + "=" * 60)
        print("Lambda response:")
        print(json.dumps(json.loads(result["body"]), indent=2))

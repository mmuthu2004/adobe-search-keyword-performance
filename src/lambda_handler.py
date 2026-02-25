"""
lambda_handler.py
=================
AWS Lambda entry point — dual-path routing with context-aware handoff.
"""
import csv
import io
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import boto3

sys.path.insert(0, os.path.dirname(__file__))

from config.config_loader                  import load_config
from preprocessing.preprocessing_pipeline  import PreprocessingPipeline
from processing.search_keyword_analyzer    import SearchKeywordAnalyzer
from output.writer                         import OutputWriter
from governance.lineage_tracker            import LineageTracker
from utils.pii_masker                      import mask as mask_pii


class SNSNotifier:
    """Stub notifier — alerts module not present; failures are logged only."""
    def __init__(self, cfg):
        self._topic = cfg.get("alerts.sns_topic_arn", default=None)

    def alert_failure(self, run_id, stage, error, input_file):
        logger.warning(
            "ALERT (SNS stub) run_id=%s stage=%s input=%s error=%s",
            run_id, stage, input_file, error,
        )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger("lambda_handler")

_cfg      = None
_notifier = None


def _get_config():
    global _cfg
    if _cfg is None:
        _cfg = load_config(env=os.environ.get("APP_ENV", "dev"))
    return _cfg


def _get_phase_folder(key: str) -> str:
    """
    Derive phase partition label from the S3 object key.
    Checks the folder path first (raw/phase1/file.tab),
    then falls back to filename pattern matching.
    Expected structure: raw/{phase}/filename.tab
    """
    # Primary: read from path structure raw/phase1/filename
    parts = key.split("/")
    for part in parts:
        if part.startswith("phase") and part[5:].isdigit():
            return part
    # Fallback: detect from filename keywords
    k = key.lower()
    if   "phase1" in k or "crawl"  in k: return "phase1"
    elif "phase2" in k or "walk"   in k: return "phase2"
    elif "phase3" in k or "jog"    in k: return "phase3"
    elif "phase4" in k or "run"    in k: return "phase4"
    elif "phase5" in k or "sprint" in k: return "phase5"
    return "other"


def _get_notifier(cfg):
    global _notifier
    if _notifier is None:
        _notifier = SNSNotifier(cfg)
    return _notifier


def handler(event, context=None):
    run_id   = str(uuid.uuid4())
    start_ts = time.time()
    cfg      = _get_config()
    lineage  = LineageTracker(cfg)
    notifier = _get_notifier(cfg)

    logger.info("=== SKP Pipeline Start | run_id=%s ===", run_id)

    try:
        bucket, key = _parse_event(event)
        logger.info("Input: s3://%s/%s", bucket, key)
    except Exception as exc:
        logger.error("Event parsing failed: %s", exc)
        notifier.alert_failure(run_id=run_id, stage="event_parsing", error=str(exc), input_file="unknown")
        return _error_response(500, f"Event parsing failed: {exc}")

    # Skip marker / placeholder files (e.g. .keep) — only process .tab files
    if not key.lower().endswith(".tab"):
        logger.info("Skipping non-.tab file: %s", key)
        return {"statusCode": 200, "body": json.dumps({"message": "Skipped non-tab file", "key": key})}

    phase_folder = _get_phase_folder(key)

    s3_client = boto3.client("s3")
    input_uri = f"s3://{bucket}/{key}"

    # Naming convention enforcement (configurable — off by default in dev).
    # Full timestamp required: <name>_YYYYMMDD_HHMMSS.tab
    # This convention is feed-frequency agnostic — works for daily, hourly, or
    # any other cadence without needing a convention change later.
    # Lambda validates — it does NOT rename. Naming is the source's responsibility.
    filename = key.split("/")[-1]
    enforce_date = cfg.get("data_quality.enforce_filename_date", default=False)
    if enforce_date and not _has_date_in_filename(filename):
        msg = (f"Rejected: '{filename}' does not include a full timestamp. "
               f"Source must deliver files as <name>_YYYYMMDD_HHMMSS.tab "
               f"e.g. hit_data_phase1_20260225_143000.tab")
        logger.warning(msg)
        _reject_input(s3_client, bucket, key, phase_folder, reason="missing_date_in_filename")
        return {"statusCode": 400, "body": json.dumps({"message": msg, "key": key})}

    if _is_already_processed(s3_client, bucket, key, phase_folder):
        msg = (f"Duplicate rejected: '{filename}' was already processed. "
               f"Each daily delivery must have a unique date in the filename.")
        logger.warning(msg)
        _reject_input(s3_client, bucket, key, phase_folder, reason="duplicate")
        return {"statusCode": 409, "body": json.dumps({"message": msg, "key": key})}

    threshold_mb = cfg.get("processing.small_file_threshold_mb", default=20)

    try:
        head         = s3_client.head_object(Bucket=bucket, Key=key)
        file_size    = head["ContentLength"]
        file_size_mb = file_size / 1024 / 1024
        logger.info("File size: %.1fMB | Threshold: %dMB", file_size_mb, threshold_mb)
    except Exception as exc:
        logger.error("S3 head_object failed: %s", exc)
        notifier.alert_failure(run_id=run_id, stage="s3_head", error=str(exc), input_file=input_uri)
        return _error_response(500, f"Failed to read file metadata: {exc}")

    if file_size >= threshold_mb * 1024 * 1024:
        logger.info("ROUTE -> EMR Serverless (file %.1fMB >= threshold %dMB)", file_size_mb, threshold_mb)
        return _handoff_to_emr(input_uri, run_id, cfg, lineage, notifier,
                                start_ts, s3_client=s3_client, bucket=bucket, key=key,
                                reason="file_size_threshold", phase_folder=phase_folder)

    if context and hasattr(context, "get_remaining_time_in_millis"):
        remaining_ms = context.get_remaining_time_in_millis()
        if remaining_ms < 60_000:
            logger.warning("ROUTE -> EMR Serverless (only %.0fs remaining)", remaining_ms / 1000)
            return _handoff_to_emr(input_uri, run_id, cfg, lineage, notifier,
                                    start_ts, s3_client=s3_client, bucket=bucket, key=key,
                                    reason="timeout_safety", phase_folder=phase_folder)

    logger.info("ROUTE -> Pure Python (file %.1fMB < threshold %dMB)", file_size_mb, threshold_mb)
    local_input = f"/tmp/{run_id}_input.tab"

    try:
        s3_client.download_file(bucket, key, local_input)
        logger.info("Downloaded %.1fMB from S3.", file_size_mb)
    except Exception as exc:
        logger.error("S3 download failed: %s", exc)
        notifier.alert_failure(run_id=run_id, stage="s3_download", error=str(exc), input_file=input_uri)
        lineage.record(run_id=run_id, input_file=input_uri, dq_passed=False,
                       error=str(exc), duration_seconds=time.time()-start_ts)
        return _error_response(500, f"S3 download failed: {exc}")

    if context and hasattr(context, "get_remaining_time_in_millis"):
        remaining_ms = context.get_remaining_time_in_millis()
        if remaining_ms < 60_000:
            return _handoff_to_emr(input_uri, run_id, cfg, lineage, notifier,
                                    start_ts, s3_client=s3_client, bucket=bucket, key=key,
                                    reason="timeout_safety_post_download", phase_folder=phase_folder)

    try:
        with open(local_input, encoding="utf-8", errors="replace") as fh:
            raw_content = fh.read()
        pipeline                    = PreprocessingPipeline(cfg)
        rows, error_rows, dq_report = pipeline.run(raw_content)
        dq_passed                   = dq_report.get("overall_passed", True)
        input_rows                  = dq_report.get("rows_in", 0)
        error_count                 = dq_report.get("error_count", 0)
        logger.info("Preprocessing complete. Clean rows: %d | Error rows: %d", len(rows), error_count)
    except Exception as exc:
        logger.error("Preprocessing failed: %s", exc)
        notifier.alert_failure(run_id=run_id, stage="preprocessing", error=str(exc), input_file=input_uri)
        lineage.record(run_id=run_id, input_file=input_uri, dq_passed=False,
                       error=str(exc), duration_seconds=time.time()-start_ts)
        return _error_response(500, f"Preprocessing failed: {exc}")

    dq_error_s3_uri = None
    if error_rows:
        dq_error_s3_uri = _write_dq_errors(s3_client, bucket, key, error_rows, run_id, cfg=cfg)

    if cfg.get("data_quality.emit_quality_report", default=False):
        _write_dq_report(s3_client, bucket, key, run_id, dq_report)

    if context and hasattr(context, "get_remaining_time_in_millis"):
        remaining_ms = context.get_remaining_time_in_millis()
        if remaining_ms < 60_000:
            return _handoff_to_emr(input_uri, run_id, cfg, lineage, notifier,
                                    start_ts, s3_client=s3_client, bucket=bucket, key=key,
                                    reason="timeout_safety_post_preprocessing",
                                    phase_folder=phase_folder)

    try:
        analyzer = SearchKeywordAnalyzer(cfg)
        results  = analyzer.analyze(rows)
        logger.info("Analysis complete. Output rows: %d", len(results))
    except Exception as exc:
        logger.error("Analysis failed: %s", exc)
        notifier.alert_failure(run_id=run_id, stage="analysis", error=str(exc), input_file=input_uri)
        lineage.record(run_id=run_id, input_file=input_uri, input_rows=input_rows,
                       dq_passed=dq_passed, error=str(exc), duration_seconds=time.time()-start_ts)
        return _error_response(500, f"Analysis failed: {exc}")

    try:
        writer       = OutputWriter(cfg)
        exec_date    = datetime.now(timezone.utc)
        filename     = writer.get_filename(exec_date)
        local_output = f"/tmp/{run_id}_{filename}"
        writer.write_local(results, local_output)

        # Hive-style phase-first partitioning for Athena/Glue compatibility
        s3_output_key = (
            f"processed/{phase_folder}"
            f"/year={exec_date.year}"
            f"/month={exec_date.month:02d}"
            f"/day={exec_date.day:02d}"
            f"/{filename}"
        )
        s3_output_uri = writer.upload_to_s3(local_output, s3_output_key)
    except Exception as exc:
        logger.error("Output write failed: %s", exc)
        notifier.alert_failure(run_id=run_id, stage="output_write", error=str(exc), input_file=input_uri)
        lineage.record(run_id=run_id, input_file=input_uri, input_rows=input_rows,
                       dq_passed=dq_passed, error=str(exc), duration_seconds=time.time()-start_ts)
        return _error_response(500, f"Output write failed: {exc}")

    _archive_input(s3_client, bucket, key, phase_folder=phase_folder)

    duration = time.time() - start_ts
    lineage.record(
        run_id=run_id, input_file=input_uri,
        input_rows=input_rows, output_file=s3_output_uri or local_output,
        output_rows=len(results), engine_used="pure_python",
        dq_passed=dq_passed, duration_seconds=duration,
    )

    col0, col1, col2 = cfg.get("output.columns",
        default=["Search Engine Domain", "Search Keyword", "Revenue"])
    logger.info("=" * 60)
    logger.info("RESULTS — Search Keyword Performance")
    logger.info("=" * 60)
    if not results:
        logger.info("No qualifying search engine revenue found.")
    else:
        logger.info("%-25s %-25s %10s", col0, col1, col2)
        logger.info("-" * 60)
        for row in results:
            logger.info("%-25s %-25s %10s", row[col0], row[col1], f"${row[col2]:.2f}")
    logger.info("=" * 60)
    logger.info("Engine: pure_python | Output: %s", s3_output_uri or local_output)
    logger.info("Duration: %.2fs | Run ID: %s", duration, run_id)

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
        })
    }


def _handoff_to_emr(input_uri, run_id, cfg, lineage, notifier, start_ts,
                    s3_client=None, bucket=None, key=None, reason="", phase_folder="other"):
    # Archive is intentionally NOT done here.
    # The Spark job archives the input file on successful completion,
    # ensuring we never lose data if the EMR job fails.
    result = _trigger_emr_job(input_uri, run_id, cfg, notifier, phase_folder=phase_folder)
    lineage.record(
        run_id=run_id, input_file=input_uri,
        engine_used="pyspark_emr", dq_passed=True,
        duration_seconds=time.time()-start_ts,
    )
    if reason:
        logger.info("EMR handoff reason: %s", reason)
    return result


def _trigger_emr_job(input_s3_uri, run_id, cfg, notifier, phase_folder="other"):
    app_id           = os.environ.get("EMR_SERVERLESS_APP_ID")
    exec_role_arn    = os.environ.get("EMR_EXECUTION_ROLE_ARN")
    raw_bucket       = os.environ.get("RAW_BUCKET", "")
    processed_bucket = os.environ.get("PROCESSED_BUCKET", "")
    logs_bucket      = os.environ.get("LOGS_BUCKET", "")
    region           = os.environ.get("AWS_REGION", "us-east-1")

    if not app_id or not exec_role_arn:
        error_msg = "EMR environment variables not set."
        logger.error(error_msg)
        notifier.alert_failure(run_id=run_id, stage="emr_submission",
                               error=error_msg, input_file=input_s3_uri)
        return _error_response(500, error_msg)

    now = datetime.now(timezone.utc)
    # Hive-style phase-first output path — mirrors the Lambda path exactly
    output_path = (
        f"s3://{processed_bucket}/processed/{phase_folder}"
        f"/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
    )
    script_uri = f"s3://{raw_bucket}/scripts/spark_skp_pipeline.py"
    job_name   = f"skp-auto-{run_id[:8]}"

    # Serialise all business rules so Spark reads from config, not hardcoded constants
    config_dict = {
        "search_engine_domains":  cfg.get("business_rules.search_engine_domains",
                                          default=["google.com", "yahoo.com", "bing.com",
                                                   "ask.com", "aol.com", "msn.com"]),
        "keyword_params":         cfg.get("business_rules.keyword_query_params",
                                          default=["q", "p", "query", "text", "s", "qs"]),
        "purchase_event_id":      cfg.get("business_rules.purchase_event_id",     default="1"),
        "revenue_col_index":      cfg.get("business_rules.revenue_column_index",  default=3),
        "product_delimiter":      cfg.get("business_rules.product_delimiter",      default=","),
        "product_attr_delimiter": cfg.get("business_rules.product_attr_delimiter", default=";"),
        "event_delimiter":        cfg.get("business_rules.event_delimiter",        default=","),
        "keyword_normalize_case": cfg.get("business_rules.keyword_normalize_case", default=True),
        "min_revenue_threshold":  cfg.get("business_rules.min_revenue_threshold",  default=0.0),
        "required_columns":       cfg.get("input.required_columns",                default=[]),
        "critical_columns":       cfg.get("data_quality.critical_columns",
                                          default=["hit_time_gmt", "ip", "referrer"]),
        "dedup_key_columns":      cfg.get("data_quality.duplicate_key_columns",
                                          default=["hit_time_gmt", "ip", "page_url"]),
        "null_warning_threshold_pct": cfg.get("data_quality.null_warning_threshold_pct",
                                              default=50),
        "output_columns":         cfg.get("output.columns",
                                          default=["Search Engine Domain",
                                                   "Search Keyword", "Revenue"]),
    }

    logger.info("Submitting EMR job: %s | Input: %s | Output: %s", job_name, input_s3_uri, output_path)

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
                        "--archive-input", input_s3_uri,   # Spark archives on success
                    ],
                    "sparkSubmitParameters": (
                        "--conf spark.executor.cores=4 "
                        "--conf spark.executor.memory=8g "
                        "--conf spark.driver.cores=2 "
                        "--conf spark.driver.memory=4g "
                        "--conf spark.sql.adaptive.enabled=true"
                    ),
                }
            },
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{logs_bucket}/emr-logs/"
                    }
                }
            } if logs_bucket else {},
            tags={"RunId": run_id, "ManagedBy": "skp-lambda"},
        )
        job_run_id = response["jobRunId"]
        logger.info("EMR job submitted. Job Run ID: %s", job_run_id)
        return {
            "statusCode": 202,
            "body": json.dumps({
                "message":    "Large file routed to EMR Serverless",
                "run_id":     run_id,
                "job_run_id": job_run_id,
                "input_file": input_s3_uri,
                "engine":     "pyspark_emr",
                "status":     "SUBMITTED",
            })
        }
    except Exception as exc:
        logger.error("EMR submission failed: %s", exc)
        notifier.alert_failure(run_id=run_id, stage="emr_submission",
                               error=str(exc), input_file=input_s3_uri)
        return _error_response(500, f"EMR submission failed: {exc}")


def _write_dq_errors(s3_client, bucket, key, error_rows, run_id, cfg=None):
    try:
        now       = datetime.now(timezone.utc)
        orig_name = key.split("/")[-1].rsplit(".", 1)[0]
        error_key = (
            f"dq-errors/{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{orig_name}_{run_id[:8]}_errors.csv"
        )
        # Mask PII columns (ip, user_agent) before writing to S3
        pii_cols = set(
            cfg.get("data_quality.pii_columns", default=["ip", "user_agent"])
            if cfg else ["ip", "user_agent"]
        )
        masked_rows = []
        for row in error_rows:
            masked = dict(row)
            for col in pii_cols:
                if col in masked and masked[col]:
                    masked[col] = mask_pii(str(masked[col]))
            masked_rows.append(masked)

        fieldnames = list(masked_rows[0].keys()) if masked_rows else []
        if "_error_reason" not in fieldnames:
            fieldnames.append("_error_reason")
        buf    = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(masked_rows)
        s3_client.put_object(
            Bucket=bucket, Key=error_key,
            Body=buf.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )
        error_uri = f"s3://{bucket}/{error_key}"
        logger.info("DQ errors written: %s (%d rows)", error_uri, len(masked_rows))
        return error_uri
    except Exception as exc:
        logger.warning("Failed to write DQ errors (non-fatal): %s", exc)
        return None


def _write_dq_report(s3_client, bucket, key, run_id, dq_report):
    """Write the DQ report JSON to the raw bucket's dq-reports/ prefix."""
    try:
        now       = datetime.now(timezone.utc)
        orig_name = key.split("/")[-1].rsplit(".", 1)[0]
        report_key = (
            f"dq-reports/{now.year}/{now.month:02d}/{now.day:02d}/"
            f"{orig_name}_{run_id[:8]}_dq_report.json"
        )
        s3_client.put_object(
            Bucket=bucket, Key=report_key,
            Body=json.dumps(dq_report, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("DQ report written: s3://%s/%s", bucket, report_key)
    except Exception as exc:
        logger.warning("Failed to write DQ report (non-fatal): %s", exc)


def _has_date_in_filename(filename):
    """
    Validates that the filename contains a full timestamp (YYYYMMDD_HHMMSS).

    Using a full timestamp (not just a date) is the correct convention because:
    - Works for daily, hourly, or any other feed frequency without change
    - Guarantees uniqueness even if the same logical dataset is delivered twice
      in the same day (restatement, correction, hourly slice)
    - Avoids collision when feed frequency is unknown or changes over time

    Required:  hit_data_phase1_20260225_143000.tab  (_YYYYMMDD_HHMMSS)
    Rejected:  hit_data_phase1_20260225.tab          (date-only — not safe for
                                                       sub-daily feeds)
    Rejected:  hit_data_phase1_crawl.tab             (no timestamp at all)
    """
    import re
    return bool(re.search(r"_\d{8}_\d{6}\.", filename))


def _is_already_processed(s3_client, bucket, key, phase_folder):
    """
    Return True if this exact filename already exists in archive/{phase_folder}/.
    With dated filenames enforced at source (YYYYMMDD in name), same filename
    means same day's same delivery — a true duplicate.
    """
    filename    = key.split("/")[-1]
    archive_key = f"archive/{phase_folder}/{filename}"
    try:
        s3_client.head_object(Bucket=bucket, Key=archive_key)
        return True
    except s3_client.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def _reject_input(s3_client, bucket, key, phase_folder, reason="duplicate"):
    """
    Move a rejected file to rejected/{phase}/year=.../month=.../day=./{filename}.
    Mirrors the Hive-style partitioning used in processed/ for auditability.
    The original object is deleted so it does not re-trigger the pipeline.
    """
    try:
        now         = datetime.now(timezone.utc)
        filename    = key.split("/")[-1]
        reject_key  = (
            f"rejected/{phase_folder}"
            f"/year={now.year}/month={now.month:02d}/day={now.day:02d}"
            f"/{filename}"
        )
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": key},
            Key=reject_key,
            Metadata={"rejection_reason": reason, "original_key": key},
            MetadataDirective="REPLACE",
        )
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.warning("Rejected (%s): s3://%s/%s -> s3://%s/%s",
                       reason, bucket, key, bucket, reject_key)
    except Exception as exc:
        logger.warning("Reject move failed (non-fatal): %s", exc)


def _archive_input(s3_client, bucket, key, phase_folder="other"):
    try:
        filename    = key.split("/")[-1]
        archive_key = f"archive/{phase_folder}/{filename}"
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": key},
            Key=archive_key,
        )
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info("Archived: s3://%s/%s -> s3://%s/%s", bucket, key, bucket, archive_key)
    except Exception as exc:
        logger.warning("Archive failed (non-fatal): %s", exc)


def _parse_event(event):
    if "Records" in event and event["Records"]:
        rec    = event["Records"][0]
        bucket = rec["s3"]["bucket"]["name"]
        key    = rec["s3"]["object"]["key"]
        return bucket, key
    if "bucket" in event and "key" in event:
        return event["bucket"], event["key"]
    raise ValueError("Unrecognised event format.")


def _error_response(status_code, message):
    logger.error("Pipeline error: %s", message)
    return {"statusCode": status_code, "body": json.dumps({"error": message})}


if __name__ == "__main__":
    import shutil
    from unittest.mock import patch

    input_path = sys.argv[1] if len(sys.argv) > 1 else "tests/data/hit_data.tab"

    class _LocalS3:
        def head_object(self, Bucket, Key):
            return {"ContentLength": os.path.getsize(Key)}
        def download_file(self, bucket, key, dest):
            shutil.copy(key, dest)
        def put_object(self, **kwargs): pass
        def copy_object(self, **kwargs): pass
        def delete_object(self, **kwargs): pass

    os.environ["PROCESSED_BUCKET"] = ""
    os.environ["LOGS_BUCKET"]      = ""
    os.environ["APP_ENV"]          = "dev"

    with patch("boto3.client", return_value=_LocalS3()):
        result = handler({"bucket": "local", "key": input_path})
        print("\n" + "="*60)
        print("Lambda response:")
        print(json.dumps(json.loads(result["body"]), indent=2))

"""
lambda_handler.py
=================
AWS Lambda entry point — dual-path routing with context-aware handoff.

Routing Logic (in order):
  1. S3 HEAD request — get file size from metadata (no download)
  2. File >= threshold (20MB) → trigger EMR Serverless immediately
  3. Lambda context check — if < 60s remaining → trigger EMR
  4. File < threshold → download → pure Python pipeline

Key Design Decisions:
  - S3 HEAD avoids downloading large files just to route them away
  - Context-aware routing prevents Lambda timeout mid-processing
  - Threshold is a cost optimization; context check is the safety net
  - Returns 202 Accepted for EMR (async) vs 200 OK for Python (sync)
"""
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import boto3

sys.path.insert(0, os.path.dirname(__file__))

from config.config_loader                 import load_config
from preprocessing.preprocessing_pipeline import PreprocessingPipeline
from processing.search_keyword_analyzer   import SearchKeywordAnalyzer
from output.writer                        import OutputWriter
from governance.lineage_tracker           import LineageTracker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger("lambda_handler")

_cfg = None

def _get_config():
    global _cfg
    if _cfg is None:
        _cfg = load_config(env=os.environ.get("APP_ENV", "dev"))
    return _cfg


# ── Main handler ─────────────────────────────────────────────────────

def handler(event, context=None):
    run_id   = str(uuid.uuid4())
    start_ts = time.time()
    cfg      = _get_config()
    lineage  = LineageTracker(cfg)

    logger.info("=== SKP Pipeline Start | run_id=%s ===", run_id)

    # ── Parse S3 event ────────────────────────────────────────────────
    try:
        bucket, key = _parse_event(event)
        logger.info("Input: s3://%s/%s", bucket, key)
    except Exception as exc:
        logger.error("Event parsing failed: %s", exc)
        return _error_response(500, f"Event parsing failed: {exc}")

    s3_client    = boto3.client("s3")
    input_uri    = f"s3://{bucket}/{key}"
    threshold_mb = cfg.get("processing.small_file_threshold_mb", default=20)

    # ── Route Check 1: File size via S3 HEAD (no download) ───────────
    # head_object returns metadata instantly — no data transferred.
    # This is the efficient way to check size before committing to download.
    try:
        head         = s3_client.head_object(Bucket=bucket, Key=key)
        file_size    = head["ContentLength"]
        file_size_mb = file_size / 1024 / 1024
        logger.info(
            "File size: %.1fMB | Threshold: %dMB | Source: S3 metadata",
            file_size_mb, threshold_mb
        )
    except Exception as exc:
        logger.error("S3 head_object failed: %s", exc)
        return _error_response(500, f"Failed to read file metadata: {exc}")

    if file_size >= threshold_mb * 1024 * 1024:
        logger.info(
            "ROUTE → EMR Serverless (file %.1fMB >= threshold %dMB)",
            file_size_mb, threshold_mb
        )
        return _handoff_to_emr(input_uri, run_id, cfg, lineage, start_ts,
                                reason="file_size_threshold")

    # ── Route Check 2: Lambda remaining time (context-aware) ──────────
    # Even for small files, if Lambda is running low on time
    # (e.g. cold start was slow, preprocessing took longer than expected)
    # hand off to EMR rather than risk a mid-processing timeout.
    # 60 seconds gives enough time for clean EMR submission + lineage write.
    if context and hasattr(context, "get_remaining_time_in_millis"):
        remaining_ms = context.get_remaining_time_in_millis()
        remaining_s  = remaining_ms / 1000
        logger.info("Lambda time remaining: %.0fs", remaining_s)

        if remaining_ms < 60_000:
            logger.warning(
                "ROUTE → EMR Serverless (only %.0fs remaining — safety handoff)",
                remaining_s
            )
            return _handoff_to_emr(input_uri, run_id, cfg, lineage, start_ts,
                                    reason="timeout_safety")

    # ── Pure Python path: download and process ────────────────────────
    logger.info(
        "ROUTE → Pure Python (file %.1fMB < threshold %dMB, time OK)",
        file_size_mb, threshold_mb
    )

    local_input = f"/tmp/{run_id}_input.tab"
    try:
        s3_client.download_file(bucket, key, local_input)
        logger.info("Downloaded %.1fMB from S3.", file_size_mb)
    except Exception as exc:
        logger.error("S3 download failed: %s", exc)
        lineage.record(run_id=run_id, input_file=input_uri,
                       dq_passed=False, error=str(exc),
                       duration_seconds=time.time()-start_ts)
        return _error_response(500, f"S3 download failed: {exc}")

    # ── Route Check 3: Post-download context check ────────────────────
    # Check again after download — download itself consumes time.
    if context and hasattr(context, "get_remaining_time_in_millis"):
        remaining_ms = context.get_remaining_time_in_millis()
        if remaining_ms < 60_000:
            logger.warning(
                "ROUTE → EMR Serverless (%.0fs remaining after download — safety handoff)",
                remaining_ms / 1000
            )
            return _handoff_to_emr(input_uri, run_id, cfg, lineage, start_ts,
                                    reason="timeout_safety_post_download")

    # ── Preprocessing ─────────────────────────────────────────────────
    try:
        with open(local_input, encoding="utf-8", errors="replace") as fh:
            raw_content = fh.read()
        pipeline        = PreprocessingPipeline(cfg)
        rows, dq_report = pipeline.run(raw_content)
        dq_passed       = dq_report.get("overall_passed", True)
        input_rows      = dq_report.get("rows_in", 0)
        logger.info("Preprocessing complete. Clean rows: %d", len(rows))
    except Exception as exc:
        logger.error("Preprocessing failed: %s", exc)
        lineage.record(run_id=run_id, input_file=input_uri,
                       dq_passed=False, error=str(exc),
                       duration_seconds=time.time()-start_ts)
        return _error_response(500, f"Preprocessing failed: {exc}")

    # ── Route Check 4: Post-preprocessing context check ───────────────
    # Preprocessing is the most memory-intensive step.
    # Check again before analysis — if we are tight on time, hand off.
    if context and hasattr(context, "get_remaining_time_in_millis"):
        remaining_ms = context.get_remaining_time_in_millis()
        if remaining_ms < 60_000:
            logger.warning(
                "ROUTE → EMR Serverless (%.0fs remaining after preprocessing — safety handoff)",
                remaining_ms / 1000
            )
            return _handoff_to_emr(input_uri, run_id, cfg, lineage, start_ts,
                                    reason="timeout_safety_post_preprocessing")

    # ── Core analysis ─────────────────────────────────────────────────
    try:
        analyzer = SearchKeywordAnalyzer(cfg)
        results  = analyzer.analyze(rows)
        logger.info("Analysis complete. Output rows: %d", len(results))
    except Exception as exc:
        logger.error("Analysis failed: %s", exc)
        lineage.record(run_id=run_id, input_file=input_uri,
                       input_rows=input_rows, dq_passed=dq_passed,
                       error=str(exc), duration_seconds=time.time()-start_ts)
        return _error_response(500, f"Analysis failed: {exc}")

    # ── Write output ──────────────────────────────────────────────────
    writer       = OutputWriter(cfg)
    exec_date    = datetime.now(timezone.utc)
    filename     = writer.get_filename(exec_date)
    local_output = f"/tmp/{run_id}_{filename}"
    writer.write_local(results, local_output)
    s3_output_key = f"processed/{filename}"
    s3_output_uri = writer.upload_to_s3(local_output, s3_output_key)

    # ── Archive input file — prevents reprocessing on re-upload ──────
    # Move processed file from raw/ to archive/ (separate prefix).
    # S3 trigger only watches raw/ so archived files are never reprocessed.
    _archive_input(s3_client, bucket, key)

    # ── Lineage ───────────────────────────────────────────────────────
    duration = time.time() - start_ts
    lineage.record(
        run_id=run_id, input_file=input_uri,
        input_rows=input_rows, output_file=s3_output_uri or local_output,
        output_rows=len(results), engine_used="pure_python",
        dq_passed=dq_passed, duration_seconds=duration,
    )

    # ── Log results ───────────────────────────────────────────────────
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
            logger.info("%-25s %-25s %10s",
                row[col0], row[col1], f"${row[col2]:.2f}")
    logger.info("=" * 60)
    logger.info("Engine: pure_python | Output: %s", s3_output_uri or local_output)
    logger.info("Duration: %.2fs | Run ID: %s", duration, run_id)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message":     "Pipeline complete",
            "run_id":      run_id,
            "input_file":  input_uri,
            "output_file": s3_output_uri,
            "rows_in":     input_rows,
            "rows_out":    len(results),
            "dq_passed":   dq_passed,
            "duration_s":  round(duration, 2),
            "engine":      "pure_python",
        })
    }


# ── EMR handoff ───────────────────────────────────────────────────────

def _handoff_to_emr(input_uri, run_id, cfg, lineage, start_ts, reason=""):
    """
    Central EMR handoff function — called from any routing check.
    Records lineage with the reason for handoff before submitting job.
    """
    result = _trigger_emr_job(input_uri, run_id, cfg)
    lineage.record(
        run_id=run_id, input_file=input_uri,
        engine_used="pyspark_emr", dq_passed=True,
        duration_seconds=time.time()-start_ts,
    )
    if reason:
        logger.info("EMR handoff reason: %s", reason)
    return result


def _trigger_emr_job(input_s3_uri: str, run_id: str, cfg) -> dict:
    """
    Submit PySpark job to EMR Serverless.
    Returns 202 Accepted — processing is asynchronous.

    Required env vars:
      EMR_SERVERLESS_APP_ID   — EMR application ID
      EMR_EXECUTION_ROLE_ARN  — IAM execution role
      RAW_BUCKET              — bucket containing PySpark script
      PROCESSED_BUCKET        — output bucket
      LOGS_BUCKET             — EMR logs bucket
    """
    app_id           = os.environ.get("EMR_SERVERLESS_APP_ID")
    exec_role_arn    = os.environ.get("EMR_EXECUTION_ROLE_ARN")
    raw_bucket       = os.environ.get("RAW_BUCKET", "")
    processed_bucket = os.environ.get("PROCESSED_BUCKET", "")
    logs_bucket      = os.environ.get("LOGS_BUCKET", "")
    region           = os.environ.get("AWS_REGION", "us-east-1")

    if not app_id or not exec_role_arn:
        logger.error("EMR env vars not configured.")
        return _error_response(500, "EMR environment variables not set.")

    script_uri  = f"s3://{raw_bucket}/scripts/spark_skp_pipeline.py"
    output_path = f"s3://{processed_bucket}/processed/"
    job_name    = f"skp-auto-{run_id[:8]}"

    logger.info("Submitting EMR job: %s | Input: %s", job_name, input_s3_uri)

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
                        "--input",  input_s3_uri,
                        "--output", output_path,
                        "--env",    os.environ.get("APP_ENV", "dev"),
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
        return _error_response(500, f"EMR submission failed: {exc}")


# ── Archive helper ────────────────────────────────────────────────────

def _archive_input(s3_client, bucket: str, key: str):
    """
    Move processed file from raw/ to archive/ after successful processing.
    archive/ is outside the S3 trigger path — prevents reprocessing on re-upload.

    raw/filename.tab     → archive/filename.tab
    """
    try:
        filename    = key.split("/")[-1]
        archive_key = f"archive/{filename}"
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": key},
            Key=archive_key,
        )
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info("Archived: s3://%s/%s → s3://%s/%s", bucket, key, bucket, archive_key)
    except Exception as exc:
        # Archive failure is non-fatal — log warning but don't fail the pipeline
        logger.warning("Archive failed (non-fatal): %s", exc)


# ── Helpers ───────────────────────────────────────────────────────────

def _parse_event(event: dict) -> tuple:
    if "Records" in event and event["Records"]:
        rec    = event["Records"][0]
        bucket = rec["s3"]["bucket"]["name"]
        key    = rec["s3"]["object"]["key"]
        return bucket, key
    if "bucket" in event and "key" in event:
        return event["bucket"], event["key"]
    raise ValueError("Unrecognised event format.")


def _error_response(status_code: int, message: str) -> dict:
    logger.error("Pipeline error: %s", message)
    return {"statusCode": status_code, "body": json.dumps({"error": message})}


# ── Local test entry point ────────────────────────────────────────────
if __name__ == "__main__":
    import shutil
    from unittest.mock import patch

    input_path = sys.argv[1] if len(sys.argv) > 1 else "tests/data/hit_data.tab"

    class _LocalS3:
        def head_object(self, Bucket, Key):
            return {"ContentLength": os.path.getsize(Key)}
        def download_file(self, bucket, key, dest):
            shutil.copy(key, dest)
        def put_object(self, **kwargs):
            pass

    os.environ["PROCESSED_BUCKET"] = ""
    os.environ["LOGS_BUCKET"]      = ""
    os.environ["APP_ENV"]          = "dev"

    with patch("boto3.client", return_value=_LocalS3()):
        result = handler({"bucket": "local", "key": input_path})
        print("\n" + "="*60)
        print("Lambda response:")
        print(json.dumps(json.loads(result["body"]), indent=2))

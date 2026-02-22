"""
lambda_handler.py
=================
AWS Lambda entry point for the Search Keyword Performance pipeline.

Triggered by: S3 PutObject event (file lands in raw/ prefix)
              OR manual invocation with payload

Flow:
  1. Parse S3 event → get bucket + key
  2. Download input file from S3 to /tmp
  3. Run PreprocessingPipeline (schema, null, dedup, dtype checks)
  4. Run SearchKeywordAnalyzer (session-based attribution)
  5. Write output to /tmp
  6. Upload output to processed S3 bucket
  7. Write lineage record to logs S3 bucket
  8. Return 200 with summary

Author  : Principal Data Engineer, ACS Data Engineering
"""

import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime

import boto3

# ── Path setup (Lambda /var/task) ────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))

from config.config_loader           import load_config
from preprocessing.preprocessing_pipeline import PreprocessingPipeline
from processing.search_keyword_analyzer   import SearchKeywordAnalyzer
from output.writer                        import OutputWriter
from governance.lineage_tracker           import LineageTracker

# ── Logging setup ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("lambda_handler")

# Load config once at cold start (cached for warm invocations)
_cfg = None

def _get_config():
    global _cfg
    if _cfg is None:
        _cfg = load_config(env=os.environ.get("APP_ENV", "dev"))
    return _cfg


# ── Main handler ─────────────────────────────────────────────────────

def handler(event, context=None):
    """
    Lambda handler function.

    Accepts either:
      - S3 event (from S3 trigger):   event["Records"][0]["s3"]
      - Direct invocation payload:    event["bucket"] + event["key"]
    """
    run_id    = str(uuid.uuid4())
    start_ts  = time.time()
    cfg       = _get_config()
    lineage   = LineageTracker(cfg)

    logger.info("=== SKP Pipeline Run Start | run_id=%s ===", run_id)

    # ── Parse event ─────────────────────────────────────────────────
    try:
        bucket, key = _parse_event(event)
        logger.info("Input: s3://%s/%s", bucket, key)
    except Exception as exc:
        logger.error("Failed to parse event: %s | event=%s", exc, json.dumps(event))
        return _error_response(500, f"Event parsing failed: {exc}")

    # ── Download from S3 ────────────────────────────────────────────
    local_input = f"/tmp/{run_id}_input.tab"
    try:
        s3 = boto3.client("s3")
        s3.download_file(bucket, key, local_input)
        logger.info("Downloaded %s bytes from S3", os.path.getsize(local_input))
    except Exception as exc:
        logger.error("S3 download failed: %s", exc)
        lineage.record(run_id=run_id, input_file=f"s3://{bucket}/{key}",
                       dq_passed=False, error=str(exc),
                       duration_seconds=time.time()-start_ts)
        return _error_response(500, f"S3 download failed: {exc}")

    # ── Read raw content ────────────────────────────────────────────
    with open(local_input, encoding="utf-8", errors="replace") as fh:
        raw_content = fh.read()

    input_rows = raw_content.count("\n")  # approximate

    # ── Preprocessing ────────────────────────────────────────────────
    try:
        pipeline   = PreprocessingPipeline(cfg)
        df, dq_report = pipeline.run(raw_content)
        dq_passed  = dq_report.get("overall_passed", True)
        input_rows = dq_report.get("rows_in", input_rows)
        logger.info("Preprocessing complete. Rows after cleaning: %d", len(df))
    except Exception as exc:
        logger.error("Preprocessing failed: %s", exc)
        lineage.record(run_id=run_id, input_file=f"s3://{bucket}/{key}",
                       input_rows=input_rows, dq_passed=False, error=str(exc),
                       duration_seconds=time.time()-start_ts)
        return _error_response(500, f"Preprocessing failed: {exc}")

    # ── Core analysis ────────────────────────────────────────────────
    try:
        analyzer   = SearchKeywordAnalyzer(cfg)
        result_df  = analyzer.analyze(df)
        logger.info("Analysis complete. Output rows: %d", len(result_df))
    except Exception as exc:
        logger.error("Analysis failed: %s", exc)
        lineage.record(run_id=run_id, input_file=f"s3://{bucket}/{key}",
                       input_rows=input_rows, dq_passed=dq_passed, error=str(exc),
                       duration_seconds=time.time()-start_ts)
        return _error_response(500, f"Analysis failed: {exc}")

    # ── Write output ────────────────────────────────────────────────
    writer       = OutputWriter(cfg)
    exec_date    = datetime.utcnow()
    filename     = writer.get_filename(exec_date)
    local_output = f"/tmp/{run_id}_{filename}"

    writer.write_local(result_df, local_output)

    # ── Upload to S3 ────────────────────────────────────────────────
    s3_output_key = f"processed/{filename}"
    s3_output_uri = writer.upload_to_s3(local_output, s3_output_key)

    # ── Lineage record ───────────────────────────────────────────────
    duration = time.time() - start_ts
    lineage.record(
        run_id=run_id,
        input_file=f"s3://{bucket}/{key}",
        input_rows=input_rows,
        output_file=s3_output_uri or local_output,
        output_rows=len(result_df),
        engine_used="pandas",
        dq_passed=dq_passed,
        duration_seconds=duration,
    )

    # ── Print results to CloudWatch logs ─────────────────────────────
    logger.info("=" * 60)
    logger.info("RESULTS — Search Keyword Performance")
    logger.info("=" * 60)
    if len(result_df) == 0:
        logger.info("No qualifying search engine revenue found in this file.")
    else:
        for _, row in result_df.iterrows():
            logger.info(
                "%-20s | %-25s | $%s",
                row.iloc[0], row.iloc[1], row.iloc[2]
            )
    logger.info("=" * 60)
    logger.info("Output: %s", s3_output_uri or local_output)
    logger.info("Duration: %.2fs | Run ID: %s", duration, run_id)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message":    "Pipeline complete",
            "run_id":     run_id,
            "input_file": f"s3://{bucket}/{key}",
            "output_file": s3_output_uri,
            "rows_in":    input_rows,
            "rows_out":   len(result_df),
            "dq_passed":  dq_passed,
            "duration_s": round(duration, 2),
        })
    }


# ── Helpers ──────────────────────────────────────────────────────────

def _parse_event(event: dict) -> tuple:
    """Extract S3 bucket and key from the Lambda event payload."""
    # S3 trigger event format
    if "Records" in event and event["Records"]:
        rec    = event["Records"][0]
        bucket = rec["s3"]["bucket"]["name"]
        key    = rec["s3"]["object"]["key"]
        return bucket, key

    # Direct invocation format
    if "bucket" in event and "key" in event:
        return event["bucket"], event["key"]

    raise ValueError(
        "Unrecognised event format. Expected S3 event with 'Records' "
        "or direct payload with 'bucket' and 'key'."
    )


def _error_response(status_code: int, message: str) -> dict:
    logger.error("Pipeline error: %s", message)
    return {
        "statusCode": status_code,
        "body": json.dumps({"error": message})
    }


# ── Local test entry point ────────────────────────────────────────────
if __name__ == "__main__":
    """
    Run locally against the real sample file — no AWS needed.
    Usage:  python lambda_handler.py [path/to/hit_data.tab]
    """
    import sys

    input_path = sys.argv[1] if len(sys.argv) > 1 else "tests/data/hit_data.tab"

    # Simulate an S3 event with a local path flag
    test_event = {"bucket": "local", "key": input_path}

    # Monkey-patch S3 download for local run
    original_download = None
    import boto3 as _boto3

    class _LocalS3:
        def download_file(self, bucket, key, dest):
            if bucket == "local":
                import shutil
                shutil.copy(key, dest)
            else:
                raise RuntimeError("Use bucket='local' for local testing")
        def put_object(self, **kwargs):
            pass

    # Override boto3 client for local run
    import unittest.mock as _mock
    with _mock.patch("boto3.client", return_value=_LocalS3()):
        # Also disable S3 upload in writer
        os.environ["PROCESSED_BUCKET"] = ""
        os.environ["LOGS_BUCKET"]      = ""
        os.environ["APP_ENV"]          = "dev"

        result = handler(test_event, context=None)
        print("\n" + "="*60)
        print("Lambda response:")
        print(json.dumps(json.loads(result["body"]), indent=2))

"""
lambda_handler.py
=================
AWS Lambda entry point — Pure Python path. Zero heavy dependencies.
Only runtime dependency: boto3 (pre-installed in Lambda runtime).

Flow:
  1. Parse S3 event
  2. Download input file from S3
  3. PreprocessingPipeline (schema, null, dedup checks)
  4. SearchKeywordAnalyzer (session-based attribution)
  5. Write output to /tmp
  6. Upload to S3 processed bucket
  7. Write lineage record
  8. Return 200 with summary
"""
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import boto3

# ── Path setup ───────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))

from config.config_loader                     import load_config
from preprocessing.preprocessing_pipeline     import PreprocessingPipeline
from processing.search_keyword_analyzer       import SearchKeywordAnalyzer
from output.writer                            import OutputWriter
from governance.lineage_tracker               import LineageTracker

# ── Logging — root logger must be set for CloudWatch to capture INFO ─
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger("lambda_handler")

# ── Config cached at cold start ──────────────────────────────────────
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

    logger.info("=== SKP Pipeline Run Start | run_id=%s ===", run_id)

    # ── Parse event ──────────────────────────────────────────────────
    try:
        bucket, key = _parse_event(event)
        logger.info("Input: s3://%s/%s", bucket, key)
    except Exception as exc:
        logger.error("Failed to parse event: %s", exc)
        return _error_response(500, f"Event parsing failed: {exc}")

    # ── Download from S3 ─────────────────────────────────────────────
    local_input = f"/tmp/{run_id}_input.tab"
    try:
        boto3.client("s3").download_file(bucket, key, local_input)
        file_size = os.path.getsize(local_input)
        logger.info("Downloaded %d bytes from S3.", file_size)
    except Exception as exc:
        logger.error("S3 download failed: %s", exc)
        lineage.record(run_id=run_id, input_file=f"s3://{bucket}/{key}",
                       dq_passed=False, error=str(exc),
                       duration_seconds=time.time()-start_ts)
        return _error_response(500, f"S3 download failed: {exc}")

    # ── Read raw content ──────────────────────────────────────────────
    with open(local_input, encoding="utf-8", errors="replace") as fh:
        raw_content = fh.read()

    # ── Preprocessing ─────────────────────────────────────────────────
    try:
        pipeline      = PreprocessingPipeline(cfg)
        rows, dq_report = pipeline.run(raw_content)
        dq_passed     = dq_report.get("overall_passed", True)
        input_rows    = dq_report.get("rows_in", 0)
        logger.info("Preprocessing complete. Clean rows: %d", len(rows))
    except Exception as exc:
        logger.error("Preprocessing failed: %s", exc)
        lineage.record(run_id=run_id, input_file=f"s3://{bucket}/{key}",
                       dq_passed=False, error=str(exc),
                       duration_seconds=time.time()-start_ts)
        return _error_response(500, f"Preprocessing failed: {exc}")

    # ── Core analysis ─────────────────────────────────────────────────
    try:
        analyzer = SearchKeywordAnalyzer(cfg)
        results  = analyzer.analyze(rows)
        logger.info("Analysis complete. Output rows: %d", len(results))
    except Exception as exc:
        logger.error("Analysis failed: %s", exc)
        lineage.record(run_id=run_id, input_file=f"s3://{bucket}/{key}",
                       input_rows=input_rows, dq_passed=dq_passed, error=str(exc),
                       duration_seconds=time.time()-start_ts)
        return _error_response(500, f"Analysis failed: {exc}")

    # ── Write output ──────────────────────────────────────────────────
    writer       = OutputWriter(cfg)
    exec_date    = datetime.now(timezone.utc)
    filename     = writer.get_filename(exec_date)
    local_output = f"/tmp/{run_id}_{filename}"

    writer.write_local(results, local_output)

    s3_output_key = f"processed/{filename}"
    s3_output_uri = writer.upload_to_s3(local_output, s3_output_key)

    # ── Lineage ───────────────────────────────────────────────────────
    duration = time.time() - start_ts
    lineage.record(
        run_id=run_id,
        input_file=f"s3://{bucket}/{key}",
        input_rows=input_rows,
        output_file=s3_output_uri or local_output,
        output_rows=len(results),
        engine_used="pure_python",
        dq_passed=dq_passed,
        duration_seconds=duration,
    )

    # ── Print results to CloudWatch ───────────────────────────────────
    col0, col1, col2 = cfg.get("output.columns",
        default=["Search Engine Domain","Search Keyword","Revenue"])

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
    logger.info("Output : %s", s3_output_uri or local_output)
    logger.info("Duration: %.2fs | Run ID: %s", duration, run_id)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message":    "Pipeline complete",
            "run_id":     run_id,
            "input_file": f"s3://{bucket}/{key}",
            "output_file": s3_output_uri,
            "rows_in":    input_rows,
            "rows_out":   len(results),
            "dq_passed":  dq_passed,
            "duration_s": round(duration, 2),
            "engine":     "pure_python",
        })
    }


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
    from unittest.mock import patch, MagicMock

    input_path = sys.argv[1] if len(sys.argv) > 1 else "tests/data/hit_data.tab"

    class _LocalS3:
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

"""
lineage_tracker.py
==================
Writes a JSON lineage record to S3 after every pipeline run.
Provides full auditability: input → output → code version → duration.
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)


class LineageTracker:

    def __init__(self, cfg):
        self.enabled     = cfg.get("lineage.enabled", default=True)
        self.logs_bucket = os.environ.get("LOGS_BUCKET", "")
        self.prefix      = cfg.get("aws.s3.prefix_lineage", default="lineage/")

    def record(self, **kwargs) -> dict:
        """
        Write a lineage record to S3.

        Keyword args accepted:
            run_id, input_file, input_rows, output_file, output_rows,
            engine_used, dq_passed, duration_seconds, git_commit, error
        """
        if not self.enabled:
            return {}

        record = {
            "run_id":           kwargs.get("run_id", str(uuid.uuid4())),
            "execution_date":   datetime.now(timezone.utc).isoformat(),
            "input_file":       kwargs.get("input_file", ""),
            "input_rows":       kwargs.get("input_rows", 0),
            "output_file":      kwargs.get("output_file", ""),
            "output_rows":      kwargs.get("output_rows", 0),
            "engine_used":      kwargs.get("engine_used", "pandas"),
            "dq_passed":        kwargs.get("dq_passed", False),
            "duration_seconds": round(kwargs.get("duration_seconds", 0.0), 2),
            "git_commit":       os.environ.get("GIT_COMMIT", "local"),
            "attribution_model":"first_touch",
            "error":            kwargs.get("error", None),
        }

        if self.logs_bucket:
            key = f"{self.prefix}{record['execution_date'][:10]}/{record['run_id']}.json"
            try:
                boto3.client("s3").put_object(
                    Bucket=self.logs_bucket,
                    Key=key,
                    Body=json.dumps(record, indent=2),
                    ContentType="application/json",
                )
                logger.info("Lineage record written: s3://%s/%s", self.logs_bucket, key)
            except Exception as exc:
                logger.warning("Failed to write lineage record: %s", exc)
        else:
            logger.info("Lineage record (no S3 bucket set): %s", json.dumps(record))

        return record

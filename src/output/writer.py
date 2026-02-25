"""
writer.py
=========
Pure Python output writer — zero dependencies.
Writes tab-delimited output and uploads to S3.
"""
import csv
import io
import logging
import os
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)


class OutputWriter:

    def __init__(self, cfg):
        self.delimiter        = cfg.get("output.file_delimiter",          default="\t")
        self.filename_pat     = cfg.get("output.filename_pattern",        default="{date}_SearchKeywordPerformance.tab")
        self.date_fmt         = cfg.get("output.date_format",             default="%Y-%m-%d")
        self.revenue_dp       = cfg.get("output.revenue_decimal_places",  default=2)
        self.out_cols         = cfg.get("output.columns",
                                        default=["Search Engine Domain","Search Keyword","Revenue"])
        self.processed_bucket = os.environ.get("PROCESSED_BUCKET", "")

    def get_filename(self, execution_date=None) -> str:
        date_str = (execution_date or datetime.now(timezone.utc)).strftime(self.date_fmt)
        return self.filename_pat.replace("{date}", date_str)

    def write_local(self, results: list, local_path: str) -> str:
        """Write results list of dicts to a local tab-delimited file."""
        with open(local_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.out_cols, delimiter=self.delimiter)
            writer.writeheader()
            for row in results:
                # Format revenue to fixed decimal places
                row_out = dict(row)
                row_out[self.out_cols[2]] = f"{float(row[self.out_cols[2]]):.{self.revenue_dp}f}"
                writer.writerow(row_out)

        logger.info("Output written locally: %s (%d rows)", local_path, len(results))
        return local_path

    def upload_to_s3(self, local_path: str, s3_key: str) -> str:
        """Upload local file to S3 processed bucket."""
        if not self.processed_bucket:
            logger.warning("PROCESSED_BUCKET env var not set — skipping S3 upload")
            return ""

        s3 = boto3.client("s3")
        with open(local_path, "rb") as f:
            s3.put_object(
                Bucket=self.processed_bucket,
                Key=s3_key,
                Body=f.read(),
                ContentType="text/plain",
            )

        s3_uri = f"s3://{self.processed_bucket}/{s3_key}"
        logger.info("Output uploaded to S3: %s", s3_uri)
        return s3_uri

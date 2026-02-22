"""
writer.py
=========
Writes the final tab-delimited output file and uploads to S3.
Filename format: YYYY-mm-dd_SearchKeywordPerformance.tab
"""

import io
import logging
import os
from datetime import datetime

import boto3
import pandas as pd

logger = logging.getLogger(__name__)


class OutputWriter:

    def __init__(self, cfg):
        self.delimiter     = cfg.get("output.file_delimiter",      default="\t")
        self.encoding      = cfg.get("output.file_encoding",       default="utf-8")
        self.filename_pat  = cfg.get("output.filename_pattern",    default="{date}_SearchKeywordPerformance.tab")
        self.date_fmt      = cfg.get("output.date_format",         default="%Y-%m-%d")
        self.revenue_dp    = cfg.get("output.revenue_decimal_places", default=2)
        self.processed_bucket = os.environ.get("PROCESSED_BUCKET", "")

    def get_filename(self, execution_date: datetime = None) -> str:
        """Generate output filename from pattern and execution date."""
        date_str = (execution_date or datetime.utcnow()).strftime(self.date_fmt)
        return self.filename_pat.replace("{date}", date_str)

    def write_local(self, df: pd.DataFrame, local_path: str) -> str:
        """Write results to a local tab-delimited file."""
        # Format revenue column
        revenue_col = df.columns[-1]
        df = df.copy()
        df[revenue_col] = df[revenue_col].apply(lambda x: f"{x:.{self.revenue_dp}f}")

        df.to_csv(local_path, sep=self.delimiter, index=False, encoding=self.encoding)
        logger.info("Output written locally: %s (%d rows)", local_path, len(df))
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

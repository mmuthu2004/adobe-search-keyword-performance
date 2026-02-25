"""
SEM Insights — standalone runner.

Reads a hit-level .tab file, runs SEM metric extraction, writes the results
to a local CSV, and uploads it to S3 under the same Hive-style partition
layout used by the main pipeline:

    s3://{processed_bucket}/processed/sem_insights/year=YYYY/month=MM/day=DD/{file}

Usage
-----
    # Offline — local CSV only, no S3 upload
    python -m src.sem_insights.run_sem_insights --no-s3

    # Default dev environment (skp-processed-dev-099622553872)
    python -m src.sem_insights.run_sem_insights

    # Explicit environment
    python -m src.sem_insights.run_sem_insights --env staging
    python -m src.sem_insights.run_sem_insights --env prod

    # Fully custom
    python -m src.sem_insights.run_sem_insights \\
        --input     tests/data/hit_data.tab \\
        --output    output/sem_insights_report.csv \\
        --s3-bucket skp-processed-dev-099622553872 \\
        --run-id    my-pipeline-run-001
"""

import argparse
import csv
import io
import logging
import os
import sys
from datetime import datetime, timezone

# Allow running as a script from the project root without installing the package.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from src.sem_insights.sem_extractor import SemExtractor  # noqa: E402

logger = logging.getLogger(__name__)

# Account ID suffix baked into every bucket name in this project.
_ACCOUNT_ID = "099622553872"

# Processed-bucket name pattern — mirrors config.yaml naming convention.
_PROCESSED_BUCKET = "skp-processed-{env}-" + _ACCOUNT_ID

# Hive partition prefix inside the processed bucket.
# Matches the main pipeline: processed/{phase}/year=Y/month=M/day=D/
_S3_PREFIX_PATTERN = (
    "processed/sem_insights/"
    "year={year}/month={month:02d}/day={day:02d}/"
    "{filename}"
)


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def build_s3_key(filename: str, run_dt: datetime) -> str:
    """
    Return the Hive-partitioned S3 key for a SEM insights CSV.

    Example:
        processed/sem_insights/year=2026/month=02/day=25/sem_insights_20260225.csv
    """
    return _S3_PREFIX_PATTERN.format(
        year=run_dt.year,
        month=run_dt.month,
        day=run_dt.day,
        filename=filename,
    )


def upload_to_s3(local_path: str, bucket: str, s3_key: str) -> str:
    """
    Upload a local file to S3 and return the full S3 URI.

    Uses boto3, which is already a project dependency (Lambda runtime + CI).
    Raises RuntimeError with a helpful message if boto3 is not available
    so the runner degrades gracefully when used in a pure-offline context.
    """
    try:
        import boto3
    except ImportError:
        raise RuntimeError(
            "boto3 is required for S3 upload. "
            "Run: pip install boto3  or pass --no-s3 to skip the upload."
        )

    client = boto3.client("s3")
    logger.info("Uploading to s3://%s/%s …", bucket, s3_key)
    client.upload_file(local_path, bucket, s3_key)
    s3_uri = f"s3://{bucket}/{s3_key}"
    logger.info("Upload complete → %s", s3_uri)
    return s3_uri


# ---------------------------------------------------------------------------
# Local I/O helpers
# ---------------------------------------------------------------------------

def load_rows(path: str, delimiter: str = "\t") -> list:
    """
    Read a tab-delimited hit-level file and return a list of row dicts.

    CRLF normalisation is applied before parsing so the file is safe to
    process regardless of the line-ending convention used by the source system.
    """
    with open(path, newline="", encoding="utf-8") as fh:
        content = fh.read().replace("\r\n", "\n").replace("\r", "\n")
    reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)
    rows = list(reader)
    logger.info("Loaded %d rows from %s", len(rows), path)
    return rows


def write_csv(rows: list, path: str) -> None:
    """
    Write SEM insight rows to a local CSV file.

    The output directory is created automatically if it does not exist.
    """
    if not rows:
        logger.warning("No SEM insight rows to write — output file not created.")
        return

    out_dir = os.path.dirname(path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=SemExtractor.SCHEMA_COLS)
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Wrote %d rows → %s", len(rows), path)


def print_preview(rows: list) -> None:
    """Pretty-print results to stdout so they are visible without opening a file."""
    if not rows:
        return

    col_widths = {col: len(col) for col in SemExtractor.SCHEMA_COLS}
    for row in rows:
        for col in SemExtractor.SCHEMA_COLS:
            col_widths[col] = max(col_widths[col], len(str(row[col])))

    header = "  ".join(col.ljust(col_widths[col]) for col in SemExtractor.SCHEMA_COLS)
    separator = "  ".join("-" * col_widths[col] for col in SemExtractor.SCHEMA_COLS)

    print("\nSEM Insights Preview")
    print("=" * len(separator))
    print(header)
    print(separator)
    for row in rows:
        print("  ".join(str(row[col]).ljust(col_widths[col]) for col in SemExtractor.SCHEMA_COLS))
    print()


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "SEM Insights standalone runner — extracts keyword performance "
            "from hit-level data and uploads to S3 with Hive partitioning."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input",
        default="tests/data/hit_data.tab",
        metavar="PATH",
        help="Path to the hit-level .tab input file  (default: tests/data/hit_data.tab)",
    )
    parser.add_argument(
        "--output",
        default=None,
        metavar="PATH",
        help="Local path for the output CSV  (default: output/sem_insights_YYYYMMDD.csv)",
    )
    parser.add_argument(
        "--env",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Target environment — determines the S3 bucket  (default: dev)",
    )
    parser.add_argument(
        "--s3-bucket",
        default=None,
        metavar="BUCKET",
        help=(
            "Override the S3 bucket name.  "
            "Defaults to skp-processed-{env}-" + _ACCOUNT_ID
        ),
    )
    parser.add_argument(
        "--no-s3",
        action="store_true",
        help="Skip S3 upload — write local CSV only (useful for offline runs / CI)",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        metavar="ID",
        help="Audit run ID written into every output row  (default: auto UUID4)",
    )
    parser.add_argument(
        "--delimiter",
        default="\t",
        metavar="CHAR",
        help="Input file column delimiter  (default: tab)",
    )
    parser.add_argument(
        "--no-preview",
        action="store_true",
        help="Suppress the stdout table preview",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Capture run timestamp once so local filename and S3 key are in sync.
    run_dt = datetime.now(timezone.utc)
    date_str = run_dt.strftime("%Y%m%d")
    filename = f"sem_insights_{date_str}.csv"

    # Resolve local output path.
    if args.output is None:
        args.output = os.path.join("output", filename)

    # Resolve S3 bucket.
    s3_bucket = args.s3_bucket or _PROCESSED_BUCKET.format(env=args.env)
    s3_key = build_s3_key(filename, run_dt)
    s3_uri = f"s3://{s3_bucket}/{s3_key}"

    logger.info("SEM Insights PoC starting")
    logger.info("  input      : %s", args.input)
    logger.info("  local out  : %s", args.output)
    logger.info("  s3 target  : %s", "skipped (--no-s3)" if args.no_s3 else s3_uri)
    logger.info("  run_id     : %s", args.run_id or "(auto)")

    # Load → extract.
    rows = load_rows(args.input, delimiter=args.delimiter)
    if not rows:
        logger.error("Input file is empty or could not be parsed: %s", args.input)
        return 1

    extractor = SemExtractor(run_id=args.run_id)
    results = extractor.extract(rows)
    logger.info("Extracted %d SEM insight rows", len(results))

    # Write local CSV first — S3 upload uses the local file.
    write_csv(results, args.output)

    # Upload to S3 unless caller opted out.
    if not args.no_s3:
        if not results:
            logger.warning("No rows extracted — skipping S3 upload.")
        else:
            try:
                upload_to_s3(args.output, s3_bucket, s3_key)
            except Exception as exc:
                logger.error("S3 upload failed: %s", exc)
                logger.error(
                    "The local CSV is intact at %s — re-run with AWS credentials "
                    "or copy manually with: aws s3 cp %s %s",
                    args.output, args.output, s3_uri,
                )
                return 1

    if not args.no_preview:
        print_preview(results)

    return 0


if __name__ == "__main__":
    sys.exit(main())

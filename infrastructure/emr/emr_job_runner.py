"""
emr_job_runner.py
=================
Submits the SKP PySpark pipeline as an EMR Serverless job.
Called by the CD pipeline and can also be run manually.

Usage:
  python infrastructure/emr/emr_job_runner.py \\
    --input  s3://skp-raw-dev-099622553872/raw/hit_data_phase3_jog.tab \\
    --output s3://skp-processed-dev-099622553872/processed/ \\
    --env    dev

Flow:
  1. Upload spark_skp_pipeline.py script to S3
  2. Submit EMR Serverless job run
  3. Poll until complete (success or failure)
  4. Print CloudWatch log location
  5. Return exit code 0 on success, 1 on failure
"""

import argparse
import boto3
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("emr_job_runner")

# ── Environment variable names ────────────────────────────────────────
ENV_APP_ID       = "EMR_SERVERLESS_APP_ID"
ENV_EXEC_ROLE    = "EMR_EXECUTION_ROLE_ARN"
ENV_RAW_BUCKET   = "RAW_BUCKET"
ENV_LOGS_BUCKET  = "LOGS_BUCKET"

# ── Script S3 location ────────────────────────────────────────────────
SCRIPT_LOCAL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "src", "processing", "spark_skp_pipeline.py"
)


def get_required_env(name):
    val = os.environ.get(name)
    if not val:
        raise EnvironmentError(
            f"Required environment variable '{name}' is not set.\n"
            f"Set it with: export {name}=<value>"
        )
    return val


def upload_script_to_s3(s3_client, bucket, script_path):
    """Upload the PySpark script to S3 so EMR Serverless can access it."""
    key = "scripts/spark_skp_pipeline.py"
    logger.info("Uploading script to s3://%s/%s", bucket, key)
    with open(script_path, "rb") as f:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=f.read(),
            ContentType="text/x-python",
        )
    return f"s3://{bucket}/{key}"


def submit_job(emr_client, app_id, exec_role_arn, script_uri,
               input_path, output_path, env, run_id):
    """Submit the PySpark job to EMR Serverless."""
    job_name = f"skp-pipeline-{env}-{run_id[:8]}"

    logger.info("Submitting EMR Serverless job: %s", job_name)
    logger.info("  Application: %s", app_id)
    logger.info("  Script:      %s", script_uri)
    logger.info("  Input:       %s", input_path)
    logger.info("  Output:      %s", output_path)

    response = emr_client.start_job_run(
        applicationId=app_id,
        executionRoleArn=exec_role_arn,
        name=job_name,
        jobDriver={
            "sparkSubmit": {
                "entryPoint": script_uri,
                "entryPointArguments": [
                    "--input",  input_path,
                    "--output", output_path,
                    "--env",    env,
                ],
                "sparkSubmitParameters": (
                    "--conf spark.executor.cores=4 "
                    "--conf spark.executor.memory=8g "
                    "--conf spark.driver.cores=2 "
                    "--conf spark.driver.memory=4g "
                    "--conf spark.sql.shuffle.partitions=200 "
                    "--conf spark.sql.adaptive.enabled=true "
                    "--conf spark.sql.adaptive.coalescePartitions.enabled=true "
                    "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer"
                ),
            }
        },
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{os.environ.get(ENV_LOGS_BUCKET, '')}/emr-logs/"
                }
            }
        },
        tags={
            "Project":     "SKP",
            "Environment": env,
            "RunId":       run_id,
            "ManagedBy":   "emr_job_runner",
        },
    )

    job_run_id = response["jobRunId"]
    logger.info("Job submitted. Job Run ID: %s", job_run_id)
    return job_run_id


def poll_job(emr_client, app_id, job_run_id, poll_interval=15, timeout=3600):
    """
    Poll EMR Serverless until job completes or times out.

    EMR Serverless job states:
      SUBMITTED → PENDING → SCHEDULED → RUNNING → SUCCESS / FAILED / CANCELLED
    """
    terminal_states = {"SUCCESS", "FAILED", "CANCELLED"}
    start_time = time.time()
    last_state = None

    logger.info("Polling job %s (interval: %ds, timeout: %ds)...", job_run_id, poll_interval, timeout)

    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout:
            logger.error("Job timed out after %ds.", timeout)
            return "TIMEOUT", None

        response  = emr_client.get_job_run(applicationId=app_id, jobRunId=job_run_id)
        job_run   = response["jobRun"]
        state     = job_run["state"]
        state_msg = job_run.get("stateDetails", "")

        if state != last_state:
            logger.info("Job state: %s %s", state, f"— {state_msg}" if state_msg else "")
            last_state = state

        if state in terminal_states:
            duration = elapsed
            logger.info("Job finished in %.0fs with state: %s", duration, state)
            return state, job_run

        time.sleep(poll_interval)


def print_summary(job_run, job_run_id, app_id, logs_bucket, env):
    """Print job summary and CloudWatch log location."""
    state = job_run["state"] if job_run else "TIMEOUT"
    logger.info("=" * 60)
    logger.info("EMR Serverless Job Summary")
    logger.info("=" * 60)
    logger.info("Job Run ID:  %s", job_run_id)
    logger.info("State:       %s", state)
    if job_run:
        logger.info("Created:     %s", job_run.get("createdAt", ""))
        logger.info("Updated:     %s", job_run.get("updatedAt", ""))
    if logs_bucket:
        logger.info("Logs:        s3://%s/emr-logs/applications/%s/jobs/%s/",
                    logs_bucket, app_id, job_run_id)
    logger.info("=" * 60)


def run(input_path, output_path, env="dev"):
    """Main entry point."""
    import uuid
    run_id = str(uuid.uuid4())

    # Validate required env vars
    app_id        = get_required_env(ENV_APP_ID)
    exec_role_arn = get_required_env(ENV_EXEC_ROLE)
    raw_bucket    = get_required_env(ENV_RAW_BUCKET)
    logs_bucket   = os.environ.get(ENV_LOGS_BUCKET, "")

    region = os.environ.get("AWS_REGION", "us-east-1")

    s3_client  = boto3.client("s3",              region_name=region)
    emr_client = boto3.client("emr-serverless",  region_name=region)

    # Upload script
    script_uri = upload_script_to_s3(s3_client, raw_bucket, SCRIPT_LOCAL_PATH)

    # Submit job
    job_run_id = submit_job(
        emr_client, app_id, exec_role_arn,
        script_uri, input_path, output_path, env, run_id
    )

    # Poll to completion
    final_state, job_run = poll_job(emr_client, app_id, job_run_id)

    # Summary
    print_summary(job_run, job_run_id, app_id, logs_bucket, env)

    return 0 if final_state == "SUCCESS" else 1


def parse_args():
    parser = argparse.ArgumentParser(description="Submit SKP PySpark job to EMR Serverless")
    parser.add_argument("--input",  required=True, help="S3 input file path")
    parser.add_argument("--output", required=True, help="S3 output directory")
    parser.add_argument("--env",    default="dev", choices=["dev", "staging", "prod"])
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    sys.exit(run(args.input, args.output, args.env))

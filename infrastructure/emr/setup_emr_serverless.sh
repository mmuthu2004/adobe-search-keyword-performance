#!/bin/bash
# =============================================================================
# setup_emr_serverless.sh
# =============================================================================
# Creates the complete EMR Serverless infrastructure for the SKP pipeline.
#
# What this script creates:
#   1. EMR Serverless Application (Spark 3.4)
#   2. IAM execution role with S3 + CloudWatch permissions
#   3. IAM trust policy for EMR Serverless
#   4. Exports environment variables needed by emr_job_runner.py
#
# Usage:
#   chmod +x infrastructure/emr/setup_emr_serverless.sh
#   ./infrastructure/emr/setup_emr_serverless.sh
#
# Prerequisites:
#   - AWS CLI configured (aws configure)
#   - S3 buckets already created (run setup_s3.sh first)
#   - Sufficient IAM permissions
# =============================================================================

set -e  # Exit on any error

# ── Configuration ─────────────────────────────────────────────────────
AWS_REGION="us-east-1"
APP_NAME="skp-search-keyword-performance"
ROLE_NAME="skp-emr-execution-role"
ENV="dev"

# Get account ID dynamically
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
RAW_BUCKET="skp-raw-dev-${ACCOUNT_ID}"
PROCESSED_BUCKET="skp-processed-dev-${ACCOUNT_ID}"
LOGS_BUCKET="skp-logs-dev-${ACCOUNT_ID}"

echo "============================================================"
echo "SKP EMR Serverless Setup"
echo "============================================================"
echo "Account:   ${ACCOUNT_ID}"
echo "Region:    ${AWS_REGION}"
echo "Raw:       ${RAW_BUCKET}"
echo "Processed: ${PROCESSED_BUCKET}"
echo "Logs:      ${LOGS_BUCKET}"
echo "============================================================"

# ── Step 1: Create IAM Trust Policy ───────────────────────────────────
echo ""
echo "[1/4] Creating IAM trust policy..."

cat > /tmp/emr-trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "emr-serverless.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# ── Step 2: Create IAM Execution Role ────────────────────────────────
echo "[2/4] Creating IAM execution role: ${ROLE_NAME}..."

# Check if role already exists
if aws iam get-role --role-name ${ROLE_NAME} 2>/dev/null; then
    echo "  Role already exists — skipping creation."
else
    aws iam create-role \
        --role-name ${ROLE_NAME} \
        --assume-role-policy-document file:///tmp/emr-trust-policy.json \
        --description "EMR Serverless execution role for SKP pipeline"
    echo "  Role created."
fi

# Create S3 access policy for EMR
cat > /tmp/emr-s3-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadRawBucket",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::${RAW_BUCKET}",
        "arn:aws:s3:::${RAW_BUCKET}/*"
      ]
    },
    {
      "Sid": "WriteProcessedBucket",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::${PROCESSED_BUCKET}",
        "arn:aws:s3:::${PROCESSED_BUCKET}/*"
      ]
    },
    {
      "Sid": "WriteLogsBucket",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::${LOGS_BUCKET}",
        "arn:aws:s3:::${LOGS_BUCKET}/*"
      ]
    },
    {
      "Sid": "EMRServerlessGlue",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetTable",
        "glue:GetPartitions"
      ],
      "Resource": "*"
    }
  ]
}
EOF

# Create and attach the policy
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/skp-emr-s3-policy"

if aws iam get-policy --policy-arn ${POLICY_ARN} 2>/dev/null; then
    echo "  S3 policy already exists — updating..."
    # Create new version
    aws iam create-policy-version \
        --policy-arn ${POLICY_ARN} \
        --policy-document file:///tmp/emr-s3-policy.json \
        --set-as-default
else
    aws iam create-policy \
        --policy-name skp-emr-s3-policy \
        --policy-document file:///tmp/emr-s3-policy.json \
        --description "S3 access policy for SKP EMR Serverless jobs"
    echo "  S3 policy created."
fi

aws iam attach-role-policy \
    --role-name ${ROLE_NAME} \
    --policy-arn ${POLICY_ARN}
echo "  S3 policy attached."

# ── Step 3: Create EMR Serverless Application ─────────────────────────
echo ""
echo "[3/4] Creating EMR Serverless application: ${APP_NAME}..."

# Check if application already exists
EXISTING_APP_ID=$(aws emr-serverless list-applications \
    --region ${AWS_REGION} \
    --query "applications[?name=='${APP_NAME}' && state!='TERMINATED'].applicationId" \
    --output text 2>/dev/null)

if [ -n "${EXISTING_APP_ID}" ]; then
    echo "  Application already exists: ${EXISTING_APP_ID}"
    APP_ID=${EXISTING_APP_ID}
else
    APP_ID=$(aws emr-serverless create-application \
        --name ${APP_NAME} \
        --type SPARK \
        --release-label emr-7.0.0 \
        --region ${AWS_REGION} \
        --initial-capacity '{
            "DRIVER": {
                "workerCount": 1,
                "workerConfiguration": {
                    "cpu": "2vCPU",
                    "memory": "4GB",
                    "disk": "20GB"
                }
            },
            "EXECUTOR": {
                "workerCount": 5,
                "workerConfiguration": {
                    "cpu": "4vCPU",
                    "memory": "8GB",
                    "disk": "20GB"
                }
            }
        }' \
        --maximum-capacity '{
            "cpu": "40vCPU",
            "memory": "80GB",
            "disk": "200GB"
        }' \
        --auto-stop-configuration '{
            "enabled": true,
            "idleTimeoutMinutes": 15
        }' \
        --query "applicationId" \
        --output text)
    echo "  Application created: ${APP_ID}"
fi

# Get execution role ARN
EXEC_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

# ── Step 4: Wait for application to be ready ──────────────────────────
echo ""
echo "[4/4] Waiting for application to reach CREATED state..."

for i in {1..20}; do
    STATE=$(aws emr-serverless get-application \
        --application-id ${APP_ID} \
        --region ${AWS_REGION} \
        --query "application.state" \
        --output text)
    echo "  State: ${STATE}"
    if [ "${STATE}" = "CREATED" ]; then
        echo "  Application is ready."
        break
    fi
    sleep 10
done

# ── Export environment variables ──────────────────────────────────────
echo ""
echo "============================================================"
echo "SETUP COMPLETE"
echo "============================================================"
echo ""
echo "Run these commands to set environment variables:"
echo ""
echo "  set EMR_SERVERLESS_APP_ID=${APP_ID}"
echo "  set EMR_EXECUTION_ROLE_ARN=${EXEC_ROLE_ARN}"
echo "  set RAW_BUCKET=${RAW_BUCKET}"
echo "  set PROCESSED_BUCKET=${PROCESSED_BUCKET}"
echo "  set LOGS_BUCKET=${LOGS_BUCKET}"
echo "  set AWS_REGION=${AWS_REGION}"
echo ""
echo "Or add these to GitHub Secrets for CI/CD:"
echo "  EMR_SERVERLESS_APP_ID    = ${APP_ID}"
echo "  EMR_EXECUTION_ROLE_ARN   = ${EXEC_ROLE_ARN}"
echo ""
echo "To submit a test job:"
echo "  python infrastructure/emr/emr_job_runner.py \\"
echo "    --input  s3://${RAW_BUCKET}/raw/hit_data_phase3_jog.tab \\"
echo "    --output s3://${PROCESSED_BUCKET}/processed/ \\"
echo "    --env    dev"
echo "============================================================"

# Write to a local env file for convenience (gitignored)
cat > .env.emr << EOF
EMR_SERVERLESS_APP_ID=${APP_ID}
EMR_EXECUTION_ROLE_ARN=${EXEC_ROLE_ARN}
RAW_BUCKET=${RAW_BUCKET}
PROCESSED_BUCKET=${PROCESSED_BUCKET}
LOGS_BUCKET=${LOGS_BUCKET}
AWS_REGION=${AWS_REGION}
EOF

echo ""
echo "Environment variables also saved to: .env.emr (gitignored)"

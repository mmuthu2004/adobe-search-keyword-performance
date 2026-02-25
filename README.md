# Adobe Analytics — Search Keyword Performance Pipeline

> **Platform:** AWS Lambda + EMR Serverless
> **Language:** Python 3.12
> **Environment:** `us-east-1` | Account `099622553872`
> **Status:** Production-ready · Feature-complete · All tests passing

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Business Logic — Attribution Algorithm](#3-business-logic--attribution-algorithm)
4. [Repository Structure](#4-repository-structure)
5. [Module Reference](#5-module-reference)
6. [S3 Bucket Layout](#6-s3-bucket-layout)
7. [Configuration System](#7-configuration-system)
8. [Data Quality Framework](#8-data-quality-framework)
9. [Data Governance](#9-data-governance)
10. [Local Development Setup](#10-local-development-setup)
11. [Running Tests](#11-running-tests)
12. [Deployment](#12-deployment)
13. [CI/CD Pipeline](#13-cicd-pipeline)
14. [Infrastructure](#14-infrastructure)
15. [Security](#15-security)
16. [Troubleshooting](#16-troubleshooting)

---

## 1. Overview

This pipeline processes Adobe Analytics hit-level data to produce a **Search Keyword Performance** report — a ranked list of search engine keywords credited with driving revenue on the site.

### Problem Statement

Given a tab-delimited file of raw Adobe Analytics hit records, determine:
- Which search engine keywords drove visitors to the site
- Which of those visitors subsequently made a purchase
- How much revenue to attribute to each keyword

### Output

A tab-delimited file with three columns, sorted by Revenue descending:

```
Search Engine Domain    Search Keyword    Revenue
google.com              ipod              480.00
bing.com                zune              250.00
```

### Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Processing engine — small files | Pure Python (Lambda) | No cluster overhead; sub-second cold start |
| Processing engine — large files | PySpark (EMR Serverless) | Linear scale-out; serverless, no cluster management |
| Routing threshold | Config-driven (50 MB dev / 1024 MB prod) | Never hardcoded; change without code deploy |
| Attribution model | First-touch, session-based | First search engine visit per IP gets full revenue credit |
| Duplicate prevention | Timestamp in filename `_YYYYMMDD_HHMMSS` | Feed-frequency agnostic (daily, hourly, or any cadence) |
| Idempotency | Archive-as-registry | `archive/{phase}/{filename}` existence = already processed |
| Dead-letter | `rejected/` prefix | Duplicate or non-conforming files preserved, not deleted |
| Output partitioning | Hive-style (`processed/{phase}/year=YYYY/month=MM/day=DD/`) | Athena/Glue can auto-discover partitions |

---

## 2. Architecture

### Dual-Path Routing

Every `.tab` file dropped into `raw/{phase}/` triggers an S3 event notification → Lambda. Lambda inspects the file size and routes accordingly:

```
S3 raw/{phase}/*.tab
        │
        ▼
┌───────────────────┐
│   AWS Lambda      │  ← Entry point: lambda_handler.handler()
│                   │
│  1. Validate name │  ← _YYYYMMDD_HHMMSS required in prod
│  2. Check archive │  ← idempotency: already processed?
│  3. Check size    │  ← compare to threshold_mb
└───────────────────┘
        │
   ┌────┴────┐
   │         │
   ▼         ▼
< threshold  >= threshold
   │              │
   ▼              ▼
Pure Python    EMR Serverless
(in-Lambda)    (async PySpark)
   │              │
   ▼              ▼
Preprocess     spark_skp_pipeline.py
+ Analyze      (same logic, Spark API)
+ Write TSV         │
   │                ▼
   ▼         s3://processed/
s3://processed/     │
   │                ▼
   └──────┬─────────┘
          ▼
  archive/{phase}/{file}    ← on success
  dq-errors/{Y/M/D}/        ← rows that failed DQ
  dq-reports/{Y/M/D}/       ← per-run DQ summary JSON
  lineage/{date}/{run_id}   ← full audit record
```

### Pure Python Path (Lambda)

```
raw_content (string)
     │
     ▼
PreprocessingPipeline.run()
  ├─ CRLF normalisation
  ├─ Schema validation    → error_rows if columns missing
  ├─ Null validation      → error_rows if critical cols empty
  └─ Deduplication        → error_rows if composite key repeated
     │
     ▼  (clean_rows, error_rows, dq_report)
SearchKeywordAnalyzer.analyze()
  ├─ Pass 1: Build session map (first search engine hit per IP)
  └─ Pass 2: Attribute revenue (join purchases → sessions)
     │
     ▼  (list of {domain, keyword, revenue} dicts)
OutputWriter.write_local() → upload_to_s3()
     │
     ▼
s3://processed/{phase}/year=YYYY/month=MM/day=DD/YYYY-MM-DD_SearchKeywordPerformance.tab
```

### EMR Serverless Path (PySpark)

Lambda submits an async `StartJobRun` call and returns HTTP 202 immediately. The Spark job runs to completion independently:

```
Lambda (async handoff)
  └─ emr-serverless:StartJobRun
       --entry-point s3://.../scripts/spark_skp_pipeline.py
       --arguments --input, --output, --config-json, --archive-input
            │
            ▼
     EMR Serverless (PySpark 3.5 / EMR 7.0)
       Step 1: read_input()         → Spark DataFrame
       Step 2: validate_schema()    → fail fast
       Step 3: validate_nulls()     → fail fast
       Step 4: deduplicate()        → dropDuplicates()
       Step 5: build_session_map()  → Window function, first-touch per IP
       Step 6: extract_purchase_rows() → UDF: parse product_list
       Step 7: attribute_revenue()  → inner join on IP
       Step 8: aggregate_results()  → groupBy + sum + sort
       Step 9: write_output()       → coalesce(1) → single .tab file
      Step 10: archive input        → copy raw → archive, delete raw
      Step 11: write DQ report      → JSON to dq-reports/
```

---

## 3. Business Logic — Attribution Algorithm

### Session-Based First-Touch Attribution

**Attribution model:** The first search engine entry in a session gets 100% of the revenue credit for any purchase made in that session.

**Session key:** IP address (`ip` column). In production, replace with a visitor/cookie ID if available.

**Algorithm (two-pass):**

```
Pass 1 — Build session map
──────────────────────────
Sort all hits by hit_time_gmt ascending (chronological order).
For each row:
  if referrer hostname matches a known search engine domain:
    if ip NOT already in session_map:   ← first touch wins
      extract keyword from referrer URL query params (q, p, query, text, s, qs)
      session_map[ip] = {domain, keyword}

Pass 2 — Attribute revenue
──────────────────────────
For each row:
  if purchase event ID ('1') in event_list:
    if product_list is non-empty:
      revenue = sum of revenue values at index 3 in each product entry
      if revenue > 0:
        if ip in session_map:
          credit revenue to session_map[ip].domain + keyword

Aggregate
─────────
Group by (domain, keyword) → sum(revenue)
Sort by revenue descending
Round to 2 decimal places
```

### Referrer Parsing

```
referrer: https://www.google.com/search?q=ipod&ie=UTF-8
  hostname:  www.google.com
  normalize: google.com          ← strip subdomains: parts[-2:]
  domain set: {google.com, bing.com, yahoo.com, ...}  ← O(1) lookup
  keyword params (in order): q, p, query, text, s, qs
  keyword: ipod                  ← first matching param wins
  normalize: ipod.lower()        ← case normalization ON by default
```

### Product List Revenue Parsing

```
product_list: Electronics;iPod;1;290.00;event1;evar1,Accessories;Cable;2;15.00
  delimiter between products: ,
  delimiter within product:   ;
  revenue index (0-based):    3
  total revenue: 290.00 + 15.00 = 305.00
```

Revenue is only counted when **purchase event ID `1`** is present in `event_list`. This prevents counting revenue from non-purchase events (cart adds, wish lists, etc.).

---

## 4. Repository Structure

```
adobe-search-keyword-performance/
│
├── src/                                 ← All application source code
│   ├── lambda_handler.py                ← AWS Lambda entry point; routing logic
│   ├── config/
│   │   ├── config.yaml                  ← Master config (all defaults)
│   │   ├── config.dev.yaml              ← Dev overrides
│   │   ├── config.staging.yaml          ← Staging overrides
│   │   ├── config.prod.yaml             ← Prod overrides
│   │   └── config_loader.py             ← Deep-merge + env-var substitution
│   ├── preprocessing/
│   │   └── preprocessing_pipeline.py    ← Schema, null, dedup checks (stdlib only)
│   ├── processing/
│   │   ├── search_keyword_analyzer.py   ← Core attribution engine (pure Python)
│   │   └── spark_skp_pipeline.py        ← PySpark mirror of above; EMR entry point
│   ├── output/
│   │   └── writer.py                    ← TSV writer + S3 upload
│   ├── governance/
│   │   └── lineage_tracker.py           ← Per-run audit record → S3
│   └── utils/
│       └── pii_masker.py                ← SHA-256 masking for ip/user_agent
│
├── tests/
│   ├── data/
│   │   └── hit_data.tab                 ← 21-row ground truth sample file
│   ├── unit/
│   │   └── test_config_loader.py        ← Config loading and merging
│   ├── integration/
│   │   ├── test_end_to_end.py           ← Full pipeline against sample data
│   │   └── test_spark_parity.py         ← Spark output == Python output
│   └── generate_test_data.py            ← Script to regenerate test fixtures
│
├── infrastructure/
│   ├── cloudformation/
│   │   └── skp-stack.yaml               ← Lambda, IAM, S3 notifications, lifecycle rules
│   └── emr/
│       ├── emr_job_runner.py            ← EMR Serverless API wrapper
│       └── setup_emr_serverless.sh      ← One-time EMR application setup
│
├── .github/
│   └── workflows/
│       └── cicd.yml                     ← CI (test + build) + CD (deploy to Lambda)
│
├── requirements.txt                     ← Lambda runtime deps (boto3, pyyaml)
├── requirements-spark.txt               ← EMR/local Spark deps (pyspark, pytest)
├── .flake8                              ← Linting configuration
└── .gitignore
```

---

## 5. Module Reference

### `src/lambda_handler.py`

AWS Lambda entry point. Handles all routing, file validation, and pipeline orchestration.

| Function | Responsibility |
|---|---|
| `handler(event, context)` | Main entry point; parses S3 event; orchestrates all steps |
| `_get_phase_folder(key)` | Extracts phase tag from S3 key (path-first, keyword fallback) |
| `_has_date_in_filename(name)` | Validates `_YYYYMMDD_HHMMSS` naming convention |
| `_is_already_processed(...)` | `head_object` on `archive/` key — idempotency check |
| `_reject_input(...)` | Moves file to `rejected/{phase}/year=.../month=.../day=.../` |
| `_archive_input(...)` | Moves file to `archive/{phase}/` on successful completion |
| `_move_s3_object(...)` | Shared copy+delete helper (S3 has no native rename) |
| `_handoff_to_emr(...)` | Submits EMR Serverless job; returns HTTP 202 immediately |
| `_trigger_emr_job(...)` | `emr-serverless:StartJobRun` API call |
| `_write_dq_errors(...)` | Writes PII-masked error rows as CSV to `dq-errors/` |
| `_write_dq_report(...)` | Writes DQ summary JSON to `dq-reports/` |
| `_is_near_timeout(...)` | Safety: if < 60s remaining, route to EMR instead of failing |
| `_parse_event(event)` | Handles S3 Records format or direct `{bucket, key}` dict |
| `_error_response(...)` | Standardised `{statusCode, body}` error shape |

### `src/preprocessing/preprocessing_pipeline.py`

Pure Python (zero external dependencies). Runs in Lambda.

```python
rows, error_rows, dq_report = PreprocessingPipeline(cfg).run(raw_content)
```

| Step | Check | Fatal? |
|---|---|---|
| 0 | CRLF normalisation | Never — silent fix |
| 1 | CSV parsing | Never — propagates naturally |
| 2 | Schema validation | Yes (configurable: `fail_on_schema_error`) |
| 3 | Null validation | Yes (configurable: `fail_on_null_critical`) |
| 4 | Deduplication | Never — duplicates become error rows |

### `src/processing/search_keyword_analyzer.py`

Core business logic. Pure Python, no external deps. Runs in Lambda.

```python
results = SearchKeywordAnalyzer(cfg).analyze(rows)
# returns: [{"Search Engine Domain": ..., "Search Keyword": ..., "Revenue": ...}, ...]
```

### `src/processing/spark_skp_pipeline.py`

PySpark mirror of the Lambda path. Runs on EMR Serverless.

```bash
spark-submit spark_skp_pipeline.py \
  --input  s3://skp-raw-dev-099622553872/raw/phase3/hit_data_phase3_20260225_143000.tab \
  --output s3://skp-processed-dev-099622553872/processed/ \
  --env    dev \
  --config-json '{"purchase_event_id": "1", ...}'  \
  --archive-input s3://...  # archived on success
```

**Parity guarantee:** The Spark UDFs (`extract_search_info`, `parse_revenue`) contain the identical Python logic as `SearchKeywordAnalyzer`. The `test_spark_parity.py` test enforces byte-level output match between the two paths.

### `src/config/config_loader.py`

```python
from config.config_loader import load_config
cfg = load_config()                   # uses APP_ENV env var (default: dev)
cfg = load_config(env="prod")         # explicit override

cfg.get("processing.small_file_threshold_mb")   # returns value or None
cfg.get("logging.level", default="INFO")        # returns value or default
cfg.require("business_rules.purchase_event_id") # raises ConfigError if missing
```

Merge strategy: `config.yaml` (base) ← deep-merged with `config.{env}.yaml` (overrides). Lists are replaced, not appended. `${ENV_VAR}` placeholders are substituted at load time.

### `src/governance/lineage_tracker.py`

Writes a JSON audit record to `s3://skp-logs-dev-099622553872/lineage/{date}/{run_id}.json` after every run.

```json
{
  "run_id": "7ea07afb-2c32-46cd-89d1-6d684df11137",
  "execution_date": "2026-02-25T14:30:00+00:00",
  "input_file": "s3://skp-raw-dev.../raw/phase1/hit_data_phase1_20260225_143000.tab",
  "input_rows": 21,
  "output_file": "s3://skp-processed-dev.../processed/phase1/year=2026/month=02/day=25/...",
  "output_rows": 2,
  "engine_used": "python",
  "dq_passed": true,
  "duration_seconds": 0.84,
  "git_commit": "2bd01b4",
  "attribution_model": "first_touch"
}
```

### `src/utils/pii_masker.py`

SHA-256 masks `ip` and `user_agent` before writing to DQ error files or logs. Original values never leave the Lambda execution context.

```python
from utils.pii_masker import mask
mask("192.168.1.100")  # → "a3f5d2c1e8b94f7a..."
```

---

## 6. S3 Bucket Layout

### Raw Bucket: `skp-raw-dev-099622553872`

```
skp-raw-dev-099622553872/
│
├── raw/
│   ├── phase1/          ← Drop zone for phase 1 data (< 50 MB → Lambda)
│   ├── phase2/          ← Drop zone for phase 2 data (< 50 MB → Lambda)
│   └── phase3/          ← Drop zone for phase 3 data (>= 50 MB → EMR)
│
├── archive/
│   ├── phase1/          ← Successfully processed files (idempotency registry)
│   ├── phase2/
│   └── phase3/
│
├── rejected/
│   └── phase1/
│       └── year=2026/month=02/day=25/
│           └── hit_data_phase1_20260225_143000.tab   ← duplicate/non-conforming
│
├── dq-errors/
│   └── 2026/02/25/
│       └── hit_data_phase1_..._dq_errors.csv         ← PII-masked bad rows
│
├── dq-reports/
│   └── 2026/02/25/
│       └── hit_data_phase1_..._dq_report.json        ← per-run DQ summary
│
├── scripts/
│   └── spark_skp_pipeline.py                         ← PySpark script for EMR
│
└── lambda/
    └── skp_lambda.zip                                ← deployment package
```

### Processed Bucket: `skp-processed-dev-099622553872`

```
skp-processed-dev-099622553872/
└── processed/
    └── {phase}/
        └── year=YYYY/
            └── month=MM/
                └── day=DD/
                    └── YYYY-MM-DD_SearchKeywordPerformance.tab
```

Hive-style partitioning enables AWS Athena and Glue to auto-discover partitions without any `MSCK REPAIR TABLE` commands.

### Logs Bucket: `skp-logs-dev-099622553872`

```
skp-logs-dev-099622553872/
└── lineage/
    └── 2026-02-25/
        └── {run_id}.json
```

### File Naming Convention

Source systems **must** include a full timestamp in the filename:

```
hit_data_phase1_YYYYMMDD_HHMMSS.tab
         ↑       ↑         ↑
         any    date      time
         name  (8 digits) (6 digits)
```

**Why full timestamp (not date-only)?** The feed frequency is unknown at design time. A full timestamp (`_YYYYMMDD_HHMMSS`) is unique across daily, hourly, or any other cadence without requiring a naming convention change later.

Files without a timestamp are rejected to `rejected/` in production (`enforce_filename_date: true` in `config.yaml`). Enforcement is off in dev for test flexibility.

---

## 7. Configuration System

### Config File Hierarchy

```
config.yaml          ← Base: all defaults, all business rules
    ↑ deep-merged with
config.dev.yaml      ← Dev: lower threshold, no filename enforcement, local lineage
config.staging.yaml  ← Staging: staging bucket names
config.prod.yaml     ← Prod: prod bucket names (account ID suffix required)
```

### Key Configuration Values

```yaml
# config.yaml (excerpt)

business_rules:
  purchase_event_id: "1"           # Adobe event that actualizes revenue
  search_engine_domains:           # Referrer domains treated as search engines
    - google.com
    - yahoo.com
    - bing.com
    - msn.com
    - ask.com
    - aol.com
  keyword_query_params:            # URL params checked in order for keyword
    - q                            # google.com / bing.com
    - p                            # yahoo.com
    - query
  attribution_model: first_touch   # First search engine hit per IP gets credit
  revenue_column_index: 3          # 0-based index in semicolon-delimited product_list

processing:
  small_file_threshold_mb: 1024    # >= this → EMR Serverless; < this → Lambda

data_quality:
  enforce_filename_date: true      # _YYYYMMDD_HHMMSS required in filename
  fail_on_schema_error: true       # halt if required columns missing
  fail_on_null_critical: true      # halt if ip/referrer/hit_time_gmt is empty
  critical_columns: [hit_time_gmt, ip, referrer]
  duplicate_key_columns: [hit_time_gmt, ip, page_url]
```

### Environment Variable Substitution

Sensitive values use `${VAR_NAME}` placeholders in `config.yaml` and are injected at runtime via environment variables — never stored in the config files:

```yaml
aws:
  emr_serverless:
    application_id:    "${EMR_SERVERLESS_APP_ID}"
    execution_role_arn: "${EMR_EXECUTION_ROLE_ARN}"
  cloudwatch:
    alarm_email: "${ALARM_EMAIL}"
```

### Dev Overrides (`config.dev.yaml`)

```yaml
processing:
  small_file_threshold_mb: 50    # files > 50 MB → EMR (phase3 at ~58 MB hits this)

data_quality:
  enforce_filename_date: false   # test files without timestamps work in dev

logging:
  level: DEBUG
  structured: false              # human-readable logs locally

lineage:
  store: local                   # write lineage to stdout instead of S3
```

---

## 8. Data Quality Framework

Every pipeline run executes four DQ checks in order. Results are collected into a `dq_report` dict and written as JSON to `dq-reports/` on the raw bucket.

### Check Summary

| # | Check | Scope | Fatal? | Output |
|---|---|---|---|---|
| 1 | Schema validation | File-level: all required columns present | Yes (config) | All rows tagged as errors |
| 2 | Null validation | Row-level: critical columns non-empty | Yes (config) | Failing rows → `error_rows` |
| 3 | Deduplication | Row-level: composite key `hit_time_gmt + ip + page_url` | Never | Duplicate rows → `error_rows` |
| 4 | Null rate warning | Column-level: non-critical columns | Never | `WARNING` log emitted |

### DQ Report Schema

```json
{
  "run_id": "7ea07afb-...",
  "execution_date": "2026-02-25T14:30:00+00:00",
  "input_file": "s3://...",
  "overall_passed": true,
  "rows_in": 21,
  "rows_out": 21,
  "error_count": 0,
  "checks": [
    {
      "check": "schema_validation",
      "passed": true,
      "missing": [],
      "actual_columns": ["hit_time_gmt", "ip", ...]
    },
    {
      "check": "null_validation",
      "passed": true,
      "critical_failures": 0,
      "total_rows": 21
    },
    {
      "check": "duplicate_check",
      "passed": true,
      "duplicate_count": 0,
      "rows_before": 21,
      "rows_after": 21
    }
  ]
}
```

### DQ Error Records

Rows that fail any check are written as a PII-masked CSV to `dq-errors/`. The `ip` and `user_agent` columns are SHA-256 hashed before writing — original values never leave the execution environment.

```csv
_error_reason,hit_time_gmt,ip,user_agent,...
null_critical_column: ip,1234567890,EMPTY,a3f5d2c1e8b94f7a...,...
duplicate_row: key=(...),1234567890,b2e4c8a1...,d5f7e9b2...,...
```

---

## 9. Data Governance

### Data Lineage

Every pipeline run writes a lineage record to `s3://skp-logs-{env}/lineage/{date}/{run_id}.json`. The record includes:
- Input/output S3 URIs and row counts
- Engine used (`python` or `pyspark`)
- DQ pass/fail
- Wall-clock duration
- Git commit SHA (for code version traceability)
- Attribution model used

### PII Handling

The columns `ip` and `user_agent` are classified as PII per `config.yaml`:

```yaml
data_quality:
  pii_columns: [ip, user_agent]
  pii_hash_algorithm: sha256
```

PII values are:
- **Truncated to 8 characters** in DEBUG logs (partial, non-reversible)
- **SHA-256 hashed** before writing to DQ error files
- **Never written** to the final output (`_SearchKeywordPerformance.tab`)

### Idempotency

The pipeline is idempotent by design. If the same file is delivered twice:

1. Lambda checks `archive/{phase}/{filename}` with a `head_object` call
2. If the file exists in archive → the delivery is a duplicate
3. The file is moved to `rejected/{phase}/year=.../month=.../day=.../`
4. Lambda returns HTTP 200 (not an error — expected outcome)

This design ensures safe re-delivery without double-counting revenue.

---

## 10. Local Development Setup

### Prerequisites

- Python 3.12
- AWS CLI configured (`aws configure`)
- Java 11+ (required by PySpark for local Spark tests)

### Setup

```bash
# Clone the repository
git clone https://github.com/mmuthu2004/adobe-search-keyword-performance.git
cd adobe-search-keyword-performance

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate          # Linux/macOS
venv\Scripts\activate             # Windows

# Install Lambda dependencies
pip install -r requirements.txt

# Install Spark dependencies (for local parity tests)
pip install -r requirements-spark.txt
```

### Environment Variables

```bash
# Minimum required for local testing
export APP_ENV=dev
export PROCESSED_BUCKET=skp-processed-dev-099622553872
export LOGS_BUCKET=skp-logs-dev-099622553872

# Required for EMR tests
export EMR_SERVERLESS_APP_ID=<your-emr-app-id>
export EMR_EXECUTION_ROLE_ARN=arn:aws:iam::099622553872:role/skp-emr-execution-role

# Required for SNS alerts (optional in dev)
export ALARM_EMAIL=your@email.com
```

### Running the Pipeline Locally

```bash
# Unit test style — invoke handler directly
PYTHONPATH=src python -c "
import os; os.environ['APP_ENV'] = 'dev'
from lambda_handler import handler
result = handler({'bucket': 'skp-raw-dev-099622553872', 'key': 'raw/phase1/hit_data.tab'})
print(result)
"

# Spark pipeline locally (requires PySpark installed)
spark-submit src/processing/spark_skp_pipeline.py \
  --input  tests/data/hit_data.tab \
  --output /tmp/skp_output/ \
  --local
```

---

## 11. Running Tests

### Test Suite Overview

| Suite | Location | What it tests |
|---|---|---|
| Unit | `tests/unit/` | Config loader: loading, merging, env var substitution, error handling |
| Integration | `tests/integration/test_end_to_end.py` | Full pipeline against 21-row sample; ground truth verification |
| Parity | `tests/integration/test_spark_parity.py` | Spark output matches Python output byte-for-byte |

### Running Tests

```bash
# All tests
PYTHONPATH=src python -m pytest tests/ -v

# Unit tests only
PYTHONPATH=src python -m pytest tests/unit/ -v

# Integration tests only
PYTHONPATH=src python -m pytest tests/integration/test_end_to_end.py -v

# Parity tests (requires PySpark — slow, ~60s)
PYTHONPATH=src python -m pytest tests/integration/test_spark_parity.py -v
```

### Ground Truth

The sample file `tests/data/hit_data.tab` (21 rows) has documented ground truth:

| Domain | Keyword | Expected Revenue |
|---|---|---|
| `google.com` | `ipod` | `480.00` (Ipod $290 + ipod $190 — case normalized) |
| `bing.com` | `zune` | `250.00` |

Total revenue: **$730.00**

`yahoo.com` had a session but no subsequent purchase → **must not appear in output**.

### Code Quality

```bash
# Flake8 linting (same config as CI)
flake8 src/ --max-line-length=120 --exclude=__pycache__
```

---

## 12. Deployment

### Lambda Deployment Package

```bash
# Build
mkdir -p lambda_package
cp -r src/* lambda_package/
pip install pyyaml \
  --platform manylinux2014_x86_64 \
  --target lambda_package/ \
  --implementation cp \
  --python-version 3.12 \
  --only-binary=:all: \
  --upgrade
cd lambda_package && zip -r ../skp_lambda.zip . -x "*.pyc" -x "*/__pycache__/*" && cd ..

# Deploy
aws lambda update-function-code \
  --function-name skp-search-keyword-performance \
  --zip-file fileb://skp_lambda.zip
```

### Spark Script Update

When `spark_skp_pipeline.py` changes, the S3 copy must be updated manually (or via CI/CD):

```bash
aws s3 cp src/processing/spark_skp_pipeline.py \
  s3://skp-raw-dev-099622553872/scripts/spark_skp_pipeline.py
```

### Required AWS Secrets (GitHub Actions)

| Secret | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | IAM user access key with Lambda update permission |
| `AWS_SECRET_ACCESS_KEY` | IAM user secret key |

> **Note:** OIDC (OpenID Connect) federated credentials are preferred over long-lived access keys. Migrate to `aws-actions/configure-aws-credentials` with OIDC when possible.

### Required Lambda Environment Variables

| Variable | Example | Description |
|---|---|---|
| `APP_ENV` | `dev` | Selects config override file |
| `RAW_BUCKET` | `skp-raw-dev-099622553872` | Source and archive bucket |
| `PROCESSED_BUCKET` | `skp-processed-dev-099622553872` | Output bucket |
| `LOGS_BUCKET` | `skp-logs-dev-099622553872` | Lineage records bucket |
| `EMR_SERVERLESS_APP_ID` | `00g3mrt47suvu80b` | EMR Serverless application ID |
| `EMR_EXECUTION_ROLE_ARN` | `arn:aws:iam::...` | IAM role for EMR job execution |

---

## 13. CI/CD Pipeline

Pipeline defined in `.github/workflows/cicd.yml`.

### Trigger Rules

| Trigger | Jobs run |
|---|---|
| Push to `feature/**`, `bugfix/**`, `hotfix/**` | CI only |
| Pull request to `main` or `develop` | CI only |
| Push to `main` | CI + CD (deploy) |
| Push to `develop` | CI only |

### CI Job (every push)

```
1. Checkout
2. Set up Python 3.12
3. Install dependencies (pyyaml, boto3, pytest, flake8)
4. flake8 code quality check
5. Unit tests   (PYTHONPATH=src pytest tests/unit/)
6. Integration tests (PYTHONPATH=src pytest tests/integration/)
7. Build Lambda deployment package (zip)
8. Upload zip as GitHub Actions artifact
```

### CD Job (push to `main` only, after CI passes)

```
1. Download Lambda zip artifact from CI job
2. Configure AWS credentials (from GitHub Secrets)
3. Upload zip to s3://skp-raw-dev-099622553872/lambda/skp_lambda.zip
4. aws lambda update-function-code + wait function-updated
5. Smoke test: upload hit_data.tab → invoke Lambda → verify statusCode=200
6. Verify output: check processed/ for today's file, confirm google.com/$480
```

---

## 14. Infrastructure

### CloudFormation Stack (`infrastructure/cloudformation/skp-stack.yaml`)

Provisions all AWS resources in a single stack:

| Resource | Purpose |
|---|---|
| `AWS::Lambda::Function` | `skp-search-keyword-performance` |
| `AWS::IAM::Role` | Lambda execution role |
| `AWS::IAM::ManagedPolicy` | S3 + EMR Serverless permissions |
| `AWS::S3::BucketNotification` | raw/*.tab → Lambda trigger |
| `AWS::Lambda::Permission` | Allows S3 to invoke Lambda |
| `AWS::Logs::LogGroup` | `/skp/search-keyword-performance` CloudWatch log group |
| `AWS::S3::BucketLifecycleConfiguration` | archive/ → Glacier after 90d; dq-errors/ → expire after 30d |

### IAM Permissions

The Lambda execution role has least-privilege S3 permissions:

| Action | Resource | Purpose |
|---|---|---|
| `s3:GetObject`, `s3:HeadObject`, `s3:DeleteObject` | `raw/*` | Read and remove input files |
| `s3:PutObject`, `s3:DeleteObject` | `raw/archive/*` | Archive processed files |
| `s3:PutObject` | `raw/dq-errors/*` | Write DQ error records |
| `s3:PutObject` | `raw/rejected/*` | Write rejected/duplicate files |
| `s3:PutObject` | `processed/*`, `logs/*` | Write output and lineage |
| `s3:ListBucket` | raw, processed, logs buckets | Existence checks |
| `emr-serverless:StartJobRun`, `emr-serverless:TagResource` | EMR application | Trigger Spark jobs |
| `iam:PassRole` | EMR execution role | Pass role to EMR |

### EMR Serverless

```
Application ID:    Set via EMR_SERVERLESS_APP_ID env var
EMR Release:       emr-7.0.0
Runtime:           Spark 3.5
Driver:            1 worker × 2 vCPU × 4 GB
Executors:         5 workers × 4 vCPU × 8 GB (initial capacity)
Script location:   s3://skp-raw-dev-099622553872/scripts/spark_skp_pipeline.py
```

### Setup EMR Serverless (first time)

```bash
bash infrastructure/emr/setup_emr_serverless.sh
```

---

## 15. Security

| Control | Implementation |
|---|---|
| PII masking | `ip` and `user_agent` SHA-256 hashed before any S3 write or log |
| Secrets management | All ARNs, IDs, and credentials via environment variables or AWS Secrets (never in code or config) |
| IAM least privilege | Lambda role scoped to exact S3 prefixes and operations required |
| S3 event filter | Suffix `.tab` filter prevents non-data files (e.g., `.keep`, `.json`) from triggering Lambda |
| Input validation | Filename convention enforced before any file is read |
| Idempotency | Archive-as-registry prevents duplicate processing of the same delivery |
| Lifecycle management | DQ error files expire after 30 days; archive transitions to Glacier after 90 days |

---

## 16. Troubleshooting

### Lambda logs

```bash
# Tail recent Lambda logs
MSYS_NO_PATHCONV=1 aws logs tail /skp/search-keyword-performance \
  --follow --format short
```

### File was not processed (no output)

1. Check Lambda was triggered: look for `"=== SKP Lambda Handler Start ==="` in CloudWatch
2. Check for filename rejection: look for `"rejected"` or `"enforce_filename_date"` in logs
3. Check for duplicate: look for `"already processed"` in logs — file may already be in `archive/`
4. Check DQ report: `s3://skp-raw-dev-099622553872/dq-reports/`

### EMR job failed

```bash
# Get job run status
aws emr-serverless get-job-run \
  --application-id $EMR_SERVERLESS_APP_ID \
  --job-run-id <job_run_id>

# Get driver logs
aws emr-serverless get-dashboard-for-job-run \
  --application-id $EMR_SERVERLESS_APP_ID \
  --job-run-id <job_run_id>
```

Common causes:
- **ExitCode 2 / argument error:** `spark_skp_pipeline.py` on S3 is outdated. Re-upload: `aws s3 cp src/processing/spark_skp_pipeline.py s3://.../scripts/`
- **AccessDeniedException on TagResource:** ensure `emr-serverless:TagResource` is in the Lambda IAM policy
- **No output file:** check if the input had zero qualifying purchase rows (expected for test files with no purchase events)

### Output revenue is wrong

1. Verify `purchase_event_id: "1"` in `config.yaml` matches the Adobe Analytics event used for purchase
2. Verify `revenue_column_index: 3` matches the position of revenue in your `product_list` format
3. Check `keyword_normalize_case: true` — `Ipod` and `ipod` will be combined if enabled (default: true)
4. Run the integration test: `PYTHONPATH=src python -m pytest tests/integration/test_end_to_end.py -v`

---

## Appendix — Input File Format

Tab-delimited hit-level data file. Required columns:

| Column | Type | Description |
|---|---|---|
| `hit_time_gmt` | int | Unix timestamp of the hit |
| `date_time` | datetime | Human-readable timestamp |
| `user_agent` | string | Browser/client user agent string (**PII**) |
| `ip` | string | Visitor IP address (**PII** · session key) |
| `event_list` | string | Comma-delimited Adobe event IDs (e.g. `1,2,5`) |
| `geo_city` | string | City derived from IP geolocation |
| `geo_region` | string | Region derived from IP geolocation |
| `geo_country` | string | Country derived from IP geolocation |
| `pagename` | string | Adobe Analytics page name |
| `page_url` | string | Full URL of the page viewed |
| `product_list` | string | Semicolon-delimited product entries |
| `referrer` | string | Full URL of the referring page |

`product_list` format:
```
Category;ProductName;Quantity;Revenue;EventList;eVarList
Electronics;iPod;1;290.00;event1;evar1,Accessories;Cable;2;15.00
```

---

*Adobe Analytics — Search Keyword Performance Pipeline · v1.0*

# Clinical Decision Support Data Platform

Production-style data platform for clinical decision-support product analytics, operational reporting, third-party ingestion, governed transformations, and warehouse delivery.

## Architecture

```text
Application APIs / web / mobile / content / integrations / SaaS
                         |
                         v
                  Python ingestion
                         |
             immutable landing / RAW
                         |
                      Snowflake
            RAW -> STAGING -> CORE -> MARTS
                         |
             dbt transformations/tests
                         |
                  Airflow orchestration
                         |
     Product / Analytics / Operations / Finance
```

## Operating modes

### Generated demo mode

`DEMO_MODE=true` requires no external credentials. The repository can generate synthetic records and the static browser consoles run without a backend.

- `site/index.html` — source-to-serving merge and pipeline operations console.
- `site/customer_payment_stream.html` — customer/account checks, payment events, RAW/CLEAN/MERGE/CORE/CURATED state, and a compressed rolling 10-minute stream demonstration.

### Connected mode

`DEMO_MODE=false` uses the same pipeline structure against real systems. Copy `.env.example` to `.env`, replace the environment-specific values, and run `python scripts/preflight.py` before deployment.

The connection contract is centralized in:

- `.env.example` — credentials, Snowflake account/database/warehouse/role, API base URL and resource paths.
- `config/integrations.yml` — source names, default paths, keys, watermarks, contracts, landing objects, Snowflake schemas, Airflow connection ID, dbt layers, and merge policy.
- `src/clinical_data_platform/config.py` — typed settings and validation.
- `scripts/preflight.py` — deployment-readiness checks.

Real deployment requires more than secret values alone: the actual API base URL/resource paths and response contract, network access, Snowflake grants, and the organization's approved secret-management mechanism must match the target environment. All default endpoint names and paths in this repository are project-owned examples and are environment-overridable.

## Primary systems

- Python ingestion framework with typed contracts, retries, idempotency, audit metadata, and reconciliation.
- Snowflake warehouse with RAW, STAGING, CORE, MART, OPS, and GOVERNANCE schemas.
- dbt project with staging, intermediate, dimensions, facts, product marts, tests, snapshots, and macros.
- Airflow DAGs with task groups, backfill-safe windows, data-quality gates, and downstream publication.
- Data-quality and observability controls for freshness, volume, schema drift, uniqueness, reconciliation, and SLA health.
- Static operations consoles under `site/` showing generated pipeline runs, merge outcomes, model freshness, source-to-target checks, account checks, payments, and rolling stream behavior.

## Repository map

```text
src/clinical_data_platform/   Python ingestion and platform runtime
snowflake/                    DDL, MERGE, streams/tasks, security, performance
dbt/                          Transformations, tests, snapshots, marts
airflow/                      DAGs, operators, task groups, sensors
sources/                      Source contracts and extraction patterns
config/                       Environment-overridable integration registry
lake/raw/                     Immutable source-preserving payloads
lake/clean/                   Contract-valid normalized records
lake/merge/                   Deterministic current/history merge rules
lake/curated/                 Business-serving data products
contracts/                    Event and batch contracts
data_quality/                 Validation and anomaly controls
reconciliation/               Source-to-target controls
observability/                Freshness, volume, lineage, pipeline health
governance/                   Naming, ownership, classification, retention
tests/                        Unit, contract, integration, regression checks
site/                         Operational data-movement and stream consoles
scripts/                      Validation, preflight, and operational commands
```

## Core data products

- Clinical tool catalog and specialty taxonomy
- Product usage and session analytics
- Search, favorites, recent activity, and content engagement
- Tool interaction and completion funnels
- Integration launch and delivery activity
- Account and organization engagement
- Content lifecycle and publishing analytics
- Pipeline reliability, data-quality, and reconciliation marts

## Connection contract

The platform expects environment values for:

```text
SOURCE_API_BASE_URL
SOURCE_API_TOKEN
SOURCE_API_HEALTH_PATH
SOURCE_PRODUCT_EVENTS_PATH
SOURCE_CLINICAL_TOOLS_PATH
SOURCE_CONTENT_PATH
SOURCE_ACCOUNTS_PATH
SOURCE_PAYMENTS_PATH
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_DATABASE
SNOWFLAKE_SCHEMA
SNOWFLAKE_ROLE
AIRFLOW_CONN_SNOWFLAKE_CLINICAL_ANALYTICS
DBT_PROFILES_DIR
DBT_TARGET
RAW_URI
CLEAN_URI
QUARANTINE_URI
CHECKPOINT_URI
```

The HTTP framework already supports cursor pagination, retries, bounded `updated_from`/`updated_before` windows, rate-limit handling, request IDs, and contract-defined resource mapping. The Snowflake loader supports transaction boundaries, validated identifiers, temporary staging, parameterized writes, deterministic version-aware `MERGE`, and audit records.

All example records in this repository are de-identified/generated and the schemas, service names, paths, and infrastructure conventions are project-owned reference implementations.

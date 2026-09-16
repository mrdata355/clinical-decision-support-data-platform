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

## Primary systems

- Python ingestion framework with typed contracts, retries, idempotency, audit metadata, and reconciliation.
- Snowflake warehouse with RAW, STAGING, CORE, MART, OPS, and GOVERNANCE schemas.
- dbt project with staging, intermediate, dimensions, facts, product marts, tests, snapshots, and macros.
- Airflow DAGs with task groups, backfill-safe windows, data-quality gates, and downstream publication.
- Data-quality and observability controls for freshness, volume, schema drift, uniqueness, reconciliation, and SLA health.
- Static operations console under `site/` showing generated pipeline runs, merge outcomes, model freshness, and source-to-target checks.

## Repository map

```text
src/clinical_data_platform/   Python ingestion and platform runtime
snowflake/                    DDL, MERGE, streams/tasks, security, performance
 dbt/                         Transformations, tests, snapshots, marts
airflow/                      DAGs, operators, task groups, sensors
sources/                      Source contracts and extraction patterns
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
site/                         Operational data-movement console
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

All example records in this repository are de-identified/generated and the schemas, service names, paths, and infrastructure conventions are project-owned reference implementations.

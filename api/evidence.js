'use strict';

const { engine } = require('../server/platform-engine');
const { methodNotAllowed, requestId, sendJson } = require('../server/http');

const ASSETS = [
  { domain: 'runtime', path: 'src/clinical_data_platform/runtime.py', capability: 'typed ingestion runtime, deterministic extraction windows, validation and terminal-outcome accounting' },
  { domain: 'control-plane', path: 'src/clinical_data_platform/control_plane.py', capability: 'idempotency, circuit breaker, adaptive backpressure, state transitions and SLO burn-rate controls' },
  { domain: 'contracts', path: 'contracts/clinical_tool.v1.schema.json', capability: 'clinical-tool catalog, evidence provenance, calculator input/output and publication contract' },
  { domain: 'contracts', path: 'contracts/quality_rating.v1.schema.json', capability: 'reference quality-rating component and provenance contract' },
  { domain: 'governance', path: 'governance/catalog.yml', capability: 'asset ownership, classification, retention, freshness SLOs and lineage' },
  { domain: 'lakehouse', path: 'lake/clean/clean_contract.yml', capability: 'normalized clean-zone contracts and quarantine policy' },
  { domain: 'warehouse', path: 'snowflake/production_reference/enterprise_platform.sql', capability: 'expanded RAW/STAGING/CORE/MART/FEATURES/OPS/GOVERNANCE warehouse object model' },
  { domain: 'continuous-query', path: 'snowflake/streams_tasks/incremental_processing.sql', capability: 'Streams, Tasks, Dynamic Tables and continuous SLO surfaces' },
  { domain: 'warehouse-procedures', path: 'snowflake/procedures/merge_and_reconcile.sql', capability: 'transactional publishability, reconciliation, watermarks and quarantine replay' },
  { domain: 'performance', path: 'snowflake/performance/query_optimization.sql', capability: 'query cost, pruning, clustering, queue, cache, spill, dynamic-table and warehouse optimization diagnostics' },
  { domain: 'orchestration', path: 'airflow/dags/product_data_platform.py', capability: 'task groups, quality gates and backfill-safe orchestration' },
  { domain: 'dbt', path: 'dbt/models/schema.yml', capability: 'source freshness, model contracts, relationships and production tests' },
  { domain: 'dbt', path: 'dbt/models/marts/product/fct_search_funnel_daily.sql', capability: 'search-to-tool discovery and completion funnel' },
  { domain: 'dbt', path: 'dbt/models/marts/product/fct_integration_health_daily.sql', capability: 'de-identified EHR launch/autofill/writeback health mart' },
  { domain: 'dbt', path: 'dbt/models/marts/product/fct_recommendation_performance_daily.sql', capability: 'recommendation model attribution and version performance' },
  { domain: 'quality', path: 'data_quality/quality_engine.py', capability: 'model-aware quality evaluation, freshness, uniqueness, range and schema controls' },
  { domain: 'reconciliation', path: 'reconciliation/reconcile.py', capability: 'source-target counts, hashes, distributions, SCD and referential controls' },
  { domain: 'observability', path: 'observability/health.py', capability: 'batch, continuous-query and service health/SLO evaluation' },
  { domain: 'mlops', path: 'mlops/model_registry.yml', capability: 'training labels, features, evaluation gates, stages, drift and governance for top product/ops models' },
  { domain: 'mlops', path: 'mlops/platform.py', capability: 'dataset snapshots, leakage-safe time splits, registry, promotion gates and drift monitoring' },
  { domain: 'analytics-copilot', path: 'copilot/sql_rag.py', capability: 'governed retrieval plus read-only SQL planning with schema allowlists' },
  { domain: 'vercel-api', path: 'server/platform-engine.js', capability: 'live serverless telemetry and synthetic workload engine' },
  { domain: 'vercel-api', path: 'api/customer-action.js', capability: 'live account action -> event -> canonical state -> telemetry proof path' },
  { domain: 'vercel-api', path: 'api/sql-bot.js', capability: 'live governed report-SQL planning endpoint' },
  { domain: 'frontend', path: 'site/index.html', capability: 'production operations command center' },
  { domain: 'frontend', path: 'site/app.js', capability: 'live chart polling, customer actions, evidence viewer and SQL copilot UI' },
];

module.exports = function evidence(req, res) {
  const id = requestId(req);
  if (req.method !== 'GET') return methodNotAllowed(req, res, ['GET'], id);
  const snapshot = engine.snapshot();
  return sendJson(res, 200, {
    ok: true,
    requestId: id,
    generatedAt: new Date().toISOString(),
    repository: 'mrdata355/clinical-decision-support-data-platform',
    commit: snapshot.deployment.gitCommitSha,
    branch: snapshot.deployment.gitCommitRef,
    deployment: snapshot.deployment,
    proofBoundary: {
      live: [
        'Vercel function execution',
        'request IDs and server timing',
        'deployment region and git metadata when supplied by Vercel',
        'server-side control API responses',
        'customer action API responses',
        'governed SQL-copilot planning responses',
      ],
      synthetic: [
        'clinical/account event payloads displayed by the public dashboard',
        'model metrics until an approved production model registry is connected',
        'warehouse query results until approved Snowflake credentials are configured',
      ],
    },
    assets: ASSETS,
  }, id);
};

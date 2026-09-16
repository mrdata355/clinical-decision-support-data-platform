'use strict';

const { engine } = require('../server/platform-engine');
const { methodNotAllowed, requestId, sendJson } = require('../server/http');

const ASSETS = [
  { domain: 'runtime', path: 'src/clinical_data_platform/runtime.py', capability: 'typed ingestion runtime, contracts, deterministic windows, metrics' },
  { domain: 'control-plane', path: 'src/clinical_data_platform/control_plane.py', capability: 'state machine, idempotency, circuit breaker, adaptive backpressure, SLO burn rate' },
  { domain: 'warehouse', path: 'snowflake/production_reference/enterprise_platform.sql', capability: 'schemas, pipeline audit, watermarks, merge, streams/tasks, reconciliation' },
  { domain: 'orchestration', path: 'airflow/dags/product_data_platform.py', capability: 'task groups, quality gates, backfill-safe orchestration' },
  { domain: 'transformation', path: 'dbt/models/marts/product/fct_tool_engagement_daily.sql', capability: 'curated product mart' },
  { domain: 'quality', path: 'data_quality/quality_engine.py', capability: 'freshness, volume, uniqueness, schema controls' },
  { domain: 'reconciliation', path: 'reconciliation/reconcile.py', capability: 'source-to-target financial-style controls' },
  { domain: 'observability', path: 'observability/health.py', capability: 'platform health and SLO evaluation' },
  { domain: 'vercel-api', path: 'server/platform-engine.js', capability: 'live serverless production telemetry engine' },
  { domain: 'frontend', path: 'site/index.html', capability: 'production operations command center' },
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
      live: ['Vercel function execution', 'request IDs', 'server timing', 'deployment region', 'deployment git metadata', 'control API responses'],
      synthetic: ['clinical/account/payment event payloads shown in dashboard'],
    },
    assets: ASSETS,
  }, id);
};

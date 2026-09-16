'use strict';

const { engine } = require('../server/platform-engine');
const { methodNotAllowed, requestId, sendJson } = require('../server/http');

module.exports = function health(req, res) {
  const id = requestId(req);
  if (req.method !== 'GET') return methodNotAllowed(req, res, ['GET'], id);
  const snapshot = engine.snapshot();
  const healthy = snapshot.metrics.sloStatus === 'healthy' && snapshot.reconciliation.status === 'PASS';
  return sendJson(res, healthy ? 200 : 503, {
    ok: healthy,
    requestId: id,
    status: healthy ? 'healthy' : 'degraded',
    checkedAt: new Date().toISOString(),
    checks: {
      runtime: 'pass',
      reconciliation: snapshot.reconciliation.status.toLowerCase(),
      stream: snapshot.engine.running ? 'running' : 'paused',
      faultMode: snapshot.engine.faultMode,
      p95LagMs: snapshot.metrics.p95LagMs,
      freshnessMs: snapshot.metrics.freshnessMs,
    },
    deployment: snapshot.deployment,
  }, id);
};

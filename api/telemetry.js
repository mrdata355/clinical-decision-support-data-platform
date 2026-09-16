'use strict';

const { engine } = require('../server/platform-engine');
const { methodNotAllowed, requestId, sendJson } = require('../server/http');

module.exports = function telemetry(req, res) {
  const started = process.hrtime.bigint();
  const id = requestId(req);
  if (req.method !== 'GET') return methodNotAllowed(req, res, ['GET'], id);

  try {
    const snapshot = engine.snapshot();
    const computeMs = Number(process.hrtime.bigint() - started) / 1e6;
    return sendJson(res, 200, {
      ok: true,
      requestId: id,
      serverComputeMs: Number(computeMs.toFixed(3)),
      ...snapshot,
    }, id);
  } catch (error) {
    console.error(JSON.stringify({ level: 'error', msg: 'telemetry_failed', requestId: id, error: error.message }));
    return sendJson(res, 500, { ok: false, requestId: id, error: 'TELEMETRY_FAILED' }, id);
  }
};

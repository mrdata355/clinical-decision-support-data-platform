'use strict';

const { ACTIONS, engine } = require('../server/platform-engine');
const { bodyObject, methodNotAllowed, requestId, sendJson } = require('../server/http');

module.exports = function control(req, res) {
  const started = process.hrtime.bigint();
  const id = requestId(req);
  if (req.method !== 'POST') return methodNotAllowed(req, res, ['POST'], id);

  const body = bodyObject(req);
  const action = String(body.action || '').trim();
  if (!ACTIONS.has(action)) {
    return sendJson(res, 400, {
      ok: false,
      requestId: id,
      error: 'INVALID_ACTION',
      allowedActions: [...ACTIONS],
    }, id);
  }

  try {
    const snapshot = engine.applyAction(action, body.payload || {}, 'dashboard');
    const computeMs = Number(process.hrtime.bigint() - started) / 1e6;
    console.log(JSON.stringify({ level: 'info', msg: 'control_action', requestId: id, action, computeMs: Number(computeMs.toFixed(3)) }));
    return sendJson(res, 200, {
      ok: true,
      requestId: id,
      action,
      serverComputeMs: Number(computeMs.toFixed(3)),
      ...snapshot,
    }, id);
  } catch (error) {
    console.error(JSON.stringify({ level: 'error', msg: 'control_failed', requestId: id, action, error: error.message }));
    return sendJson(res, 500, { ok: false, requestId: id, error: 'CONTROL_FAILED' }, id);
  }
};

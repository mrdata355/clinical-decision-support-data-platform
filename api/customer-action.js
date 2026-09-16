'use strict';

const crypto = require('node:crypto');
const { engine } = require('../server/platform-engine');
const { bodyObject, methodNotAllowed, requestId, sendJson } = require('../server/http');

const ALLOWED = new Set(['customer_check', 'tool_interaction', 'payment_approved']);

module.exports = function customerAction(req, res) {
  const started = process.hrtime.bigint();
  const id = requestId(req);
  if (req.method !== 'POST') return methodNotAllowed(req, res, ['POST'], id);

  try {
    const body = bodyObject(req);
    const accountId = String(body.accountId || '').trim();
    const action = String(body.action || 'customer_check').trim();
    if (!engine.accounts.has(accountId)) {
      return sendJson(res, 404, { ok: false, requestId: id, error: 'ACCOUNT_NOT_FOUND' }, id);
    }
    if (!ALLOWED.has(action)) {
      return sendJson(res, 400, { ok: false, requestId: id, error: 'INVALID_CUSTOMER_ACTION', allowed: [...ALLOWED] }, id);
    }

    const account = engine.accounts.get(accountId);
    const now = Date.now();
    const event = {
      eventId: `evt_manual_${crypto.randomUUID().replace(/-/g, '').slice(0, 16)}`,
      eventType: action,
      businessKey: accountId,
      sourceVersion: account.sourceVersion + 1,
      eventAt: now - 25,
      arrivalAt: now,
      amount: action === 'payment_approved' ? Number(body.amount || 99.99) : null,
      replay: false,
    };
    const processed = engine._process(event);
    engine.lastAction = { name: action, at: new Date().toISOString(), actor: `customer:${accountId}` };
    const snapshot = engine.snapshot();
    const computeMs = Number(process.hrtime.bigint() - started) / 1e6;
    return sendJson(res, 200, {
      ok: true,
      requestId: id,
      accountId,
      action,
      processedEvent: {
        eventId: processed.eventId,
        outcome: processed.outcome,
        lagMs: processed.lagMs,
        traceId: processed.traceId,
      },
      serverComputeMs: Number(computeMs.toFixed(3)),
      ...snapshot,
    }, id);
  } catch (error) {
    return sendJson(res, 500, { ok: false, requestId: id, error: 'CUSTOMER_ACTION_FAILED' }, id);
  }
};

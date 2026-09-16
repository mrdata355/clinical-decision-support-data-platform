'use strict';

const crypto = require('node:crypto');

function requestId(req) {
  return req.headers['x-vercel-id'] || req.headers['x-request-id'] || crypto.randomUUID();
}

function setCommonHeaders(res, id) {
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'no-store, max-age=0');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
  res.setHeader('X-Request-Id', id);
}

function sendJson(res, status, payload, id) {
  setCommonHeaders(res, id);
  res.status(status).json(payload);
}

function methodNotAllowed(req, res, allowed, id) {
  res.setHeader('Allow', allowed.join(', '));
  sendJson(res, 405, {
    ok: false,
    error: 'METHOD_NOT_ALLOWED',
    allowed,
    method: req.method,
    requestId: id,
  }, id);
}

function bodyObject(req) {
  if (!req.body) return {};
  if (typeof req.body === 'object') return req.body;
  if (typeof req.body === 'string') {
    try {
      return JSON.parse(req.body);
    } catch {
      return {};
    }
  }
  return {};
}

module.exports = { bodyObject, methodNotAllowed, requestId, sendJson, setCommonHeaders };

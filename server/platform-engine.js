'use strict';

const crypto = require('node:crypto');

const MAX_EVENTS = 160;
const MAX_LAGS = 400;
const ACCOUNT_COUNT = 14;
const ACTIONS = new Set(['start', 'stop', 'burst', 'replay', 'reconcile', 'inject_fault', 'recover', 'reset']);
const EVENT_TYPES = [
  'tool_view',
  'tool_start',
  'tool_complete',
  'search',
  'content_view',
  'integration_launch',
  'payment_approved',
  'customer_check',
];

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function percentile(values, p) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = clamp(Math.ceil((p / 100) * sorted.length) - 1, 0, sorted.length - 1);
  return sorted[idx];
}

function stableInt(input) {
  const digest = crypto.createHash('sha256').update(String(input)).digest();
  return digest.readUInt32BE(0);
}

function iso(ms = Date.now()) {
  return new Date(ms).toISOString();
}

class PlatformEngine {
  constructor() {
    this.instanceId = crypto.randomUUID();
    this.bootedAt = Date.now();
    this.reset(false);
  }

  reset(reseedInstance = false) {
    if (reseedInstance) {
      this.instanceId = crypto.randomUUID();
      this.bootedAt = Date.now();
    }
    this.running = true;
    this.faultMode = false;
    this.sequence = 0;
    this.lastAdvanceAt = Date.now();
    this.lastCuratedAt = 0;
    this.lastAction = { name: 'boot', at: iso(), actor: 'system' };
    this.events = [];
    this.seenEventIds = new Set();
    this.lags = [];
    this.counts = {
      raw: 0,
      clean: 0,
      merge: 0,
      core: 0,
      curated: 0,
      inserted: 0,
      updated: 0,
      unchanged: 0,
      duplicate: 0,
      stale: 0,
      quarantined: 0,
      retries: 0,
      reconciliationRuns: 0,
    };
    this.accounts = new Map();
    for (let i = 0; i < ACCOUNT_COUNT; i += 1) {
      const id = `acct_${String(1001 + i)}`;
      this.accounts.set(id, {
        accountId: id,
        sourceVersion: 1,
        checks: i % 4,
        interactions: 2 + (i % 7),
        status: i % 11 === 0 ? 'review' : 'active',
        lastPayment: null,
        updatedAt: iso(Date.now() - i * 31000),
      });
    }
  }

  _nextEvent(duplicate = false) {
    if (duplicate && this.events.length) {
      const source = this.events[Math.min(this.events.length - 1, stableInt(this.sequence) % Math.min(20, this.events.length))];
      return {
        ...source,
        arrivalAt: Date.now(),
        eventAt: Date.now() - (25 + (stableInt(`${source.eventId}:replay`) % 700)),
        replay: true,
      };
    }

    this.sequence += 1;
    const seed = stableInt(`${this.instanceId}:${this.sequence}:${Math.floor(Date.now() / 1000)}`);
    const accountIndex = seed % ACCOUNT_COUNT;
    const accountId = `acct_${String(1001 + accountIndex)}`;
    const account = this.accounts.get(accountId);
    const eventType = EVENT_TYPES[seed % EVENT_TYPES.length];
    const versionLift = seed % 7 === 0 ? 1 : 0;
    const sourceVersion = Math.max(1, account.sourceVersion + versionLift - (seed % 29 === 0 ? 1 : 0));
    const eventId = `evt_${String(this.sequence).padStart(8, '0')}_${crypto.createHash('sha1').update(`${seed}`).digest('hex').slice(0, 8)}`;

    return {
      eventId,
      eventType,
      businessKey: accountId,
      sourceVersion,
      eventAt: Date.now() - (20 + (seed % 900)),
      arrivalAt: Date.now(),
      amount: eventType === 'payment_approved' ? Number((25 + (seed % 825) + 0.99).toFixed(2)) : null,
      replay: false,
    };
  }

  _process(event) {
    this.counts.raw += 1;
    const started = Date.now();
    let outcome = 'unchanged';
    let accepted = false;

    if (this.seenEventIds.has(event.eventId)) {
      outcome = 'duplicate';
      this.counts.duplicate += 1;
    } else {
      this.seenEventIds.add(event.eventId);
      this.counts.clean += 1;
      this.counts.merge += 1;
      const account = this.accounts.get(event.businessKey);

      if (!account || (this.faultMode && this.sequence % 5 === 0)) {
        outcome = 'quarantined';
        this.counts.quarantined += 1;
      } else if (event.sourceVersion < account.sourceVersion) {
        outcome = 'stale';
        this.counts.stale += 1;
      } else {
        accepted = true;
        this.counts.core += 1;
        this.counts.curated += 1;
        this.lastCuratedAt = Date.now();

        if (event.sourceVersion > account.sourceVersion) {
          account.sourceVersion = event.sourceVersion;
          outcome = 'updated';
          this.counts.updated += 1;
        } else if (account.interactions <= 2) {
          outcome = 'inserted';
          this.counts.inserted += 1;
        } else {
          outcome = 'unchanged';
          this.counts.unchanged += 1;
        }

        account.interactions += 1;
        if (event.eventType === 'customer_check') account.checks += 1;
        if (event.eventType === 'payment_approved') account.lastPayment = event.amount;
        account.status = this.faultMode && this.sequence % 8 === 0 ? 'degraded' : 'active';
        account.updatedAt = iso();
      }
    }

    const processingPenalty = this.faultMode ? 150 + (stableInt(event.eventId) % 480) : 5 + (stableInt(event.eventId) % 55);
    const lagMs = Math.max(1, Date.now() - event.eventAt + processingPenalty + (Date.now() - started));
    this.lags.push(lagMs);
    if (this.lags.length > MAX_LAGS) this.lags.shift();

    const processed = {
      ...event,
      outcome,
      accepted,
      lagMs,
      processedAt: Date.now(),
      traceId: crypto.createHash('sha256').update(`${event.eventId}:${event.arrivalAt}`).digest('hex').slice(0, 24),
    };
    this.events.unshift(processed);
    if (this.events.length > MAX_EVENTS) this.events.length = MAX_EVENTS;
    return processed;
  }

  _advance() {
    const now = Date.now();
    const elapsedMs = now - this.lastAdvanceAt;
    this.lastAdvanceAt = now;
    if (!this.running || elapsedMs <= 0) return;

    const baseRate = this.faultMode ? 1.4 : 4.2;
    const target = clamp(Math.floor((elapsedMs / 1000) * baseRate), 1, 26);
    for (let i = 0; i < target; i += 1) {
      this._process(this._nextEvent(false));
      if ((this.sequence + i) % 31 === 0) this._process(this._nextEvent(true));
    }
  }

  applyAction(action, payload = {}, actor = 'dashboard') {
    if (!ACTIONS.has(action)) {
      const error = new Error(`Unsupported action: ${action}`);
      error.code = 'UNSUPPORTED_ACTION';
      throw error;
    }

    this._advance();
    switch (action) {
      case 'start':
        this.running = true;
        break;
      case 'stop':
        this.running = false;
        break;
      case 'burst': {
        const requested = Number(payload.count ?? 32);
        const count = clamp(Number.isFinite(requested) ? Math.floor(requested) : 32, 1, 250);
        for (let i = 0; i < count; i += 1) this._process(this._nextEvent(false));
        break;
      }
      case 'replay':
        this._process(this._nextEvent(true));
        break;
      case 'reconcile':
        this.counts.reconciliationRuns += 1;
        break;
      case 'inject_fault':
        this.faultMode = true;
        this.counts.retries += 1;
        break;
      case 'recover':
        this.faultMode = false;
        break;
      case 'reset':
        this.reset(false);
        break;
      default:
        break;
    }

    this.lastAction = { name: action, at: iso(), actor };
    return this.snapshot();
  }

  _series() {
    const buckets = Array.from({ length: 30 }, (_, i) => ({
      second: 29 - i,
      received: 0,
      accepted: 0,
      lagTotal: 0,
      lagCount: 0,
    }));
    const now = Date.now();
    for (const event of this.events) {
      const age = Math.floor((now - event.processedAt) / 1000);
      if (age < 0 || age >= 30) continue;
      const bucket = buckets[29 - age];
      bucket.received += 1;
      if (event.accepted) bucket.accepted += 1;
      bucket.lagTotal += event.lagMs;
      bucket.lagCount += 1;
    }
    return buckets.map((bucket) => ({
      t: bucket.second,
      received: bucket.received,
      accepted: bucket.accepted,
      avgLagMs: bucket.lagCount ? Math.round(bucket.lagTotal / bucket.lagCount) : 0,
    }));
  }

  _reconciliation() {
    const accounted = this.counts.inserted + this.counts.updated + this.counts.unchanged + this.counts.duplicate + this.counts.stale + this.counts.quarantined;
    return {
      status: accounted === this.counts.raw ? 'PASS' : 'CHECK',
      sourceCount: this.counts.raw,
      explainedCount: accounted,
      difference: this.counts.raw - accounted,
      runCount: this.counts.reconciliationRuns,
    };
  }

  snapshot() {
    this._advance();
    const now = Date.now();
    const recent = this.events.filter((event) => now - event.processedAt <= 5000);
    const eps = Number((recent.length / 5).toFixed(2));
    const accepted = recent.filter((event) => event.accepted).length;
    const errorRatio = this.counts.raw ? (this.counts.quarantined + this.counts.stale) / this.counts.raw : 0;
    const p95LagMs = Math.round(percentile(this.lags, 95));
    const freshnessMs = this.lastCuratedAt ? now - this.lastCuratedAt : null;
    const reconciliation = this._reconciliation();
    const sloStatus = this.faultMode || p95LagMs > 1500 || errorRatio > 0.04 ? 'degraded' : 'healthy';

    return {
      schemaVersion: 2,
      generatedAt: iso(now),
      engine: {
        instanceId: this.instanceId,
        bootedAt: iso(this.bootedAt),
        stateScope: 'vercel-function-instance',
        running: this.running,
        faultMode: this.faultMode,
        lastAction: this.lastAction,
      },
      deployment: {
        environment: process.env.VERCEL_ENV || process.env.NODE_ENV || 'local',
        region: process.env.VERCEL_REGION || 'local',
        url: process.env.VERCEL_PROJECT_PRODUCTION_URL || process.env.VERCEL_URL || null,
        gitCommitSha: process.env.VERCEL_GIT_COMMIT_SHA || null,
        gitCommitRef: process.env.VERCEL_GIT_COMMIT_REF || null,
        gitRepoSlug: process.env.VERCEL_GIT_REPO_SLUG || 'clinical-decision-support-data-platform',
        runtime: `node-${process.versions.node}`,
      },
      metrics: {
        received: this.counts.raw,
        accepted,
        eps,
        p95LagMs,
        freshnessMs,
        errorRatio: Number(errorRatio.toFixed(5)),
        sloStatus,
        uptimeSeconds: Math.floor((now - this.bootedAt) / 1000),
      },
      stages: {
        raw: this.counts.raw,
        clean: this.counts.clean,
        merge: this.counts.merge,
        core: this.counts.core,
        curated: this.counts.curated,
      },
      outcomes: {
        inserted: this.counts.inserted,
        updated: this.counts.updated,
        unchanged: this.counts.unchanged,
        duplicate: this.counts.duplicate,
        stale: this.counts.stale,
        quarantined: this.counts.quarantined,
        retries: this.counts.retries,
      },
      reconciliation,
      series: this._series(),
      accounts: [...this.accounts.values()].sort((a, b) => b.updatedAt.localeCompare(a.updatedAt)).slice(0, 12),
      events: this.events.slice(0, 60).map((event) => ({
        eventId: event.eventId,
        eventType: event.eventType,
        businessKey: event.businessKey,
        sourceVersion: event.sourceVersion,
        outcome: event.outcome,
        lagMs: event.lagMs,
        traceId: event.traceId,
        processedAt: iso(event.processedAt),
      })),
      controls: [...ACTIONS],
      evidence: {
        liveRuntime: true,
        clinicalPayloadsSynthetic: true,
        message: 'Deployment/runtime metadata and API execution are live. Clinical event payloads are synthetic until approved source credentials are configured.',
      },
    };
  }
}

const globalKey = '__clinicalProductionEngineV2';
if (!globalThis[globalKey]) globalThis[globalKey] = new PlatformEngine();

module.exports = {
  ACTIONS,
  engine: globalThis[globalKey],
};

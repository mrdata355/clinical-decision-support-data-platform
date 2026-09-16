'use strict';

const { bodyObject, methodNotAllowed, requestId, sendJson } = require('../server/http');

const MAX_ROWS = 500;
const FORBIDDEN = /\b(insert|update|delete|merge|copy|put|get|remove|alter|drop|truncate|create|grant|revoke|call|use)\b/i;
const ALLOWED_SCHEMA = /\b(?:from|join)\s+(MART|MART_DBT|OPS)\./ig;

function deploymentMeta() {
  return {
    environment: process.env.VERCEL_ENV || process.env.NODE_ENV || 'local',
    region: process.env.VERCEL_REGION || 'local',
    url: process.env.VERCEL_PROJECT_PRODUCTION_URL || process.env.VERCEL_URL || null,
    gitCommitSha: process.env.VERCEL_GIT_COMMIT_SHA || null,
    gitCommitRef: process.env.VERCEL_GIT_COMMIT_REF || null,
  };
}

function normalizeQuestion(value) {
  const q = String(value || '').trim();
  if (q.length < 4) throw new Error('QUESTION_TOO_SHORT');
  if (q.length > 800) throw new Error('QUESTION_TOO_LONG');
  return q;
}

function plan(question) {
  const q = question.toLowerCase();
  if (/(ehr|autofill|writeback|integration)/.test(q)) {
    return {
      report: 'ehr_integration',
      rationale: 'Uses de-identified continuous EHR integration telemetry.',
      sql: `SELECT EVENT_DATE, INTEGRATION_ID, LAUNCHES, CONTEXT_OPENS, AUTOFILL_EVENTS, WRITEBACKS, CONFIRMATION_RATE\nFROM MART.DT_LIVE_EHR_INTEGRATION\nWHERE EVENT_DATE >= DATEADD('day', -7, CURRENT_DATE())\nORDER BY EVENT_DATE DESC, CONFIRMATION_RATE ASC`,
    };
  }
  if (/(search|discover|query)/.test(q)) {
    return {
      report: 'search_discovery',
      rationale: 'Uses the governed continuous search discovery aggregate.',
      sql: `SELECT EVENT_DATE, CHANNEL, COUNTRY_CODE, SEARCHES, SEARCH_RESULT_CLICKS, TOOL_VIEWS, TOOL_COMPLETIONS, DIV0(SEARCH_RESULT_CLICKS, SEARCHES) AS SEARCH_RESULT_CTR\nFROM MART.DT_LIVE_SEARCH_DISCOVERY\nWHERE EVENT_DATE >= DATEADD('day', -7, CURRENT_DATE())\nORDER BY EVENT_DATE DESC, SEARCHES DESC`,
    };
  }
  if (/(pipeline|fresh|stale|failure|quarantine|lag)/.test(q)) {
    return {
      report: 'pipeline_health',
      rationale: 'Uses operational SLO state instead of raw pipeline payloads.',
      sql: `SELECT PIPELINE_NAME, LAST_RUN_ID, LAST_STATUS, LAST_SOURCE_COUNT, FRESHNESS_SECONDS, QUARANTINE_RATIO, FAILURES_24H\nFROM OPS.DT_CONTINUOUS_PIPELINE_SLO\nORDER BY FAILURES_24H DESC, FRESHNESS_SECONDS DESC`,
    };
  }
  if (/(recommend|model|ranking|ctr)/.test(q)) {
    return {
      report: 'recommendation_performance',
      rationale: 'Compares recommendation model versions using attributed product outcomes.',
      sql: `SELECT EVENT_DATE, MODEL_NAME, MODEL_VERSION, IMPRESSIONS, CLICKS, ATTRIBUTED_TOOL_STARTS, ATTRIBUTED_TOOL_COMPLETIONS, CTR, CLICK_TO_COMPLETION_RATE\nFROM MART_DBT.FCT_RECOMMENDATION_PERFORMANCE_DAILY\nWHERE EVENT_DATE >= DATEADD('day', -14, CURRENT_DATE())\nORDER BY EVENT_DATE DESC, CTR DESC`,
    };
  }
  if (/(quality|rating|trust|evidence)/.test(q)) {
    return {
      report: 'quality_trust',
      rationale: 'Uses the quality/trust mart; scores are reference implementation metadata, not clinical advice.',
      sql: `SELECT EVENT_DATE, TOOL_ID, PRIMARY_SPECIALTY, OVERALL_SCORE, SCIENTIFIC_SOUNDNESS_SCORE, IMPORTANCE_SCORE, USABILITY_FEASIBILITY_SCORE, FAIRNESS_EQUITY_STATUS, QUALITY_RATING_VIEWS, TOOL_COMPLETIONS\nFROM MART_DBT.FCT_QUALITY_TRUST_DAILY\nWHERE EVENT_DATE >= DATEADD('day', -14, CURRENT_DATE())\nORDER BY EVENT_DATE DESC, TOOL_COMPLETIONS DESC`,
    };
  }
  return {
    report: 'tool_funnel',
    rationale: 'Defaults to the governed continuous tool funnel.',
    sql: `SELECT EVENT_DATE, TOOL_ID, TOOL_NAME, PRIMARY_SPECIALTY, CHANNEL, COUNTRY_CODE, TOOL_VIEWS, TOOL_STARTS, TOOL_COMPLETIONS, VIEW_TO_START_RATE, START_TO_COMPLETE_RATE\nFROM MART.DT_LIVE_TOOL_FUNNEL\nWHERE EVENT_DATE >= DATEADD('day', -7, CURRENT_DATE())\nORDER BY EVENT_DATE DESC, TOOL_COMPLETIONS DESC`,
  };
}

function guardSql(sql) {
  let statement = String(sql || '').trim().replace(/;+\s*$/, '');
  if (!/^(select|with)\b/i.test(statement)) throw new Error('READ_ONLY_SQL_REQUIRED');
  if (FORBIDDEN.test(statement)) throw new Error('FORBIDDEN_SQL_OPERATION');
  if (/\bselect\s+\*/i.test(statement)) throw new Error('SELECT_STAR_DENIED');
  if (![...statement.matchAll(ALLOWED_SCHEMA)].length) throw new Error('GOVERNED_SCHEMA_REQUIRED');
  if (/\b(?:from|join)\s+(RAW|STAGING)\./i.test(statement)) throw new Error('RAW_STAGING_ACCESS_DENIED');
  const limit = statement.match(/\blimit\s+(\d+)\b/i);
  if (limit) statement = statement.replace(/\blimit\s+\d+\b/i, `LIMIT ${Math.min(MAX_ROWS, Number(limit[1]))}`);
  else statement += `\nLIMIT ${MAX_ROWS}`;
  return statement;
}

module.exports = function sqlBot(req, res) {
  const started = process.hrtime.bigint();
  const id = requestId(req);
  if (req.method !== 'POST') return methodNotAllowed(req, res, ['POST'], id);

  try {
    const question = normalizeQuestion(bodyObject(req).question);
    const candidate = plan(question);
    const sql = guardSql(candidate.sql);
    const computeMs = Number(process.hrtime.bigint() - started) / 1e6;
    return sendJson(res, 200, {
      ok: true,
      requestId: id,
      generatedAt: new Date().toISOString(),
      deployment: deploymentMeta(),
      question,
      report: candidate.report,
      rationale: candidate.rationale,
      sql,
      evidence: ['copilot/report_catalog.yml', 'copilot/sql_rag.py', 'dbt/models/schema.yml'],
      execution: {
        mode: 'plan_only',
        readOnly: true,
        maxRows: MAX_ROWS,
        allowedSchemas: ['MART', 'MART_DBT', 'OPS'],
        note: 'Execution is disabled in the public portfolio deployment until approved warehouse credentials are configured.',
      },
      serverComputeMs: Number(computeMs.toFixed(3)),
    }, id);
  } catch (error) {
    return sendJson(res, 400, { ok: false, requestId: id, error: error.message || 'SQL_BOT_ERROR' }, id);
  }
};

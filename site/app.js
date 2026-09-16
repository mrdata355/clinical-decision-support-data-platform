(() => {
  'use strict';

  const $ = (id) => document.getElementById(id);
  const $$ = (selector) => [...document.querySelectorAll(selector)];
  const repoBase = 'https://github.com/mrdata355/clinical-decision-support-data-platform/blob';
  const state = { telemetry: null, evidence: null, polling: null, online: false, activeTab: 'overview', toastTimer: null };

  const formatNumber = (value) => Number(value || 0).toLocaleString();
  const short = (value, size = 10) => value ? String(value).slice(0, size) : '—';
  const timeAgo = (ms) => {
    if (ms == null) return '—';
    if (ms < 1000) return `${Math.round(ms)} ms`;
    return `${(ms / 1000).toFixed(1)} s`;
  };

  function toast(message, isError = false) {
    const el = $('toast');
    el.textContent = message;
    el.style.borderColor = isError ? 'rgba(255,107,122,.5)' : 'rgba(87,230,255,.35)';
    el.classList.add('show');
    clearTimeout(state.toastTimer);
    state.toastTimer = setTimeout(() => el.classList.remove('show'), 2600);
  }

  async function api(path, options = {}) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 8000);
    try {
      const response = await fetch(path, {
        cache: 'no-store',
        headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
        ...options,
        signal: controller.signal,
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok && path !== '/api/health') throw new Error(body.error || `HTTP_${response.status}`);
      return { response, body };
    } finally {
      clearTimeout(timeout);
    }
  }

  function setConnection(online, label) {
    state.online = online;
    const el = $('connectionState');
    el.classList.toggle('online', online);
    el.classList.toggle('offline', !online);
    el.innerHTML = `<span></span> ${label}`;
  }

  function setTab(name) {
    state.activeTab = name;
    $$('.rail-btn[data-tab]').forEach((button) => button.classList.toggle('active', button.dataset.tab === name));
    $$('.tab-panel').forEach((panel) => panel.classList.toggle('active', panel.dataset.panel === name));
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  function statusPill(outcome) {
    const safe = String(outcome || 'unchanged').toLowerCase().replace(/[^a-z_]/g, '');
    return `<span class="status-pill status-${safe}">${safe.toUpperCase()}</span>`;
  }

  function renderChart(series) {
    const canvas = $('flowChart');
    const ctx = canvas.getContext('2d');
    const ratio = window.devicePixelRatio || 1;
    const cssWidth = canvas.clientWidth || 900;
    const cssHeight = canvas.clientHeight || 160;
    if (canvas.width !== Math.floor(cssWidth * ratio) || canvas.height !== Math.floor(cssHeight * ratio)) {
      canvas.width = Math.floor(cssWidth * ratio);
      canvas.height = Math.floor(cssHeight * ratio);
    }
    ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
    ctx.clearRect(0, 0, cssWidth, cssHeight);
    const pad = 12;
    const usableW = cssWidth - pad * 2;
    const usableH = cssHeight - pad * 2;
    const values = series || [];
    const max = Math.max(4, ...values.flatMap((d) => [d.received || 0, d.accepted || 0]));

    ctx.strokeStyle = 'rgba(78,104,132,.18)';
    ctx.lineWidth = 1;
    for (let i = 0; i <= 4; i += 1) {
      const y = pad + (usableH / 4) * i;
      ctx.beginPath(); ctx.moveTo(pad, y); ctx.lineTo(cssWidth - pad, y); ctx.stroke();
    }

    const draw = (key, color) => {
      ctx.strokeStyle = color;
      ctx.lineWidth = 2;
      ctx.shadowBlur = 10;
      ctx.shadowColor = color;
      ctx.beginPath();
      values.forEach((point, i) => {
        const x = pad + (values.length <= 1 ? 0 : (i / (values.length - 1)) * usableW);
        const y = pad + usableH - ((point[key] || 0) / max) * usableH;
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      });
      ctx.stroke();
      ctx.shadowBlur = 0;
    };
    draw('received', '#57e6ff');
    draw('accepted', '#9b7bff');
  }

  function renderTelemetry(data) {
    state.telemetry = data;
    setConnection(true, data.engine?.running ? 'LIVE API ONLINE' : 'API ONLINE / PAUSED');
    $('deployEnv').textContent = String(data.deployment?.environment || '—').toUpperCase();
    $('deployRegion').textContent = String(data.deployment?.region || '—').toUpperCase();
    $('deployCommit').textContent = short(data.deployment?.gitCommitSha, 12);
    $('requestId').textContent = short(data.requestId, 18);
    $('functionInstance').textContent = short(data.engine?.instanceId, 14);
    $('computeMs').textContent = `${Number(data.serverComputeMs || 0).toFixed(2)} ms`;

    $('metricReceived').textContent = formatNumber(data.metrics?.received);
    $('metricEps').textContent = Number(data.metrics?.eps || 0).toFixed(2);
    $('metricLag').textContent = `${formatNumber(data.metrics?.p95LagMs)} ms`;
    $('metricFresh').textContent = timeAgo(data.metrics?.freshnessMs);
    $('metricError').textContent = `${(Number(data.metrics?.errorRatio || 0) * 100).toFixed(2)}%`;
    $('metricSlo').textContent = String(data.metrics?.sloStatus || '—').toUpperCase();
    $('metricSlo').style.color = data.metrics?.sloStatus === 'healthy' ? 'var(--green)' : 'var(--red)';

    const stages = data.stages || {};
    const raw = Number(stages.raw || 0);
    ['Raw', 'Clean', 'Merge', 'Core', 'Curated'].forEach((name) => {
      const key = name.toLowerCase();
      $(`stage${name}`).textContent = formatNumber(stages[key]);
      $(`bar${name}`).style.width = `${raw ? Math.min(100, (Number(stages[key] || 0) / raw) * 100) : 0}%`;
    });

    const recon = data.reconciliation || {};
    $('reconBadge').textContent = `RECONCILE ${recon.status || '—'} · Δ ${recon.difference ?? '—'}`;
    $('reconBadge').style.color = recon.status === 'PASS' ? 'var(--green)' : 'var(--red)';
    $('reconSource').textContent = formatNumber(recon.sourceCount);
    $('reconExplained').textContent = formatNumber(recon.explainedCount);
    $('reconDiff').textContent = formatNumber(recon.difference);
    $('reconRuns').textContent = formatNumber(recon.runCount);
    $('lastActionTag').textContent = `LAST ACTION ${(data.engine?.lastAction?.name || '—').toUpperCase()} · ${data.engine?.lastAction?.at ? new Date(data.engine.lastAction.at).toLocaleTimeString() : '—'}`;

    renderChart(data.series || []);
    renderEvents(data.events || []);
    renderAccounts(data.accounts || []);
    renderOutcomes(data.outcomes || {});
    $('lastSync').textContent = `SYNC ${new Date(data.generatedAt).toLocaleTimeString()} · ${short(data.requestId, 12)}`;
    $('footerTime').textContent = new Date().toLocaleString();
  }

  function renderEvents(events) {
    $('eventCountTag').textContent = `${events.length} ROWS`;
    $('eventRows').innerHTML = events.slice(0, 22).map((event) => `
      <tr>
        <td title="${event.eventId}">${short(event.eventId, 15)}</td>
        <td>${event.eventType}</td>
        <td>${event.businessKey}</td>
        <td>${event.sourceVersion}</td>
        <td>${statusPill(event.outcome)}</td>
        <td>${event.lagMs} ms</td>
        <td title="Click to copy trace"><button class="text-btn trace-copy" data-trace="${event.traceId}">${short(event.traceId, 9)}</button></td>
      </tr>`).join('');
    $$('.trace-copy').forEach((button) => button.addEventListener('click', async () => {
      await navigator.clipboard?.writeText(button.dataset.trace || '');
      toast(`Trace copied: ${button.dataset.trace}`);
    }));
  }

  function renderAccounts(accounts) {
    $('accountRows').innerHTML = accounts.map((account) => `
      <tr>
        <td><button class="text-btn customer-action" data-account="${account.accountId}" title="Run a live customer check">${account.accountId}</button></td>
        <td>${account.checks}</td><td>${account.interactions}</td>
        <td>${statusPill(account.status === 'active' ? 'updated' : account.status)}</td><td>${account.sourceVersion}</td>
      </tr>`).join('');
    $$('.customer-action').forEach((button) => button.addEventListener('click', () => runCustomerAction(button.dataset.account)));
  }

  function renderOutcomes(outcomes) {
    const order = ['inserted', 'updated', 'unchanged', 'duplicate', 'stale', 'quarantined', 'retries'];
    $('outcomeGrid').innerHTML = order.map((key) => `<div class="outcome-card"><span>${key}</span><strong>${formatNumber(outcomes[key])}</strong></div>`).join('');
  }

  async function runCustomerAction(accountId) {
    try {
      toast(`Running customer check for ${accountId}…`);
      const { body } = await api('/api/customer-action', {
        method: 'POST',
        body: JSON.stringify({ accountId, action: 'customer_check' }),
      });
      renderTelemetry(body);
      toast(`${accountId} updated · ${body.processedEvent?.outcome || 'processed'} · trace ${short(body.processedEvent?.traceId, 10)}`);
    } catch (error) {
      toast(`Customer action failed: ${error.message}`, true);
    }
  }

  async function poll() {
    try {
      const { body } = await api(`/api/telemetry?t=${Date.now()}`);
      renderTelemetry(body);
    } catch (error) {
      setConnection(false, 'LIVE API OFFLINE');
      $('healthLabel').textContent = 'Runtime unavailable';
      $('healthBtn').className = 'health-chip degraded';
      console.error(error);
    }
  }

  async function control(action, payload = {}) {
    try {
      toast(`Executing ${action}…`);
      const { body } = await api('/api/control', { method: 'POST', body: JSON.stringify({ action, payload }) });
      renderTelemetry(body);
      toast(`${action.toUpperCase()} acknowledged · ${short(body.requestId, 12)}`);
      if (action === 'inject_fault' || action === 'recover') await checkHealth();
    } catch (error) {
      toast(`${action.toUpperCase()} failed: ${error.message}`, true);
    }
  }

  async function checkHealth() {
    try {
      const { response, body } = await api('/api/health');
      const healthy = response.ok && body.status === 'healthy';
      $('healthBtn').className = `health-chip ${healthy ? 'healthy' : 'degraded'}`;
      $('healthLabel').textContent = healthy ? 'Runtime healthy' : 'Runtime degraded';
      toast(`Health: ${String(body.status || 'unknown').toUpperCase()} · ${short(body.requestId, 12)}`);
    } catch (error) {
      $('healthBtn').className = 'health-chip degraded';
      $('healthLabel').textContent = 'Health check failed';
    }
  }

  async function loadEvidence() {
    try {
      const { body } = await api('/api/evidence');
      state.evidence = body;
      const live = body.proofBoundary?.live || [];
      const synthetic = body.proofBoundary?.synthetic || [];
      $('evidenceSummary').innerHTML = `<b>Deployment commit:</b> ${body.commit ? short(body.commit, 16) : 'local / not supplied'} &nbsp;·&nbsp; <b>Live proof:</b> ${live.join(', ')} &nbsp;·&nbsp; <b>Synthetic boundary:</b> ${synthetic.join(', ')}.`;
      const ref = body.commit || body.branch || 'main';
      $('assetGrid').innerHTML = (body.assets || []).map((asset) => `
        <a class="asset-card" href="${repoBase}/${encodeURIComponent(ref)}/${asset.path}" target="_blank" rel="noopener">
          <span class="asset-domain">${asset.domain}</span><span class="asset-path">${asset.path}</span><span class="asset-cap">${asset.capability}</span>
        </a>`).join('');
    } catch (error) {
      $('evidenceSummary').textContent = `Evidence API unavailable: ${error.message}`;
    }
  }

  function injectSqlCopilot() {
    const controlsPanel = document.querySelector('[data-panel="controls"]');
    if (!controlsPanel || $('sqlCopilotCard')) return;
    const article = document.createElement('article');
    article.id = 'sqlCopilotCard';
    article.className = 'panel full-panel';
    article.style.marginTop = '14px';
    article.innerHTML = `
      <div class="panel-head"><div><span class="kicker">GOVERNED RAG / SQL COPILOT</span><h2>Generate a report query</h2></div><span class="mono-tag">SELECT-ONLY</span></div>
      <div class="engineering-note"><b>Policy:</b> MART / MART_DBT / OPS only, no RAW/STAGING access, explicit columns, max 500 rows. The public deployment plans SQL but does not execute against a warehouse without approved credentials.</div>
      <div style="display:grid;grid-template-columns:1fr auto;gap:10px;margin-top:14px">
        <input id="sqlQuestion" class="btn" style="text-align:left;width:100%;cursor:text" value="Show tool completions by specialty for the last 7 days" aria-label="Analytics question" />
        <button id="sqlAskBtn" class="primary-btn" type="button">Generate SQL</button>
      </div>
      <pre id="sqlAnswer" style="white-space:pre-wrap;overflow:auto;margin:14px 0 0;padding:14px;border:1px solid var(--line);border-radius:12px;background:#06111e;color:#b9cee2;min-height:120px">Ask for tool funnel, search discovery, EHR integration, pipeline health, quality, or recommendation-model reporting.</pre>`;
    controlsPanel.appendChild(article);
    $('sqlAskBtn').addEventListener('click', async () => {
      const question = $('sqlQuestion').value.trim();
      if (!question) return;
      $('sqlAnswer').textContent = 'Planning governed SQL…';
      try {
        const { body } = await api('/api/sql-bot', { method: 'POST', body: JSON.stringify({ question }) });
        $('sqlAnswer').textContent = `${body.rationale}\n\n${body.sql}\n\nEvidence: ${(body.evidence || []).join(', ')}\nRequest: ${body.requestId}`;
        toast(`SQL plan generated · ${body.report}`);
      } catch (error) {
        $('sqlAnswer').textContent = `SQL copilot error: ${error.message}`;
        toast(`SQL copilot failed: ${error.message}`, true);
      }
    });
  }

  function bind() {
    $$('.rail-btn[data-tab]').forEach((button) => button.addEventListener('click', () => setTab(button.dataset.tab)));
    $$('[data-tab-jump]').forEach((button) => button.addEventListener('click', () => setTab(button.dataset.tabJump)));
    $$('[data-action]').forEach((button) => button.addEventListener('click', () => {
      control(button.dataset.action);
      const dialog = $('commandDialog');
      if (dialog.open) dialog.close();
    }));
    $('healthBtn').addEventListener('click', checkHealth);
    $('commandBtn').addEventListener('click', () => $('commandDialog').showModal());
    $('closeCommand').addEventListener('click', () => $('commandDialog').close());
    window.addEventListener('resize', () => state.telemetry && renderChart(state.telemetry.series || []));
    window.addEventListener('keydown', (event) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault(); $('commandDialog').showModal(); return;
      }
      if ($('commandDialog').open) {
        const map = { s: 'start', b: 'burst', d: 'replay', r: 'reconcile', f: 'inject_fault', h: 'recover' };
        const action = map[event.key.toLowerCase()];
        if (action) { event.preventDefault(); control(action); $('commandDialog').close(); }
      }
    });
  }

  async function init() {
    injectSqlCopilot();
    bind();
    await Promise.allSettled([poll(), loadEvidence(), checkHealth()]);
    state.polling = setInterval(poll, 1300);
  }

  init();
})();

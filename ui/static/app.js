'use strict';

// Kamin DB Syncer UI. Screens: sync list (home) · new-sync wizard · sync page (overview / runs / schema / settings)
// · run log · schema history · environment.

const $app = document.getElementById('app');
let timer = null;
let status = null;
let leaveGuard = null; // returns a message when the current screen has unsaved changes

// ---------- helpers ----------

const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const fmt = (iso) => (iso ? new Date(iso).toLocaleString() : '—');
const plural = (n, one, other) => (Number(n) === 1 ? one : other);

function rel(iso) {
  if (!iso) return '';
  const d = new Date(iso), s = Math.round((d - Date.now()) / 1000), a = Math.abs(s);
  const say = (n, w) => (s < 0 ? `${n} ${w} ago` : `in ${n} ${w}`);
  if (a < 45) return s < 0 ? 'just now' : 'now';
  if (a < 3600) return say(Math.round(a / 60), 'min');
  if (a < 86400) return say(Math.round(a / 3600), 'h');
  return d.toLocaleString(undefined, { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' });
}

function bytes(b) {
  if (!b) return '0 B';
  if (b >= 1 << 30) return `${(b / (1 << 30)).toFixed(1)} GB`;
  if (b >= 1 << 20) return `${(b / (1 << 20)).toFixed(1)} MB`;
  if (b >= 1 << 10) return `${Math.round(b / (1 << 10))} KB`;
  return `${b} B`;
}
const traffic = (st) => (st ? bytes((st.bytes_read || 0) + (st.bytes_written || 0)) : '—');
const num = (n) => Number(n || 0).toLocaleString();
const mb = (m) => (m >= 1024 ? `${(m / 1024).toFixed(1)} GB` : m >= 1 ? `${Math.round(m)} MB` : m > 0 ? '<1 MB' : '0');
const sizesCache = {};
async function jobSizes(id) {
  const c = sizesCache[id];
  if (c && Date.now() - c.at < 60000) return c.v;
  const v = await api(`/api/jobs/${id}/sizes`);
  sizesCache[id] = { at: Date.now(), v };
  return v;
}
const dbLine = (d) => (!d ? '' : d.error ? `<span class="err-text small">${esc(d.error)}</span>`
  : `${mb(d.data_mb)} data · ${num(d.tables)} ${plural(d.tables, 'table', 'tables')} · ${num(d.rows)} ${plural(d.rows, 'row', 'rows')}${d.files_mb ? ` · files ${mb(d.files_mb)}` : ''}`);
const limitText = (m) => (m > 0 ? `up to ${m} Mbit/s each way` : 'no limit');

function duration(a, b) {
  if (!a) return '—';
  let sec = Math.max(0, Math.round(((b ? new Date(b) : new Date()) - new Date(a)) / 1000));
  const h = Math.floor(sec / 3600), m = Math.floor((sec % 3600) / 60);
  sec %= 60;
  return h ? `${h} h ${m} min` : m ? `${m} min ${sec} s` : `${sec} s`;
}

async function api(path, opts = {}) {
  // absolute URL: relative fetch fails when the page was opened as http://user:pass@host/
  const res = await fetch(location.origin + path, {
    ...opts,
    headers: opts.body ? { 'Content-Type': 'application/json' } : {},
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  if (res.status === 204) return null;
  const data = await res.json().catch(() => null);
  if (!res.ok) {
    const d = data?.detail;
    const err = new Error(Array.isArray(d) ? d.join('; ') : d || res.statusText);
    err.details = Array.isArray(d) ? d : [err.message];
    throw err;
  }
  return data;
}

function toast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.hidden = false;
  clearTimeout(toast.h);
  toast.h = setTimeout(() => (t.hidden = true), 3500);
}

function poll(fn, ms) {
  clearInterval(timer);
  timer = setInterval(() => fn().catch(() => {}), ms);
}

async function loadStatus() {
  status = await api('/api/status');
  return status;
}

// ---------- schedules in plain words ----------

const PRESETS = [
  { cron: '*/10 * * * *', title: 'Every 10 minutes', note: 'recommended for production databases' },
  { cron: '*/30 * * * *', title: 'Every 30 minutes' },
  { cron: '0 * * * *', title: 'Every hour' },
  { cron: '0 3 * * *', title: 'Every night at 3:00' },
  { cron: '-', title: 'Manual only', note: 'run it from the sync page' },
];
function scheduleText(job) {
  const eff = job.effective_schedule;
  if (!eff) return 'manual only';
  const p = PRESETS.find((x) => x.cron === eff);
  return p ? p.title.toLowerCase() : `cron ${eff}`;
}

// ---------- job state in one sentence ----------

const needsTarget = (c) => !!(c.schema?.publish || c.data?.tables?.length);

function jobState(j) {
  const blocked = needsTarget(j.config) && !j.target_approved;
  if (j.active && j.last_status === 'running') return { dot: 'running', text: `Running · ${duration(j.last_started_at)}`, blocked };
  if (j.active) return { dot: 'queued', text: 'Queued', blocked };
  if (blocked) return { dot: 'waiting', text: 'Waiting for target approval', blocked };
  if (!j.enabled) return { dot: '', text: 'Disabled', blocked };
  switch (j.last_status) {
    case 'succeeded': return { dot: 'ok', text: `Synced ${rel(j.last_finished_at || j.last_started_at)}`, blocked };
    case 'failed': return { dot: 'failed', text: `Failed ${rel(j.last_started_at)}`, blocked };
    case 'cancelled': return { dot: 'waiting', text: `Stopped ${rel(j.last_started_at)}`, blocked };
    default: return { dot: '', text: 'Never run', blocked };
  }
}

function nextText(j) {
  if (!j.enabled) return 'disabled';
  if (!j.effective_schedule) return 'manual only';
  return j.next_run_at ? `next ${rel(j.next_run_at)} · ${scheduleText(j)}` : scheduleText(j);
}

async function runNow(id) {
  try {
    const r = await api(`/api/jobs/${id}/run`, { method: 'POST' });
    location.hash = `#/runs/${r.run_id}`;
  } catch (e) { toast(e.message); }
}

// ---------- home: list of syncs ----------

async function viewHome() {
  const render = async () => {
    const jobs = await api('/api/jobs');
    const states = jobs.map((j) => ({ j, s: jobState(j) }));
    const attention = states.filter(({ j, s }) => s.blocked || (j.last_status === 'failed' && !j.active));
    const sub = !jobs.length ? '' : attention.length
      ? `${attention.length} ${plural(attention.length, 'needs', 'need')} attention`
      : `${jobs.length} ${plural(jobs.length, 'sync', 'syncs')} · all good`;
    $app.innerHTML = `
      <div class="page-head"><div><h1>Syncs</h1>${sub ? `<div class="sub">${sub}</div>` : ''}</div>
        <div class="actions"><a class="btn primary" href="#/new">New sync</a></div></div>
      ${attention.map(({ j, s }) => `
        <div class="callout ${s.blocked ? '' : 'err'}"><div class="body">
          <b>${esc(j.name)}: ${s.blocked ? 'target needs approval' : 'last run failed'}</b>
          <div class="small">${s.blocked ? 'The sync will not run until the target is approved.' : esc((j.last_summary || '').slice(0, 220))}</div>
          <div class="acts"><a class="btn sm" href="#/jobs/${j.id}">${s.blocked ? 'Approve target' : 'Open'}</a></div>
        </div></div>`).join('')}
      ${jobs.length ? `<div class="group">${states.map(({ j, s }) => `
        <a class="job-row" href="#/jobs/${j.id}">
          <span class="dot ${s.dot}"></span>
          <div class="main"><div class="name">${esc(j.name)}</div>
            <div class="route">${esc(j.config.source.database)} → ${esc(needsTarget(j.config) ? j.config.target.database : 'git only')}
              <span class="muted">· ${esc(j.config.source.server)}${j.config.target?.server && j.config.target.server !== j.config.source.server ? ` → ${esc(j.config.target.server)}` : ''}</span></div></div>
          <div class="state">${esc(s.text)}<small>${esc(nextText(j))}</small></div>
          <span class="chev"></span>
        </a>`).join('')}</div>`
        : `<div class="group"><div class="empty"><h3>No syncs yet</h3>
            <p>A sync copies the schema and the data of selected tables from one SQL Server database to another on a schedule
            and keeps the schema history in git.</p><a class="btn primary" href="#/new">Set up your first sync</a></div></div>`}`;
  };
  await render();
  poll(render, 5000);
}

// ---------- shared form pieces ----------

const sw = (name, on, extra = '') => `<label class="switch"><input type="checkbox" name="${name}" ${on ? 'checked' : ''} ${extra}><span></span></label>`;

function credOptions(value) {
  const list = status?.credential_list || [];
  const known = list.some((c) => c.name === value);
  return (value && !known ? `<option value="${esc(value)}" selected>${esc(value)} (not found)</option>` : '')
    + list.map((c) => `<option value="${esc(c.name)}" ${c.name === value ? 'selected' : ''}>${esc(c.name)}${c.user && c.user !== c.name ? ` — ${esc(c.user)}` : ''}${c.source === 'env' ? ' · from Secret' : ''}</option>`).join('')
    + `<option value="__new" ${!list.length && !value ? 'selected' : ''}>+ New credentials…</option>`;
}

// login picker: existing logins or a new one (login + password, saved encrypted on the server)
function credField(name, value) {
  const isNew = !(status?.credential_list || []).length && !value;
  return `<select name="${name}" data-cred>${credOptions(value)}</select>
    <div class="newcred" data-newcred="${name}" ${isNew ? '' : 'hidden'}>
      <input type="text" name="${name}-user" placeholder="SQL Server login" autocomplete="off">
      <input type="password" name="${name}-pass" placeholder="Password" autocomplete="new-password">
      <span class="hint">Stored encrypted on the server and reusable by other syncs. Uses SQL Server authentication (login and password).</span>
    </div>`;
}

function wireCreds(root) {
  root.querySelectorAll('select[data-cred]').forEach((sel) => {
    const box = root.querySelector(`[data-newcred="${sel.name}"]`);
    sel.addEventListener('change', () => { box.hidden = sel.value !== '__new'; if (!box.hidden) box.querySelector('input').focus(); });
  });
}

// ensureCred saves a new login if one is being entered and returns the login's name to use.
async function ensureCred(root, name, server) {
  const sel = root.querySelector(`select[name="${name}"]`);
  if (sel.value !== '__new') return sel.value;
  const user = root.querySelector(`[name="${name}-user"]`).value.trim();
  const password = root.querySelector(`[name="${name}-pass"]`).value;
  if (!user || !password) throw new Error('enter a login and password');
  const host = (server || '').split(/[,\\:]/)[0].replace(/[^A-Za-z0-9_.-]/g, '-');
  const credName = `${user}@${host || 'sql'}`.replace(/[^A-Za-z0-9_.@-]/g, '-').slice(0, 80);
  const saved = await api('/api/credentials', { method: 'POST', body: { name: credName, user, password } });
  await loadStatus();
  sel.innerHTML = credOptions(saved.name);
  root.querySelector(`[data-newcred="${name}"]`).hidden = true;
  return saved.name;
}

function presetChoices(name, current) {
  const known = PRESETS.some((p) => p.cron === current) || !current;
  const def = status?.default_schedule;
  return `<div class="choices">
    ${def ? `<label class="choice ${!current ? 'on' : ''}"><input type="radio" name="${name}" value="" ${!current ? 'checked' : ''}>
      <div><b>Service default</b><span class="muted small">${esc(PRESETS.find((p) => p.cron === def)?.title || def)} (SYNC_SCHEDULE)</span></div></label>` : ''}
    ${PRESETS.map((p) => `<label class="choice ${current === p.cron ? 'on' : ''}"><input type="radio" name="${name}" value="${p.cron}" ${current === p.cron ? 'checked' : ''}>
      <div><b>${p.title}</b>${p.note ? `<span class="muted small">${p.note}</span>` : ''}</div></label>`).join('')}
    <label class="choice ${!known ? 'on' : ''}"><input type="radio" name="${name}" value="custom" ${!known ? 'checked' : ''}>
      <div style="flex:1"><b>Custom schedule (cron)</b>
        <input type="text" name="${name}-cron" value="${esc(!known ? current : '')}" placeholder="e.g. 15 */2 * * *" style="margin-top:8px">
        <span class="muted small" data-cron-preview></span></div></label>
  </div>`;
}

function readSchedule(root, name) {
  const v = root.querySelector(`input[name="${name}"]:checked`)?.value ?? '';
  return v === 'custom' ? root.querySelector(`input[name="${name}-cron"]`).value.trim() : v;
}

function wireChoices(root) {
  root.querySelectorAll('.choices').forEach((box) => box.addEventListener('change', () => {
    box.querySelectorAll('.choice').forEach((c) => c.classList.toggle('on', c.querySelector('input[type=radio]').checked));
  }));
}

function wireCronPreview(root, name) {
  const out = root.querySelector('[data-cron-preview]');
  const input = root.querySelector(`input[name="${name}-cron"]`);
  if (!out || !input) return;
  const upd = async () => {
    const v = input.value.trim();
    if (!v) { out.textContent = ''; return; }
    try {
      const r = await api(`/api/cron-preview?expr=${encodeURIComponent(v)}`);
      out.textContent = r.next.length ? `next: ${r.next.map((x) => new Date(x).toLocaleString(undefined, { timeZone: r.timezone, hour: '2-digit', minute: '2-digit', day: 'numeric', month: 'short' })).join(', ')} (${r.timezone})` : 'invalid expression';
    } catch { out.textContent = ''; }
  };
  input.addEventListener('input', () => { clearTimeout(upd.h); upd.h = setTimeout(upd, 300); });
  input.addEventListener('focus', () => {
    root.querySelector(`input[name="${name}"][value=custom]`).checked = true;
    root.querySelector('.choices').dispatchEvent(new Event('change'));
  });
  upd();
}

const KEY_TEXT = { int: 'integer key', guid: 'GUID key', composite: 'composite key', other: 'key', none: 'no key' };

function tablePicker(prefix, tables, selected) {
  const all = selected.includes('*');
  const sel = new Set(selected.filter((x) => x !== '*').map((x) => x.toLowerCase()));
  const totalRows = tables.reduce((s, t) => s + t.rows, 0);
  return `<div class="group" data-picker="${prefix}">
    <div class="row-item"><div class="label">All tables<small>including ones added later</small></div>
      <div class="value right">${sw(`${prefix}-all`, all)}</div></div>
    <div data-pick-list ${all ? 'hidden' : ''}>
      <div class="picker-tools"><input type="search" placeholder="Find a table" data-pick-search>
        <button type="button" class="sm" data-pick="all">Select all</button><button type="button" class="sm" data-pick="none">Clear</button>
        <span class="muted small" data-pick-count></span></div>
      <div class="picker-list">${tables.map((t) => {
        const full = `${t.schema}.${t.name}`;
        return `<label data-name="${esc(full.toLowerCase())}"><input type="checkbox" value="${esc(full)}" ${sel.has(full.toLowerCase()) ? 'checked' : ''}>
          <span class="tname">${esc(full)}</span>
          <span class="tmeta">${num(t.rows)} ${plural(t.rows, 'row', 'rows')} · ${t.size_mb >= 1 ? `${Math.round(t.size_mb)} MB · ` : ''}${t.cols} cols · ${KEY_TEXT[t.key] || t.key}</span></label>`;
      }).join('')}</div>
    </div>
    <div class="hint" style="padding:0 16px 12px">${tables.length} ${plural(tables.length, 'table', 'tables')}, ${num(totalRows)} ${plural(totalRows, 'row', 'rows')}.
      Only changed rows are transferred. Tables without a primary key are supported too.</div>
  </div>`;
}

function wirePicker(root, prefix) {
  const box = root.querySelector(`[data-picker="${prefix}"]`);
  if (!box) return () => [];
  const list = box.querySelector('[data-pick-list]');
  const allSw = box.querySelector(`input[name="${prefix}-all"]`);
  const count = box.querySelector('[data-pick-count]');
  const boxes = () => [...box.querySelectorAll('.picker-list input[type=checkbox]')];
  const upd = () => { count.textContent = `${boxes().filter((b) => b.checked).length} selected`; };
  allSw.addEventListener('change', () => { list.hidden = allSw.checked; });
  box.querySelector('[data-pick-search]').addEventListener('input', (e) => {
    const q = e.target.value.trim().toLowerCase();
    box.querySelectorAll('.picker-list label').forEach((l) => { l.hidden = !!q && !l.dataset.name.includes(q); });
  });
  box.querySelectorAll('[data-pick]').forEach((b) => b.addEventListener('click', () => {
    boxes().filter((x) => !x.closest('label').hidden).forEach((x) => { x.checked = b.dataset.pick === 'all'; });
    upd();
  }));
  box.addEventListener('change', upd);
  upd();
  return () => (allSw.checked ? ['*'] : boxes().filter((b) => b.checked).map((b) => b.value));
}

function targetSentence(t) {
  if (!t) return '';
  if (!t.reachable) return `<span class="err-text">Server unreachable: ${esc(t.error)}</span>`;
  if (!t.exists) return `Database <b>${esc(t.database)}</b> does not exist on this server. It can be created with the source collation (<code>${esc(t.source_collation || '?')}</code>).`;
  return `Database <b>${esc(t.database)}</b> exists: ${t.tables} ${plural(t.tables, 'table', 'tables')}, ${Math.round(t.size_mb)} MB, collation <code>${esc(t.collation)}</code>.`;
}

// ---------- wizard: new sync ----------

const STEPS = ['Source', 'Target', 'What to sync', 'Schedule', 'Git', 'Review'];

async function viewWizard() {
  await loadStatus();
  const d = {
    step: 0, source: { server: '', database: '', credential: status.credentials[0] || '' },
    target: { server: '', database: '', credential: '' }, targetAction: 'create', targetStatus: null,
    git: { enabled: true, repoUrl: '', branch: '', path: '', auth: 'token', username: '', secret: '' },
    publish: true, data: true, tables: ['*'], schedule: status.default_schedule ? '' : '*/10 * * * *',
    name: '', runNow: true, dbs: null, tableList: null, readTables: null,
  };
  const stepsBar = () => `<div class="steps">${STEPS.map((s, i) => `<span class="${i < d.step ? 'done' : i === d.step ? 'now' : ''}">${i + 1}. ${s}</span>`).join('')}</div>`;
  const nav = (nextLabel) => `<div class="bottombar"><div class="inner">
      <a class="btn" href="#/">Cancel</a><span class="spacer"></span>
      <span class="test-result err" data-err></span>
      ${d.step > 0 ? '<button type="button" data-back>Back</button>' : ''}
      <button type="button" class="primary" data-next>${nextLabel}</button></div></div>`;
  const showErr = (m) => { const e = $app.querySelector('[data-err]'); if (e) e.textContent = m || ''; };

  const steps = [
    { // 1. source
      render: () => `<p class="wiz-q">Where does the data come from?</p>
        <p class="wiz-sub">The source server and database. The service only reads from it: no setup, no triggers, no CDC, and minimal locking.</p>
        <div class="group"><div class="fields">
          <label class="field"><span>SQL Server</span><input type="text" name="srv" value="${esc(d.source.server)}" placeholder="sql-prod.company.local or sql-prod,1433" autofocus></label>
          <div class="field"><span>Credentials</span>${credField('cred', d.source.credential)}</div>
          <div class="field"><span>Database</span>
            <div style="display:flex;gap:8px"><select name="db" ${d.dbs ? '' : 'disabled'}>${d.dbs ? d.dbs.map((x) => `<option ${x === d.source.database ? 'selected' : ''}>${esc(x)}</option>`).join('') : '<option>connect to the server first</option>'}</select>
              <button type="button" data-connect>${d.dbs ? 'Refresh' : 'Connect'}</button></div>
            <span class="test-result" data-conn></span></div>
        </div></div>`,
      wire: (root) => {
        const connect = async () => {
          const out = root.querySelector('[data-conn]');
          d.source.server = root.querySelector('[name=srv]').value.trim();
          if (!d.source.server) { out.className = 'test-result err'; out.textContent = 'enter a server'; return; }
          try { d.source.credential = await ensureCred(root, 'cred', d.source.server); } catch (e) { out.className = 'test-result err'; out.textContent = e.message; return; }
          out.className = 'test-result'; out.textContent = 'connecting…';
          const r = await api('/api/discover/databases', { method: 'POST', body: { ...d.source, database: 'master' } });
          if (!r.ok) { out.className = 'test-result err'; out.textContent = r.message; return; }
          d.dbs = r.databases; d.tableList = null;
          if (!d.dbs.includes(d.source.database)) d.source.database = d.dbs[0] || '';
          draw();
        };
        wireCreds(root);
        root.querySelector('[data-connect]').addEventListener('click', connect);
        root.querySelector('[name=srv]').addEventListener('keydown', (e) => { if (e.key === 'Enter') connect(); });
      },
      next: (root) => {
        if (!d.dbs) return 'connect to the server and choose a database';
        const db = root.querySelector('[name=db]').value;
        if (db !== d.source.database) d.tableList = null;
        d.source.database = db;
        if (!d.target.server) Object.assign(d.target, { server: d.source.server, credential: d.source.credential, database: `${db}_copy` });
        if (!d.name) d.name = db.toLowerCase().replace(/[^a-z0-9_.-]/g, '-');
      },
    },
    { // 2. target
      render: () => `<p class="wiz-q">Where should it sync to?</p>
        <p class="wiz-sub">The copy database. If it does not exist, the service creates it with the same collation as the source.
          If it already exists, you need to explicitly allow writing to it.</p>
        <div class="group"><div class="fields">
          <label class="field"><span>Server</span><input type="text" name="srv" value="${esc(d.target.server)}"></label>
          <div class="field"><span>Credentials</span>${credField('cred', d.target.credential)}</div>
          <label class="field"><span>Database</span><input type="text" name="db" value="${esc(d.target.database)}"></label>
        </div>
        <div class="row-item" data-tstatus><span class="muted">Checking…</span></div></div>`,
      wire: (root) => {
        const box = root.querySelector('[data-tstatus]');
        wireCreds(root);
        const check = async () => {
          const server = root.querySelector('[name=srv]').value.trim();
          let credential;
          try { credential = await ensureCred(root, 'cred', server); } catch (e) { box.innerHTML = `<span class="muted">${esc(e.message)}</span>`; return; }
          d.target = { server, credential, database: root.querySelector('[name=db]').value.trim() };
          if (!d.target.server || !d.target.database) { box.innerHTML = '<span class="muted">enter a server and database</span>'; return; }
          box.innerHTML = '<span class="muted">Checking…</span>';
          const t = d.targetStatus = await api('/api/discover/target', { method: 'POST', body: { source: d.source, target: d.target } });
          d.targetAction = !t.exists ? 'create' : (d.targetAction === 'approve' ? 'approve' : 'later');
          box.innerHTML = `<div class="value">${targetSentence(t)}
            ${t.reachable && !t.exists ? `<div style="margin-top:10px;display:flex;gap:10px;align-items:center">${sw('tcreate', true)} Create the database on save</div>` : ''}
            ${t.exists ? `<div class="choices" style="padding:10px 0 0">
              <label class="choice ${d.targetAction === 'approve' ? 'on' : ''}"><input type="radio" name="tact" value="approve" ${d.targetAction === 'approve' ? 'checked' : ''}><div><b>Allow writing now</b>
                <span class="muted small">the schema and data of selected tables in this database will be overwritten</span></div></label>
              <label class="choice ${d.targetAction === 'later' ? 'on' : ''}"><input type="radio" name="tact" value="later" ${d.targetAction === 'later' ? 'checked' : ''}><div><b>Approve later</b>
                <span class="muted small">the sync will not run until approved</span></div></label></div>` : ''}</div>`;
          wireChoices(box);
          box.querySelector('[name=tcreate]')?.addEventListener('change', (e) => { d.targetAction = e.target.checked ? 'create' : 'later'; });
          box.querySelectorAll('[name=tact]').forEach((r) => r.addEventListener('change', () => { d.targetAction = r.value; }));
        };
        root.querySelectorAll('input[type=text], input[type=password], select').forEach((el) => el.addEventListener('change', check));
        check();
      },
      next: () => {
        const t = d.targetStatus;
        if (!t) return 'wait for the target check to finish';
        if (!t.reachable) return 'target server is unreachable';
        if (t.server.toLowerCase() === d.source.server.toLowerCase() && t.database.toLowerCase() === d.source.database.toLowerCase()) return 'target is the same as the source';
      },
    },
    { // 3. what
      render: () => `<p class="wiz-q">What should it sync?</p>
        <p class="wiz-sub">Usually you want all three: schema history in git, the same schema in the target, and table data.</p>
        <div class="group">
          <div class="row-item"><div class="label">Schema history in git<small>a file per table, procedure, function, view and permission set; a commit on every change</small></div><div class="value right">${sw('git', d.git.enabled)}</div></div>
          <div class="row-item"><div class="label">Schema to target<small>creates and alters objects in the target only when the source schema changes</small></div><div class="value right">${sw('publish', d.publish)}</div></div>
          <div class="row-item"><div class="label">Table data<small>only changed rows are transferred, compressed</small></div><div class="value right">${sw('data', d.data)}</div></div>
        </div>
        <div data-tables ${d.data ? '' : 'hidden'}><h2>Tables</h2><div class="group"><div class="row-item muted">Loading tables…</div></div></div>`,
      wire: async (root) => {
        const wrap = root.querySelector('[data-tables]');
        root.querySelector('[name=data]').addEventListener('change', (e) => { wrap.hidden = !e.target.checked; });
        if (!d.tableList) {
          const r = await api('/api/discover/tables', { method: 'POST', body: d.source });
          if (!r.ok) { wrap.innerHTML = `<h2>Tables</h2><div class="callout err"><div class="body">${esc(r.message)}</div></div>`; return; }
          d.tableList = r.tables;
        }
        wrap.innerHTML = `<h2>Tables</h2>${tablePicker('w', d.tableList, d.tables)}`;
        d.readTables = wirePicker(wrap, 'w');
      },
      next: (root) => {
        d.git.enabled = root.querySelector('[name=git]').checked;
        d.publish = root.querySelector('[name=publish]').checked;
        d.data = root.querySelector('[name=data]').checked;
        if (d.data && d.readTables) d.tables = d.readTables();
        if (d.data && !d.tables.length) return 'select at least one table or turn off “Table data”';
        if (!d.git.enabled && !d.publish && !d.data) return 'turn on at least one option';
      },
    },
    { // 4. schedule
      render: () => `<p class="wiz-q">How often?</p>
        <p class="wiz-sub">Tables that have not changed since the last run are not even read, so frequent runs cost almost nothing.</p>
        <div class="group">${presetChoices('sched', d.schedule)}</div>
        <h2>Network usage</h2><div class="group"><div class="row-item"><div class="label">Bandwidth limit, Mbit/s<small>keeps the sync from using the whole link; leave empty for no limit</small></div>
          <div class="value"><input type="number" min="0" step="any" name="mbit" value="${d.maxMbit || ''}" placeholder="no limit"></div></div></div>`,
      wire: (root) => { wireChoices(root); wireCronPreview(root, 'sched'); },
      next: (root) => {
        d.maxMbit = Number(root.querySelector('[name=mbit]').value) || 0;
        d.schedule = readSchedule(root, 'sched');
        if (root.querySelector('input[name=sched]:checked')?.value === 'custom' && !d.schedule) return 'enter a cron expression';
      },
    },
    { // 5. git
      skip: () => !d.git.enabled,
      render: () => `<p class="wiz-q">Where should the schema history go?</p>
        <p class="wiz-sub">GitLab, GitHub, Gitea or Azure DevOps. The key is stored encrypted and never shown again.</p>
        <div class="group"><div class="fields">
          <label class="field"><span>Repository</span><input type="text" name="repo" value="${esc(d.git.repoUrl)}" placeholder="${esc(status.git_repo || 'https://gitlab.company.local/dba/schemas.git')}">
            ${status.git_repo ? `<span class="hint">leave empty to use the service repository: ${esc(status.git_repo)}</span>` : ''}</label>
          <div class="fields two" style="padding:0">
            <label class="field"><span>Branch</span><input type="text" name="branch" value="${esc(d.git.branch)}" placeholder="${esc(status.git_branch || 'main')}"></label>
            <label class="field"><span>Folder in repository</span><input type="text" name="path" value="${esc(d.git.path)}" placeholder="${esc(d.name)}"></label>
          </div>
          <div class="field"><span>Access</span><div class="segmented" data-auth>
            <button type="button" data-v="token" class="${d.git.auth !== 'ssh' ? 'on' : ''}">Token (HTTPS)</button>
            <button type="button" data-v="ssh" class="${d.git.auth === 'ssh' ? 'on' : ''}">SSH key</button></div></div>
          <label class="field" data-user ${d.git.auth === 'ssh' ? 'hidden' : ''}><span>Username</span><input type="text" name="user" value="${esc(d.git.username)}" placeholder="oauth2 for GitLab, anything for GitHub"></label>
          <label class="field"><span data-secret-label>${d.git.auth === 'ssh' ? 'SSH private key' : 'Access token'}</span>
            <textarea name="secret" rows="2" placeholder="${d.git.auth === 'ssh' ? '-----BEGIN OPENSSH PRIVATE KEY-----' : 'glpat-… / ghp_…'}" autocomplete="off" spellcheck="false">${esc(d.git.secret)}</textarea>
            ${status.secret_key_set ? '' : '<span class="hint err-text">SECRET_KEY is not set in the environment, so the key cannot be saved</span>'}</label>
          <div><button type="button" data-testgit>Test access</button> <span class="test-result" data-gitres></span></div>
        </div></div>`,
      wire: (root) => {
        root.querySelectorAll('[data-auth] button').forEach((b) => b.addEventListener('click', () => {
          d.git.auth = b.dataset.v;
          root.querySelectorAll('[data-auth] button').forEach((x) => x.classList.toggle('on', x === b));
          root.querySelector('[data-user]').hidden = d.git.auth === 'ssh';
          root.querySelector('[data-secret-label]').textContent = d.git.auth === 'ssh' ? 'SSH private key' : 'Access token';
        }));
        root.querySelector('[data-testgit]').addEventListener('click', async () => {
          readGit(root);
          const out = root.querySelector('[data-gitres]');
          out.className = 'test-result'; out.textContent = 'checking…';
          const r = await api('/api/test-git', { method: 'POST', body: { git: d.git } });
          out.className = `test-result ${r.ok ? 'ok' : 'err'}`; out.textContent = r.message;
        });
      },
      next: (root) => {
        readGit(root);
        if (!d.git.repoUrl && !status.git_repo) return 'enter a repository';
      },
    },
    { // 6. review
      render: () => {
        const t = d.targetStatus;
        const canRun = !needsTargetDraft() || !t.exists || d.targetAction === 'approve';
        const targetLine = !t.exists ? (d.targetAction === 'create' ? 'will be created' : 'needs to be created later') : d.targetAction === 'approve' ? 'existing, writing allowed' : 'existing, approve later';
        const what = [d.git.enabled && 'schema history in git', d.publish && 'schema', d.data && (d.tables.includes('*') ? 'data of all tables' : `data of ${d.tables.length} ${plural(d.tables.length, 'table', 'tables')}`)].filter(Boolean).join(', ');
        const sched = d.schedule === '' ? `service default (${status.default_schedule})` : (PRESETS.find((p) => p.cron === d.schedule)?.title || d.schedule);
        return `<p class="wiz-q">Look good?</p><p class="wiz-sub">You can change all of this later in settings.</p>
          <div class="group">
            <div class="row-item"><div class="label">Name</div><div class="value"><input type="text" name="name" value="${esc(d.name)}"><span class="hint">Latin letters, digits, _ . -</span></div></div>
            <div class="row-item"><div class="label">Source</div><div class="value">${esc(d.source.database)} <span class="muted">on ${esc(d.source.server)}</span></div></div>
            ${needsTargetDraft() ? `<div class="row-item"><div class="label">Target</div><div class="value">${esc(d.target.database)} <span class="muted">on ${esc(d.target.server)} · ${targetLine}</span></div></div>` : ''}
            <div class="row-item"><div class="label">What</div><div class="value">${esc(what)}</div></div>
            <div class="row-item"><div class="label">When</div><div class="value">${esc(sched)}</div></div>
            ${d.data ? `<div class="row-item"><div class="label">Bandwidth</div><div class="value">${esc(limitText(d.maxMbit))}</div></div>` : ''}
            ${d.git.enabled ? `<div class="row-item"><div class="label">Git</div><div class="value">${esc(d.git.repoUrl || status.git_repo)} <span class="muted">· ${esc(d.git.branch || status.git_branch)} · folder ${esc(d.git.path || d.name)}</span></div></div>` : ''}
            <div class="row-item"><div class="label">Run now<small>${canRun ? 'the first run copies everything, later runs copy only changes' : 'available after the target is approved'}</small></div>
              <div class="value right">${sw('runnow', d.runNow && canRun, canRun ? '' : 'disabled')}</div></div>
          </div><div data-errs style="margin-top:16px"></div>`;
      },
      next: async (root) => {
        d.name = root.querySelector('[name=name]').value.trim();
        d.runNow = root.querySelector('[name=runnow]').checked;
        const body = {
          name: d.name, enabled: true, schedule: d.schedule,
          config: { source: d.source, target: d.target, git: { ...d.git },
            schema: { publish: d.publish, blockOnPossibleDataLoss: true, excludeObjectTypes: 'Logins;Users;RoleMembership' },
            data: { tables: d.data ? d.tables : [], mode: 'diff', maxMbit: d.maxMbit || 0 } },
        };
        let job;
        try { job = await api('/api/jobs', { method: 'POST', body }); } catch (e) {
          root.querySelector('[data-errs]').innerHTML = `<div class="errors"><ul>${e.details.map((x) => `<li>${esc(x)}</li>`).join('')}</ul></div>`;
          return 'fix the errors';
        }
        leaveGuard = null;
        try {
          if (needsTargetDraft() && !d.targetStatus.exists && d.targetAction === 'create') await api(`/api/jobs/${job.id}/target/create`, { method: 'POST' });
          else if (needsTargetDraft() && d.targetAction === 'approve') await api(`/api/jobs/${job.id}/target/approve`, { method: 'POST' });
          if (d.runNow) { const r = await api(`/api/jobs/${job.id}/run`, { method: 'POST' }); location.hash = `#/runs/${r.run_id}`; return; }
        } catch (e) { toast(e.message); }
        location.hash = `#/jobs/${job.id}`;
      },
    },
  ];

  function needsTargetDraft() { return d.publish || d.data; }
  function readGit(root) {
    Object.assign(d.git, { repoUrl: root.querySelector('[name=repo]').value.trim(), branch: root.querySelector('[name=branch]').value.trim(),
      path: root.querySelector('[name=path]').value.trim(), username: root.querySelector('[name=user]').value.trim(),
      secret: root.querySelector('[name=secret]').value.trim() });
  }

  async function draw() {
    const st = steps[d.step];
    const last = d.step === steps.length - 1;
    $app.innerHTML = `<a class="back" href="#/">‹ Syncs</a><div class="page-head"><h1>New sync</h1></div>
      ${stepsBar()}<div data-step>${st.render()}</div>${nav(last ? 'Create sync' : 'Next')}`;
    leaveGuard = () => (d.source.server ? 'Leave setup? What you entered will not be saved.' : null);
    const root = $app.querySelector('[data-step]');
    $app.querySelector('[data-next]').addEventListener('click', async (e) => {
      e.target.disabled = true;
      showErr('');
      try {
        const err = await st.next(root);
        if (err) { showErr(err); return; }
        if (last) return;
        do { d.step++; } while (steps[d.step].skip?.());
        draw();
      } catch (x) { showErr(x.message); } finally { e.target.disabled = false; }
    });
    $app.querySelector('[data-back]')?.addEventListener('click', () => {
      do { d.step--; } while (d.step > 0 && steps[d.step].skip?.());
      draw();
    });
    try { await st.wire?.(root); } catch (x) { showErr(x.message); }
  }
  draw();
}

// ---------- sync page ----------

const TABS = [['', 'Overview'], ['runs', 'Runs'], ['schema', 'Schema changes'], ['settings', 'Settings']];

async function viewJob(id, tab) {
  await loadStatus();
  let job = await api(`/api/jobs/${id}`);
  const head = () => {
    const s = jobState(job);
    return `<a class="back" href="#/">‹ Syncs</a>
      <div class="page-head"><div><h1>${esc(job.name)}</h1>
        <div class="sub"><span class="dot ${s.dot}" style="display:inline-block;margin-right:6px"></span>${esc(s.text)} · ${esc(nextText(job))}</div></div>
        <div class="actions">${job.active && job.last_run_id ? `<a class="btn" href="#/runs/${job.last_run_id}">Open current run</a>` :
          `<button class="primary" ${s.blocked ? 'disabled title="approve the target first"' : ''} data-run>Sync now</button>`}</div></div>
      <div class="segmented" style="margin-bottom:20px">${TABS.map(([k, t]) => `<a href="#/jobs/${id}${k ? '/' + k : ''}" class="${(tab || '') === k ? 'on' : ''}">${t}</a>`).join('')}</div>`;
  };
  const wireHead = () => $app.querySelector('[data-run]')?.addEventListener('click', () => runNow(id));
  const shell = () => { $app.innerHTML = head() + '<div data-body></div>'; wireHead(); return $app.querySelector('[data-body]'); };

  if (tab === 'runs') return viewRunsInto(shell(), id);
  if (tab === 'schema') return viewSchemaInto(shell(), id);
  if (tab === 'settings') return viewSettingsInto(shell(), job);

  const render = async () => {
    job = await api(`/api/jobs/${id}`);
    const c = job.config;
    const last = job.last_run_id ? await api(`/api/runs/${job.last_run_id}`) : null;
    const tables = Object.entries(job.state?.tables || {}).sort(([a], [b]) => a.localeCompare(b));
    $app.innerHTML = head() + `
      <div data-target></div>
      ${last ? `<h2>Last run</h2><div class="group">
        <div class="metrics">
          <div class="metric"><span>Status</span><b class="${last.status === 'failed' ? 'err-text' : last.status === 'succeeded' ? 'ok-text' : ''}">${STATUS_TEXT[last.status]}</b></div>
          <div class="metric"><span>When</span><b>${esc(rel(last.started_at || last.queued_at))}</b></div>
          <div class="metric"><span>Duration</span><b>${duration(last.started_at, last.finished_at)}</b></div>
          <div class="metric"><span>Transferred</span><b>${traffic(last.stats)}</b></div>
          <div class="metric"><span>Rows changed</span><b>${num(last.stats?.rows_changed)}</b></div>
        </div>
        ${last.summary ? `<div class="row-item"><div class="value small ${last.status === 'failed' ? 'err-text' : 'muted'}">${esc(last.summary)}</div></div>` : ''}
        <a class="row-item" href="#/runs/${last.id}"><div class="value">Run log</div><span class="chev"></span></a></div>` : ''}
      <h2>What it does</h2><div class="group">
        <div class="row-item"><div class="label">Source</div><div class="value">${esc(c.source.database)} <span class="muted">· ${esc(c.source.server)}</span>
          <div class="small muted" data-size="source">calculating size…</div></div></div>
        ${needsTarget(c) ? `<div class="row-item"><div class="label">Target</div><div class="value">${esc(c.target.database)} <span class="muted">· ${esc(c.target.server)}</span>
          <div class="small muted" data-size="target">calculating size…</div></div></div>` : ''}
        <div class="row-item"><div class="label">Schema history</div><div class="value">${c.git?.enabled ? `git: ${esc(c.git.repoUrl || status.git_repo || '—')} <span class="muted">· ${esc(c.git.branch || status.git_branch)} · ${esc(c.git.path || job.name)}/</span>` : '<span class="muted">off</span>'}</div></div>
        <div class="row-item"><div class="label">Schema to target</div><div class="value">${c.schema?.publish ? 'published when the source schema changes' : '<span class="muted">off</span>'}</div></div>
        <div class="row-item"><div class="label">Data</div><div class="value">${!c.data?.tables?.length ? '<span class="muted">off</span>' : c.data.tables.includes('*') ? 'all tables' : `${c.data.tables.length} ${plural(c.data.tables.length, 'table', 'tables')}`}${c.data?.mode === 'replace' ? ' · full reload every time' : c.data?.tables?.length ? ' · changes only' : ''}</div></div>
        <div class="row-item"><div class="label">Schedule</div><div class="value">${esc(scheduleText(job))}${job.next_run_at && job.enabled ? ` <span class="muted">· next ${esc(rel(job.next_run_at))}</span>` : ''}</div></div>
        ${c.data?.tables?.length ? `<div class="row-item"><div class="label">Bandwidth</div><div class="value">${esc(limitText(c.data.maxMbit))}${status.pod_max_mbit ? ` <span class="muted">· pod-wide limit ${status.pod_max_mbit} Mbit/s</span>` : ''}</div></div>` : ''}
        <a class="row-item" href="#/settings"><div class="label">Resources</div><div class="value" data-jobres><span class="muted">calculating…</span></div><span class="chev"></span></a>
      </div>
      ${tables.length ? `<h2>Tables</h2><div class="group table-wrap"><table class="tbl"><thead><tr><th>Table</th><th class="num">Rows</th><th class="num">Size in source</th><th class="num">In target</th><th>Verified</th><th class="num">Changed last run</th><th class="num">Batch</th></tr></thead><tbody>
        ${tables.map(([k, t]) => `<tr><td>${esc(k)}${t.reloading ? ' <span class="badge b-warn">reload incomplete</span>' : ''}</td>
          <td class="num">${t.rows ? num(t.rows) : '—'}</td><td class="num" data-tsize="source:${esc(k)}">…</td><td class="num" data-tsize="target:${esc(k)}">…</td>
          <td class="muted">${esc(rel(t.lastVerified) || '—')}</td>
          <td class="num">${num(t.lastDiff)}</td><td class="num">${t.batchRows ? num(t.batchRows) : '—'}</td></tr>`).join('')}
      </tbody></table></div><p class="hint">Tables unchanged since the last run are not read. Each table is fully verified once a day.</p>` : ''}`;
    wireHead();
    if (needsTarget(c)) renderTarget($app.querySelector('[data-target]'), id, () => route());
    jobSizes(id).then((sz) => {
      for (const side of ['source', 'target']) {
        const el = $app.querySelector(`[data-size="${side}"]`);
        if (el) el.innerHTML = dbLine(sz[side]);
        $app.querySelectorAll(`[data-tsize^="${side}:"]`).forEach((td) => {
          const t = sz[side]?.per_table?.[td.dataset.tsize.slice(side.length + 1)];
          td.textContent = t ? mb(t.size_mb) : '—';
        });
      }
    }).catch(() => {});
    api('/api/sizing').then((sz) => {
      const js = sz.jobs.find((x) => x.job_id === id), el = $app.querySelector('[data-jobres]');
      if (!el || !js) return;
      el.innerHTML = js.runs ? `run ~${secs(js.dur_p95_s)}${js.interval_s ? ` (${Math.round(js.duty * 100)}% of interval)` : ''} · memory up to ${bytes(js.mem_peak)} · CPU ${cores(js.cpu_p95)}
        · network ${bytes(js.in_per_run)} ↓ / ${bytes(js.out_per_run)} ↑ per run${js.warning ? `<div class="small warn-text">${esc(js.warning)}</div>` : ''}` : '<span class="muted">available after the first run</span>';
    }).catch(() => {});
  };
  await render();
  poll(render, 8000);
}

async function renderTarget(box, id, onChange) {
  let t;
  try { t = await api(`/api/jobs/${id}/target`); } catch (e) { box.innerHTML = `<div class="callout err"><div class="body">${esc(e.message)}</div></div>`; return; }
  if (t.approved && t.reachable && t.exists) { box.innerHTML = ''; return; }
  let html;
  if (!t.reachable) html = `<div class="callout err"><div class="body"><b>Target server is unreachable</b><div class="small">${esc(t.error)}</div>
      <div class="acts"><button class="sm" data-act="refresh">Check again</button></div></div></div>`;
  else if (!t.exists) html = `<div class="callout"><div class="body"><b>Database ${esc(t.database)} does not exist on ${esc(t.server)}</b>
      <div class="small">Create it here with the source collation (${esc(t.source_collation || '?')}). Once it is created, the sync is allowed.</div>
      <div class="acts"><button class="primary sm" data-act="create">Create database</button></div></div></div>`;
  else html = `<div class="callout"><div class="body"><b>Allow writing to the existing database</b>
      <div class="small">${targetSentence(t)} The service will publish the source schema to it and overwrite the data of selected tables.
      The sync will not run until you approve it.</div>
      <div class="acts"><button class="primary sm" data-act="approve">Approve target</button></div></div></div>`;
  box.innerHTML = html + '<div class="test-result err" data-tmsg></div>';
  box.querySelectorAll('[data-act]').forEach((b) => b.addEventListener('click', async () => {
    if (b.dataset.act === 'refresh') return renderTarget(box, id, onChange);
    const q = b.dataset.act === 'create' ? `Create database ${t.database} on ${t.server}?` : `Allow syncing into ${t.database} on ${t.server}? The schema and data of selected tables will be overwritten.`;
    if (!confirm(q)) return;
    b.disabled = true;
    try {
      const r = await api(`/api/jobs/${id}/target/${b.dataset.act}`, { method: 'POST' });
      toast(r.message || 'Target approved');
      onChange();
    } catch (e) { box.querySelector('[data-tmsg]').textContent = e.message; b.disabled = false; }
  }));
}

// ---------- settings tab ----------

async function viewSettingsInto(box, job) {
  const c = job.config, s = c.schema || {}, d = c.data || {}, g = c.git || {};
  box.innerHTML = `<form data-form autocomplete="off">
    <div data-errors></div>
    <h2>General</h2><div class="group">
      <div class="row-item"><div class="label">Name</div><div class="value"><input type="text" name="name" value="${esc(job.name)}"></div></div>
      <div class="row-item"><div class="label">Enabled<small>a disabled sync does not run on schedule</small></div><div class="value right">${sw('enabled', job.enabled)}</div></div>
    </div>
    <h2>Schedule</h2><div class="group">${presetChoices('sched', job.schedule || '')}</div>
    <h2>Network usage</h2><div class="group">
      <div class="row-item"><div class="label">Bandwidth limit, Mbit/s<small>applies separately to inbound (from the source) and outbound (to the target); leave empty for no limit${status.pod_max_mbit ? `; pod-wide limit: ${status.pod_max_mbit} Mbit/s` : ''}</small></div>
        <div class="value"><input type="number" min="0" step="any" name="data.maxMbit" value="${d.maxMbit || ''}" placeholder="no limit"></div></div>
    </div>
    <h2>Source</h2><div class="group">
      <div class="row-item"><div class="label">Server</div><div class="value"><input type="text" name="src.server" value="${esc(c.source.server)}"></div></div>
      <div class="row-item"><div class="label">Database</div><div class="value"><input type="text" name="src.database" value="${esc(c.source.database)}"></div></div>
      <div class="row-item"><div class="label">Credentials</div><div class="value">${credField('src.credential', c.source.credential)}</div></div>
      <div class="row-item"><div class="value"><button type="button" class="sm" data-test="src">Test connection</button> <span class="test-result" data-res="src"></span></div></div>
    </div>
    <h2>Target</h2><div class="group">
      <div class="row-item"><div class="label">Server</div><div class="value"><input type="text" name="tgt.server" value="${esc(c.target?.server)}"></div></div>
      <div class="row-item"><div class="label">Database</div><div class="value"><input type="text" name="tgt.database" value="${esc(c.target?.database)}"></div></div>
      <div class="row-item"><div class="label">Credentials</div><div class="value">${credField('tgt.credential', c.target?.credential)}</div></div>
      <div class="row-item"><div class="value small muted">${job.target_approved ? `Writing allowed${job.approved_by ? ` (${esc(job.approved_by)}, ${esc(rel(job.approved_at))})` : ''}. Changing the server or database will require approval again.` : 'Writing to the target is not allowed yet. Approve it on the Overview tab.'}
        ${job.target_approved ? ' <button type="button" class="link" data-revoke>Revoke approval</button>' : ''}</div></div>
    </div>
    <h2>What to sync</h2><div class="group">
      <div class="row-item"><div class="label">Schema history in git</div><div class="value right">${sw('git.enabled', g.enabled)}</div></div>
      <div class="row-item"><div class="label">Schema to target</div><div class="value right">${sw('schema.publish', s.publish)}</div></div>
      <div class="row-item"><div class="label">Table data</div><div class="value right">${sw('data.on', (d.tables || []).length > 0)}</div></div>
    </div>
    <div data-tables-wrap ${(d.tables || []).length ? '' : 'hidden'}><h2>Tables</h2><div data-tables><div class="group"><div class="row-item muted">Loading tables…</div></div></div></div>
    <div data-git-wrap ${g.enabled ? '' : 'hidden'}><h2>Git</h2><div class="group">
      <div class="row-item"><div class="label">Repository</div><div class="value"><input type="text" name="git.repoUrl" value="${esc(g.repoUrl)}" placeholder="${esc(status.git_repo ? `default: ${status.git_repo}` : 'https://…/repo.git')}"></div></div>
      <div class="row-item"><div class="label">Branch</div><div class="value"><input type="text" name="git.branch" value="${esc(g.branch)}" placeholder="${esc(status.git_branch)}"></div></div>
      <div class="row-item"><div class="label">Folder</div><div class="value"><input type="text" name="git.path" value="${esc(g.path)}" placeholder="${esc(job.name)}"></div></div>
      <div class="row-item"><div class="label">Access</div><div class="value"><div class="segmented" data-auth>
        <button type="button" data-v="token" class="${g.auth !== 'ssh' ? 'on' : ''}">Token</button><button type="button" data-v="ssh" class="${g.auth === 'ssh' ? 'on' : ''}">SSH key</button></div></div></div>
      <div class="row-item" data-user ${g.auth === 'ssh' ? 'hidden' : ''}><div class="label">Username</div><div class="value"><input type="text" name="git.username" value="${esc(g.username)}" placeholder="oauth2 for GitLab"></div></div>
      <div class="row-item"><div class="label">${g.hasSecret ? 'New key' : 'Key / token'}<small>${g.hasSecret ? 'the saved key is hidden; leave empty to keep it' : 'stored encrypted'}</small></div>
        <div class="value"><textarea name="git.secret" rows="2" autocomplete="off" spellcheck="false"></textarea>
        ${g.hasSecret ? '<label class="small" style="display:flex;gap:8px;align-items:center;margin-top:6px"><input type="checkbox" name="git.clearSecret"> remove the saved key</label>' : ''}</div></div>
      <div class="row-item"><div class="value"><button type="button" class="sm" data-testgit>Test access</button> <span class="test-result" data-gitres></span></div></div>
    </div></div>
    <details class="advanced"><summary>Advanced</summary>
      <h2>Data</h2><div class="group">
        <div class="row-item"><div class="label">Mode<small>“Changes only” transfers the minimum; “Full reload” copies whole tables every time</small></div>
          <div class="value"><select name="data.mode"><option value="diff" ${d.mode !== 'replace' ? 'selected' : ''}>Changes only</option><option value="replace" ${d.mode === 'replace' ? 'selected' : ''}>Full reload</option></select></div></div>
        <div class="row-item"><div class="label">Parallel tables<small>more makes runs faster but adds load on both servers; default is 2</small></div><div class="value"><input type="number" min="1" max="16" name="data.parallelTables" value="${d.parallelTables || ''}" placeholder="2"></div></div>
        <div class="row-item"><div class="label">Batch size, rows<small>leave empty to size automatically</small></div><div class="value"><input type="number" min="0" name="data.chunkRows" value="${d.chunkRows || ''}" placeholder="automatic"></div></div>
        <div class="row-item"><div class="label">Tail window, rows<small>for huge tables with an increasing integer key: each run checks only the last N rows</small></div><div class="value"><input type="number" min="0" name="data.windowRows" value="${d.windowRows || ''}" placeholder="off"></div></div>
        <div class="row-item"><div class="label">Full check (cron)<small>when the tail window is on</small></div><div class="value"><input type="text" name="data.fullCheck" value="${esc(d.fullCheck)}" placeholder="0 2 * * * (nightly)"></div></div>
        <div class="row-item"><div class="label">Recheck foreign keys<small>scans tables under a schema lock</small></div><div class="value right">${sw('data.verifyForeignKeys', d.verifyForeignKeys)}</div></div>
      </div>
      <h2>Schema</h2><div class="group">
        <div class="row-item"><div class="label">Drop extra objects<small>ones that are not in the source</small></div><div class="value right">${sw('schema.dropObjectsNotInSource', s.dropObjectsNotInSource)}</div></div>
        <div class="row-item"><div class="label">Stop on data loss<small>for example, when dropping a column that has data</small></div><div class="value right">${sw('schema.blockOnPossibleDataLoss', s.blockOnPossibleDataLoss ?? true)}</div></div>
        <div class="row-item"><div class="label">Publish every time<small>reverts manual changes in the target</small></div><div class="value right">${sw('schema.alwaysPublish', s.alwaysPublish)}</div></div>
        <div class="row-item"><div class="label">Excluded object types</div><div class="value"><input type="text" name="schema.excludeObjectTypes" value="${esc(s.excludeObjectTypes ?? 'Logins;Users;RoleMembership')}"></div></div>
      </div>
      <h2>Delete</h2><div class="group"><div class="row-item"><div class="label">Delete sync<small>along with its run and schema change history; databases and git are not touched</small></div>
        <div class="value right"><button type="button" class="danger sm" data-del>Delete</button></div></div></div>
    </details>
  </form>
  <div class="bottombar" data-savebar hidden><div class="inner"><span>Unsaved changes</span><span class="spacer"></span>
    <button type="button" data-cancel>Discard</button><button type="button" class="primary" data-save>Save</button></div></div>`;

  const form = box.querySelector('[data-form]');
  const v = (n) => form.elements[n];
  wireChoices(form);
  wireCronPreview(form, 'sched');
  let readTables = () => d.tables || [];
  let auth = g.auth || 'token';
  let dirty = false;
  const bar = box.querySelector('[data-savebar]');
  const markDirty = () => { dirty = true; bar.hidden = false; };
  form.addEventListener('input', markDirty);
  form.addEventListener('change', markDirty);
  leaveGuard = () => (dirty ? 'You have unsaved changes. Leave without saving?' : null);

  v('data.on').addEventListener('change', (e) => { box.querySelector('[data-tables-wrap]').hidden = !e.target.checked; });
  v('git.enabled').addEventListener('change', (e) => { box.querySelector('[data-git-wrap]').hidden = !e.target.checked; });
  form.querySelectorAll('[data-auth] button').forEach((b) => b.addEventListener('click', () => {
    auth = b.dataset.v; markDirty();
    form.querySelectorAll('[data-auth] button').forEach((x) => x.classList.toggle('on', x === b));
    form.querySelector('[data-user]').hidden = auth === 'ssh';
  }));

  wireCreds(form);
  const ep = (p) => ({ server: v(`${p}.server`).value.trim(), database: v(`${p}.database`).value.trim(), credential: v(`${p}.credential`).value });
  const ensureBoth = async () => {
    for (const p of ['src', 'tgt']) await ensureCred(form, `${p}.credential`, v(`${p}.server`).value.trim());
  };
  const gitCfg = () => ({ enabled: v('git.enabled').checked, repoUrl: v('git.repoUrl').value.trim(), branch: v('git.branch').value.trim(),
    path: v('git.path').value.trim(), auth, username: v('git.username').value.trim(), secret: v('git.secret').value.trim(),
    clearSecret: !!v('git.clearSecret')?.checked });

  form.querySelectorAll('[data-test]').forEach((b) => b.addEventListener('click', async () => {
    const out = form.querySelector(`[data-res="${b.dataset.test}"]`);
    out.className = 'test-result'; out.textContent = 'checking…';
    try { await ensureCred(form, `${b.dataset.test}.credential`, v(`${b.dataset.test}.server`).value.trim()); } catch (e) { out.className = 'test-result err'; out.textContent = e.message; return; }
    const r = await api('/api/test-connection', { method: 'POST', body: ep(b.dataset.test) });
    out.className = `test-result ${r.ok ? 'ok' : 'err'}`; out.textContent = r.message;
  }));
  form.querySelector('[data-testgit]').addEventListener('click', async () => {
    const out = form.querySelector('[data-gitres]');
    out.className = 'test-result'; out.textContent = 'checking…';
    const r = await api('/api/test-git', { method: 'POST', body: { job_id: job.id, git: gitCfg() } });
    out.className = `test-result ${r.ok ? 'ok' : 'err'}`; out.textContent = r.message;
  });
  form.querySelector('[data-revoke]')?.addEventListener('click', async () => {
    if (!confirm('Stop syncing into this database until it is approved again?')) return;
    await api(`/api/jobs/${job.id}/target/revoke`, { method: 'POST' });
    toast('Approval revoked');
    route();
  });
  form.querySelector('[data-del]').addEventListener('click', async () => {
    if (!confirm(`Delete sync “${job.name}”? Databases and the git repository will not be changed.`)) return;
    await api(`/api/jobs/${job.id}`, { method: 'DELETE' });
    dirty = false;
    location.hash = '#/';
  });
  box.querySelector('[data-cancel]').addEventListener('click', () => { dirty = false; route(); });
  box.querySelector('[data-save]').addEventListener('click', async (e) => {
    try { await ensureBoth(); } catch (err) { toast(err.message); return; }
    const body = {
      name: v('name').value.trim(), enabled: v('enabled').checked, schedule: readSchedule(form, 'sched'),
      config: { source: ep('src'), target: ep('tgt'), git: gitCfg(),
        schema: { publish: v('schema.publish').checked, dropObjectsNotInSource: v('schema.dropObjectsNotInSource').checked,
          blockOnPossibleDataLoss: v('schema.blockOnPossibleDataLoss').checked, alwaysPublish: v('schema.alwaysPublish').checked,
          excludeObjectTypes: v('schema.excludeObjectTypes').value.trim() },
        data: { tables: v('data.on').checked ? readTables() : [], mode: v('data.mode').value, chunkRows: Number(v('data.chunkRows').value) || 0,
          windowRows: Number(v('data.windowRows').value) || 0, fullCheck: v('data.fullCheck').value.trim(), verifyForeignKeys: v('data.verifyForeignKeys').checked,
          maxMbit: Number(v('data.maxMbit').value) || 0, parallelTables: Number(v('data.parallelTables').value) || 0 } },
    };
    e.target.disabled = true;
    try {
      await api(`/api/jobs/${job.id}`, { method: 'PUT', body });
      dirty = false;
      toast('Saved');
      route();
    } catch (err) {
      form.querySelector('[data-errors]').innerHTML = `<div class="errors"><ul>${err.details.map((x) => `<li>${esc(x)}</li>`).join('')}</ul></div>`;
      window.scrollTo(0, 0);
    } finally { e.target.disabled = false; }
  });

  // table picker needs the source to be reachable; otherwise a plain list
  const tb = box.querySelector('[data-tables]');
  const r = await api('/api/discover/tables', { method: 'POST', body: c.source }).catch((e) => ({ ok: false, message: e.message }));
  if (r.ok) {
    tb.innerHTML = tablePicker('s', r.tables, d.tables?.length ? d.tables : ['*']);
    readTables = wirePicker(tb, 's');
  } else {
    tb.innerHTML = `<div class="group"><div class="row-item"><div class="value"><span class="err-text small">Could not load the table list: ${esc(r.message)}</span>
      <textarea name="tables-raw" style="margin-top:8px">${esc((d.tables || []).join('\n'))}</textarea><span class="hint">one per line; * for all</span></div></div></div>`;
    readTables = () => form.elements['tables-raw'].value.split(/[\n,]/).map((x) => x.trim()).filter(Boolean);
  }
}

// ---------- runs ----------

const STATUS_TEXT = { queued: 'Queued', running: 'Running', succeeded: 'Succeeded', failed: 'Failed', cancelled: 'Stopped' };
const TRIGGER_TEXT = { schedule: 'scheduled', manual: 'manual', startup: 'on startup' };
const badge = (s) => `<span class="badge b-${s || 'none'}">${esc(STATUS_TEXT[s] || '—')}</span>`;
const isActive = (s) => s === 'queued' || s === 'running';

async function viewRunsInto(box, jobId) {
  const render = async () => {
    const runs = await api(`/api/runs?limit=200${jobId ? `&job_id=${jobId}` : ''}`);
    box.innerHTML = runs.length ? `<div class="group table-wrap"><table class="tbl"><thead><tr><th>Status</th>${jobId ? '' : '<th>Sync</th>'}<th>Started</th><th>Duration</th><th class="num">Transferred</th><th class="num">Rows</th><th>Result</th></tr></thead><tbody>
      ${runs.map((r) => `<tr class="click" data-href="#/runs/${r.id}" tabindex="0"><td>${badge(r.status)}</td>${jobId ? '' : `<td>${esc(r.job_name)}</td>`}
        <td title="${esc(fmt(r.started_at || r.queued_at))}">${esc(rel(r.started_at || r.queued_at))} <span class="muted small">${esc(TRIGGER_TEXT[r.trigger_type] || '')}</span></td>
        <td>${duration(r.started_at, r.finished_at)}</td><td class="num">${traffic(r.stats)}</td><td class="num">${num(r.stats?.rows_changed)}</td>
        <td class="small muted">${r.progress && isActive(r.status) ? `${r.progress.total ? `${r.progress.done + 1}/${r.progress.total} · ` : ''}${esc(r.progress.table || '')} ${esc(r.progress.phase)}` : esc((r.summary || '').slice(0, 120))}</td></tr>`).join('')}</tbody></table></div>`
      : '<div class="group"><div class="empty">No runs yet.</div></div>';
    box.querySelectorAll('tr[data-href]').forEach((tr) => {
      const go = () => { location.hash = tr.dataset.href; };
      tr.addEventListener('click', go);
      tr.addEventListener('keydown', (e) => { if (e.key === 'Enter') go(); });
    });
  };
  await render();
  poll(render, 5000);
}

const ACTION_TEXT = { skipped: 'skipped', unchanged: 'unchanged', changed: 'changed', reload: 'reloaded', error: 'error' };
const ACTION_CLS = { skipped: 'b-none', unchanged: 'b-none', changed: 'b-succeeded', reload: 'b-running', error: 'b-failed' };
const FILTERS = [['all', 'All'], ['changed', 'Changed'], ['reload', 'Reloaded'], ['error', 'Errors'], ['quiet', 'Unchanged']];

// log line: own timestamp shown in the viewer's time zone (old lines carried a UTC "hh:mm:ss " prefix)
function logLine(l) {
  const text = l.line.replace(/^\d{2}:\d{2}:\d{2} /, '');
  const t = new Date(l.ts).toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  const cls = /FAILED|error|Exception/i.test(text) ? 'e' : /WARN|rejected/i.test(text) ? 'w' : '';
  return `<span class="${cls}"><span class="h">${t}</span> ${esc(text)}</span>\n`;
}

async function viewRun(id) {
  let after = -1, run = await api(`/api/runs/${id}`);
  const ui = { filter: 'all', q: '', page: 0 };
  $app.innerHTML = `<a class="back" href="#/jobs/${run.job_id}/runs">‹ ${esc(run.job_name)}</a>
    <div class="page-head"><div><h1>Run ${esc(fmt(run.started_at || run.queued_at))}</h1><div class="sub" data-sub></div></div>
      <div class="actions"><button class="danger" data-cancel hidden>Stop</button></div></div>
    <div data-progress></div>
    <div class="group"><div class="metrics" data-metrics></div><div data-summary></div></div>
    <h2>Tables</h2><div data-tiles></div>
    <div class="group" data-tables-box>
      <div class="picker-tools"><div class="segmented" data-filter></div>
        <input type="search" placeholder="Find a table" data-q style="max-width:260px;margin-left:auto"></div>
      <div class="table-wrap" data-tbl></div>
      <div class="pager" data-pager></div>
    </div>
    <details class="advanced" data-logbox><summary>Log</summary>
      <label class="small" style="display:flex;gap:6px;align-items:center;margin:4px 0 8px"><input type="checkbox" data-follow checked> Auto-scroll</label>
      <pre class="log" data-log></pre></details>`;
  const $log = $app.querySelector('[data-log]');
  if (isActive(run.status)) $app.querySelector('[data-logbox]').open = false;

  const renderProgress = () => {
    const p = run.progress, box = $app.querySelector('[data-progress]');
    if (!isActive(run.status) || !p) { box.innerHTML = ''; return; }
    const pct = p.total ? Math.round((p.done / p.total) * 100) : 0;
    box.innerHTML = `<div class="group progress-card"><div class="row-item"><div class="value">
      <div class="small muted">${p.step === 'schema' ? 'Schema' : `Table ${p.done + 1} of ${p.total}`} · ${esc(rel(p.since).replace('just now', 'started just now'))}</div>
      <div><b>${esc(p.table || '')}</b>${p.table ? ' — ' : ''}${esc(p.phase)}</div>
      ${p.total ? `<div class="bar"><span style="width:${pct}%"></span></div>` : ''}</div>
      ${p.total ? `<div class="value right"><b>${pct}%</b></div>` : ''}</div></div>`;
  };

  const renderMeta = () => {
    const st = run.stats || {};
    const dur = Math.max(1, ((run.finished_at ? new Date(run.finished_at) : new Date()) - new Date(run.started_at || run.queued_at)) / 1000);
    $app.querySelector('[data-sub]').innerHTML = `${badge(run.status)} · ${esc(TRIGGER_TEXT[run.trigger_type] || run.trigger_type)}${run.worker ? ` · ${esc(run.worker)}` : ''}`;
    $app.querySelector('[data-cancel]').hidden = !isActive(run.status) || !!run.cancel_requested;
    $app.querySelector('[data-metrics]').innerHTML = `
      <div class="metric"><span>Duration</span><b>${duration(run.started_at, run.finished_at)}</b></div>
      <div class="metric"><span>Rows changed</span><b>${num(st.rows_changed)}</b><small class="muted"> in ${num(st.tables_changed)} ${plural(st.tables_changed, 'table', 'tables')}</small></div>
      <div class="metric"><span>Network in</span><b>${bytes(st.net_in)}</b><small class="muted"> ≈${mbit((st.net_in || 0) / dur)}</small></div>
      <div class="metric"><span>Network out</span><b>${bytes(st.net_out)}</b><small class="muted"> ≈${mbit((st.net_out || 0) / dur)}</small></div>
      <div class="metric"><span>Pod memory peak</span><b>${bytes(st.peak_mem)}</b></div>
      <div class="metric"><span>CPU</span><b>${cores((st.cpu_seconds || 0) / dur)}</b><small class="muted"> ${Math.round(st.cpu_seconds || 0)} CPU-seconds</small></div>`;
    $app.querySelector('[data-summary]').innerHTML = run.summary ? `<div class="row-item"><div class="value small ${run.status === 'failed' ? 'err-text' : 'muted'}">${esc(run.summary)}</div></div>` : '';
    renderProgress();
    renderTables();
  };

  const renderTables = () => {
    const all = run.tables || [];
    const box = $app.querySelector('[data-tables-box]');
    const count = (f) => all.filter((t) => f(t)).length;
    const c = { changed: count((t) => t.action === 'changed'), reload: count((t) => t.action === 'reload'), error: count((t) => t.action === 'error'),
      skipped: count((t) => t.action === 'skipped'), unchanged: count((t) => t.action === 'unchanged') };
    const ins = all.reduce((s, t) => s + (t.inserted || 0), 0), upd = all.reduce((s, t) => s + (t.updated || 0), 0), del = all.reduce((s, t) => s + (t.deleted || 0), 0);
    $app.querySelector('[data-tiles]').innerHTML = all.length ? `<div class="group"><div class="metrics">
      <div class="metric"><span>Processed</span><b>${num(all.length)}</b>${run.progress?.total ? `<small class="muted"> of ${num(run.progress.total)}</small>` : ''}</div>
      <div class="metric"><span>Skipped, not read</span><b>${num(c.skipped)}</b></div>
      <div class="metric"><span>Verified, unchanged</span><b>${num(c.unchanged)}</b></div>
      <div class="metric"><span>Changed</span><b class="${c.changed ? 'ok-text' : ''}">${num(c.changed)}</b><small class="muted"> +${num(ins)} ~${num(upd)} −${num(del)}</small></div>
      <div class="metric"><span>Reloaded</span><b>${num(c.reload)}</b></div>
      <div class="metric"><span>Errors</span><b class="${c.error ? 'err-text' : ''}">${num(c.error)}</b></div>
    </div></div>` : `<div class="group"><div class="empty small">${isActive(run.status) ? 'No tables processed yet. Preparing…' : 'This run did not sync table data.'}</div></div>`;
    box.hidden = !all.length;
    if (!all.length) return;

    $app.querySelector('[data-filter]').innerHTML = FILTERS.map(([k, t]) => {
      const n = k === 'all' ? all.length : k === 'quiet' ? c.skipped + c.unchanged : c[k];
      return `<button type="button" data-f="${k}" class="${ui.filter === k ? 'on' : ''}">${t} <span class="muted">${n}</span></button>`;
    }).join('');
    const match = (t) => (ui.filter === 'all' || (ui.filter === 'quiet' ? (t.action === 'skipped' || t.action === 'unchanged') : t.action === ui.filter))
      && (!ui.q || t.table.toLowerCase().includes(ui.q));
    const rows = all.filter(match);
    const pages = Math.max(1, Math.ceil(rows.length / 10));
    ui.page = Math.min(ui.page, pages - 1);
    const pageRows = rows.slice(ui.page * 10, ui.page * 10 + 10);
    $app.querySelector('[data-tbl]').innerHTML = rows.length ? `<table class="tbl"><thead><tr><th>Table</th><th>Result</th><th class="num">Rows checked</th>
      <th class="num">+ inserted</th><th class="num">~ updated</th><th class="num">− deleted</th><th class="num">Traffic</th><th class="num">Time</th></tr></thead><tbody>
      ${pageRows.map((t) => `<tr><td>${esc(t.table)}${t.note && t.action !== 'skipped' ? `<div class="small muted">${esc(t.note)}</div>` : ''}${t.error ? `<div class="small err-text">${esc(t.error)}</div>` : ''}</td>
        <td><span class="badge ${ACTION_CLS[t.action] || 'b-none'}">${ACTION_TEXT[t.action] || t.action}</span></td>
        <td class="num">${t.action === 'skipped' ? '—' : num(t.rows_checked)}</td>
        <td class="num ${t.inserted ? 'ok-text' : 'muted'}">${t.inserted ? '+' + num(t.inserted) : '—'}</td>
        <td class="num ${t.updated ? 'warn-text' : 'muted'}">${t.updated ? '~' + num(t.updated) : '—'}</td>
        <td class="num ${t.deleted ? 'err-text' : 'muted'}">${t.deleted ? '−' + num(t.deleted) : '—'}</td>
        <td class="num">${bytes(t.bytes)}</td><td class="num">${t.ms < 1000 ? `${t.ms} ms` : secs(t.ms / 1000)}</td></tr>`).join('')}
      </tbody></table>` : '<div class="empty small">No tables match this filter.</div>';
    $app.querySelector('[data-pager]').innerHTML = pages > 1 ? `<button type="button" class="sm" data-p="${ui.page - 1}" ${ui.page === 0 ? 'disabled' : ''}>‹ Previous</button>
      <span class="small muted">page ${ui.page + 1} of ${pages} · ${rows.length} ${plural(rows.length, 'table', 'tables')}</span>
      <button type="button" class="sm" data-p="${ui.page + 1}" ${ui.page >= pages - 1 ? 'disabled' : ''}>Next ›</button>` : '';
  };

  $app.querySelector('[data-tables-box]').addEventListener('click', (e) => {
    const f = e.target.closest('[data-f]'), p = e.target.closest('[data-p]');
    if (f) { ui.filter = f.dataset.f; ui.page = 0; renderTables(); }
    if (p && !p.disabled) { ui.page = Number(p.dataset.p); renderTables(); }
  });
  $app.querySelector('[data-q]').addEventListener('input', (e) => { ui.q = e.target.value.trim().toLowerCase(); ui.page = 0; renderTables(); });

  const tick = async () => {
    const lines = await api(`/api/runs/${id}/logs?after=${after}`);
    if (lines.length) {
      after = lines[lines.length - 1].seq;
      $log.insertAdjacentHTML('beforeend', lines.map(logLine).join(''));
      if ($app.querySelector('[data-follow]').checked) $log.scrollTop = $log.scrollHeight;
    }
    run = await api(`/api/runs/${id}`);
    renderMeta();
    if (!isActive(run.status) && !lines.length) clearInterval(timer);
  };
  $app.querySelector('[data-cancel]').addEventListener('click', async () => {
    if (!confirm('Stop this run? An interrupted reload resumes on the next run.')) return;
    await api(`/api/runs/${id}/cancel`, { method: 'POST' });
    toast('Stopping…');
  });
  renderMeta();
  await tick();
  poll(tick, 2000);
}

// ---------- schema history ----------

const CHANGE_TEXT = { A: 'added', M: 'modified', D: 'deleted', R: 'renamed' };
const KIND_TEXT = { Tables: 'Tables', Views: 'Views', StoredProcedures: 'Procedures', Functions: 'Functions',
  Security: 'Permissions and roles', Sequences: 'Sequences', Synonyms: 'Synonyms', Other: 'Other' };

function renderPatch(patch) {
  return patch.split('\n').map((l) => {
    const cls = /^(\+\+\+|---|diff |index )/.test(l) ? 'h' : l.startsWith('@@') ? 'w' : l.startsWith('+') ? 'add' : l.startsWith('-') ? 'e' : '';
    return `<span class="${cls}">${esc(l)}</span>`;
  }).join('\n');
}

async function viewSchemaInto(box, jobId) {
  const list = await api(`/api/schema-changes${jobId ? `?job_id=${jobId}` : ''}`);
  if (!list.length) {
    box.innerHTML = '<div class="group"><div class="empty"><h3>No schema changes yet</h3><p>History appears after the first run with schema history in git turned on.</p></div></div>';
    return;
  }
  box.innerHTML = list.map((c) => {
    const byKind = {};
    (c.changes || []).forEach((x) => (byKind[x.kind] = byKind[x.kind] || []).push(x));
    const total = c.added + c.modified + c.deleted;
    const counts = [c.added && `+${c.added}`, c.modified && `~${c.modified}`, c.deleted && `−${c.deleted}`].filter(Boolean).join(' ');
    return `<div class="group" style="margin-bottom:12px">
      <div class="row-item"><div class="value"><b>${c.baseline ? 'Initial schema snapshot' : `${total} ${plural(total, 'change', 'changes')}`}</b>
        <div class="muted small">${esc(fmt(c.at))}${jobId ? '' : ` · ${esc(c.job_name)}`} · commit <code>${esc(c.commit.slice(0, 10))}</code> · <a href="#/runs/${c.run_id}">run</a></div></div>
        <div class="value right small">${esc(counts)}</div></div>
      ${c.baseline ? `<div class="row-item"><div class="value small muted">${c.added} objects exported to git</div></div>` : `
        ${Object.entries(byKind).map(([k, items]) => `<div class="row-item" style="align-items:flex-start"><div class="label">${esc(KIND_TEXT[k] || k)}</div><div class="value">
          ${items.map((x) => `<div><span class="${x.status === 'A' ? 'ok-text' : x.status === 'D' ? 'err-text' : 'warn-text'}">${CHANGE_TEXT[x.status] || x.status}</span> ${esc(x.object)}</div>`).join('')}</div></div>`).join('')}
        <div class="row-item"><div class="value"><button type="button" class="link" data-diff="${c.id}">Show code changes</button></div></div>
        <div style="padding:0 16px 16px" hidden data-diffbox="${c.id}"><pre class="log"></pre></div>`}
    </div>`;
  }).join('');
  box.querySelectorAll('[data-diff]').forEach((b) => b.addEventListener('click', async () => {
    const wrap = box.querySelector(`[data-diffbox="${b.dataset.diff}"]`);
    if (!wrap.hidden) { wrap.hidden = true; b.textContent = 'Show code changes'; return; }
    const pre = wrap.querySelector('pre');
    if (!pre.dataset.loaded) {
      const c = await api(`/api/schema-changes/${b.dataset.diff}`);
      pre.innerHTML = renderPatch(c.patch || '(empty)') + (c.patch_truncated ? '\n… truncated; see git for the full diff' : '');
      pre.dataset.loaded = '1';
    }
    wrap.hidden = false;
    b.textContent = 'Hide changes';
  }));
}

// ---------- resources & sizing ----------

const rate = (bps) => `${bytes(Math.round(bps || 0))}/s`;
const mbit = (bps) => `${((bps || 0) * 8 / 1e6).toFixed((bps || 0) * 8 / 1e6 < 10 ? 2 : 0)} Mbit/s`;
const cores = (c) => `${(c || 0).toFixed(c < 1 ? 2 : 1)} ${plural(Math.ceil(c || 0) || 1, 'core', 'cores')}`;
const secs = (s) => (s >= 3600 ? `${(s / 3600).toFixed(1)} h` : s >= 60 ? `${Math.round(s / 60)} min` : `${Math.round(s)} s`);

function spark(values, cls = '') {
  if (!values.length) return '<svg class="spark" viewBox="0 0 300 48"></svg>';
  const max = Math.max(...values, 1e-9);
  const pts = values.map((v, i) => `${(i / Math.max(1, values.length - 1)) * 300},${46 - (v / max) * 42}`).join(' ');
  return `<svg class="spark ${cls}" viewBox="0 0 300 48" preserveAspectRatio="none" aria-hidden="true">
    <polyline points="0,48 ${pts} 300,48" class="fill"/><polyline points="${pts}" class="line"/></svg>`;
}

async function viewResources() {
  const draw = async () => {
    const [r, sz] = await Promise.all([api('/api/resources'), api('/api/sizing')]);
    const h = r.history, c = r.current || {};
    const lim = r.mem_limit && r.mem_limit < 2 ** 60 ? ` of ${bytes(r.mem_limit)}` : '';
    const anyRun = sz.jobs.some((j) => j.in_progress);
    const hmax = (k) => h.reduce((m, x) => Math.max(m, x[k] || 0), 0);
    const podLim = status?.pod_max_mbit || (await loadStatus()).pod_max_mbit;
    $app.innerHTML = `<div class="page-head"><div><h1>Resources</h1><div class="sub">pod ${esc(r.pod)} · last 10 minutes, updated every 2 seconds</div></div></div>
      <div class="group"><div class="metrics res-metrics">
        <div class="metric"><span>Memory</span><b>${bytes(c.mem)}</b><small class="muted">${lim}</small>${spark(h.map((x) => x.mem))}
          <div class="peak">10-min peak: <b>${bytes(hmax('mem'))}</b>${r.mem_limit && r.mem_limit < 2 ** 60 ? ` · ${Math.round(hmax('mem') / r.mem_limit * 100)}% of limit` : ''}</div></div>
        <div class="metric"><span>CPU</span><b>${cores(c.cpu)}</b><small class="muted">${r.cpu_limit ? ` of ${r.cpu_limit}` : ''}</small>${spark(h.map((x) => x.cpu))}
          <div class="peak">10-min peak: <b>${cores(hmax('cpu'))}</b>${r.cpu_limit ? ` · ${Math.round(hmax('cpu') / r.cpu_limit * 100)}% of limit` : ''}</div></div>
        <div class="metric"><span>Inbound traffic</span><b>${rate(c.in_bps)}</b><small class="muted"> ${mbit(c.in_bps)}</small>${spark(h.map((x) => x.in_bps), 'in')}
          <div class="peak">10-min peak: <b>${mbit(hmax('in_bps'))}</b>${podLim ? ` · pod limit ${podLim} Mbit/s` : ''}</div></div>
        <div class="metric"><span>Outbound traffic</span><b>${rate(c.out_bps)}</b><small class="muted"> ${mbit(c.out_bps)}</small>${spark(h.map((x) => x.out_bps), 'out')}
          <div class="peak">10-min peak: <b>${mbit(hmax('out_bps'))}</b>${podLim ? ` · pod limit ${podLim} Mbit/s` : ''}</div></div>
      </div></div>
      <p class="hint">Memory and CPU cover the whole container, including SqlPackage and git. Traffic covers all of the pod’s network interfaces: SQL Server, git, MongoDB.</p>

      <h2>Recommended sizing</h2>
      <div class="callout ok"><div class="body"><b>Recommended: ${sz.replicas} ${plural(sz.replicas, 'pod', 'pods')} × ${sz.max_parallel} concurrent ${plural(sz.max_parallel, 'sync', 'syncs')}</b>
        <div class="small">Per pod: memory ${bytes(sz.mem_request)} (limit ${bytes(sz.mem_limit)}), CPU ${sz.cpu_request} (limit ${sz.cpu_limit}).
          Bandwidth: ${mbit(sz.net_avg_bps)} on average, up to ${mbit(sz.net_peak_bps)} at peak.${anyRun ? ' Includes the run in progress.' : ''}</div>
        <div class="acts"><button class="sm" data-copy>Copy Deployment YAML</button></div></div></div>
      <div class="group table-wrap"><table class="tbl"><thead><tr><th>Database</th><th class="num">Run (p95)</th><th class="num">Interval</th><th class="num">Worker load</th>
        <th class="num">Memory peak</th><th class="num">CPU</th><th class="num">Per run ↓ / ↑</th><th class="num">Avg bandwidth</th></tr></thead><tbody>
        ${sz.jobs.map((j) => `<tr><td><a href="#/jobs/${j.job_id}">${esc(j.name)}</a>${j.in_progress ? ' <span class="badge b-running">running</span>' : ''}
            ${j.warning ? `<div class="small warn-text">${esc(j.warning)}</div>` : ''}${!j.runs ? '<div class="small muted">no runs yet</div>' : ''}</td>
          <td class="num">${j.runs ? secs(j.dur_p95_s) : '—'}</td><td class="num">${j.interval_s ? secs(j.interval_s) : 'manual'}</td>
          <td class="num ${j.duty >= 1 ? 'err-text' : j.duty > 0.7 ? 'warn-text' : ''}">${j.interval_s && j.runs ? `${Math.round(j.duty * 100)}%` : '—'}</td>
          <td class="num">${j.runs ? bytes(j.mem_peak) : '—'}</td><td class="num">${j.runs ? cores(j.cpu_p95) : '—'}</td>
          <td class="num">${j.runs ? `${bytes(j.in_per_run)} / ${bytes(j.out_per_run)}` : '—'}</td><td class="num">${j.runs && j.interval_s ? mbit(j.net_avg_bps) : '—'}</td></tr>`).join('')}
      </tbody></table></div>
      ${sz.notes.map((n) => `<p class="hint">${esc(n)}</p>`).join('')}
      <details class="advanced"><summary>YAML</summary><pre class="log" style="min-height:0">${esc(sz.yaml)}</pre></details>
      <p class="hint"><a href="#/env">Service environment →</a></p>`;
    $app.querySelector('[data-copy]').addEventListener('click', async () => {
      try { await navigator.clipboard.writeText(sz.yaml); toast('YAML copied'); } catch { toast('Could not copy. Expand the YAML below.'); }
    });
  };
  await draw();
  poll(draw, 4000);
}

// ---------- environment ----------

async function viewEnv() {
  const s = await loadStatus();
  const row = (k, v, hint) => `<div class="row-item"><div class="label"><code>${k}</code><small>${hint}</small></div><div class="value">${v}</div></div>`;
  $app.innerHTML = `<a class="back" href="#/settings">‹ Resources</a><div class="page-head"><div><h1>Environment</h1><div class="sub">Service settings come from Deployment environment variables · pod ${esc(s.pod)}</div></div></div>
    <h2>Now</h2><div class="group"><div class="metrics">
      <div class="metric"><span>Running</span><b>${s.stats.running}</b></div><div class="metric"><span>Queued</span><b>${s.stats.queued}</b></div>
      <div class="metric"><span>Succeeded (24h)</span><b>${s.stats.succeeded_24h}</b></div><div class="metric"><span>Failed (24h)</span><b class="${s.stats.failed_24h ? 'err-text' : ''}">${s.stats.failed_24h}</b></div>
    </div></div>
    <h2>Variables</h2><div class="group">
      ${row('SYNC_SCHEDULE', `<code>${esc(s.default_schedule || '—')}</code>`, 'default schedule')}
      ${row('SYNC_TIMEZONE', esc(s.timezone), 'time zone for schedules')}
      ${row('MAX_PARALLEL', s.max_parallel, 'concurrent syncs per pod')}
      ${row('SCAN_MAXDOP', s.scan_maxdop, 'SQL Server cores per scan')}
      ${row('BATCH_TARGET_SECONDS', s.batch_target_seconds, 'target duration of one statement; batch sizes adapt to it')}
      ${row('GIT_REPO_URL', esc(s.git_repo || '—'), 'default shared repository')}
      ${row('SECRET_KEY', s.secret_key_set ? 'set' : '<span class="err-text">not set</span>', 'encrypts git keys entered in the UI')}
    </div>
    <h2>SQL credentials</h2><div class="group">
      ${(s.credential_list || []).map((c) => `<div class="row-item"><div class="label">${esc(c.name)}<small>${c.source === 'env' ? 'from Kubernetes Secret (CRED_*)' : 'entered in the UI, password encrypted'}</small></div>
        <div class="value">${esc(c.user)}</div>${c.source === 'ui' ? `<div class="value right"><button class="danger sm" data-delcred="${esc(c.name)}">Delete</button></div>` : ''}</div>`).join('') || '<div class="row-item muted">none</div>'}
    </div>`;
  $app.querySelectorAll('[data-delcred]').forEach((b) => b.addEventListener('click', async () => {
    if (!confirm(`Delete credentials ${b.dataset.delcred}?`)) return;
    try { await api(`/api/credentials/${encodeURIComponent(b.dataset.delcred)}`, { method: 'DELETE' }); toast('Deleted'); viewEnv(); } catch (e) { toast(e.message); }
  }));
}

// ---------- router ----------

let lastHash = location.hash;
async function route() {
  clearInterval(timer);
  leaveGuard = null;
  lastHash = location.hash;
  const h = location.hash.replace(/^#/, '') || '/';
  document.querySelectorAll('.top nav a').forEach((a) => {
    const p = a.getAttribute('href').slice(1);
    a.classList.toggle('active', p === '/' ? h === '/' || h.startsWith('/jobs') || h === '/new' : h.startsWith(p));
  });
  let m;
  try {
    if (h === '/') await viewHome();
    else if (h === '/new') await viewWizard();
    else if ((m = h.match(/^\/jobs\/(\d+)(?:\/(runs|schema|settings))?$/))) await viewJob(Number(m[1]), m[2]);
    else if (h === '/runs') {
      $app.innerHTML = '<div class="page-head"><div><h1>Run history</h1><div class="sub">all syncs, newest first</div></div></div><div data-body></div>';
      await viewRunsInto($app.querySelector('[data-body]'), null);
    } else if ((m = h.match(/^\/runs\/(\d+)$/))) await viewRun(Number(m[1]));
    else if (h === '/schema') {
      $app.innerHTML = '<div class="page-head"><div><h1>Schema changes</h1><div class="sub">all syncs, newest first</div></div></div><div data-body></div>';
      await viewSchemaInto($app.querySelector('[data-body]'), null);
    } else if (h === '/settings') await viewResources();
    else if (h === '/env') await viewEnv();
    else $app.innerHTML = '<div class="empty"><h3>Page not found</h3><a href="#/">Back to syncs</a></div>';
  } catch (e) {
    $app.innerHTML = `<div class="callout err"><div class="body"><b>Could not load</b><div class="small">${esc(e.message)}</div></div></div>`;
  }
}

window.addEventListener('hashchange', () => {
  const msg = leaveGuard?.();
  if (msg && location.hash !== lastHash && !confirm(msg)) { history.replaceState(null, '', lastHash); return; }
  route();
});
window.addEventListener('beforeunload', (e) => { if (leaveGuard?.()) e.preventDefault(); });
route();

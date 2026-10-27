// Relay live dashboard. The server streams one snapshot per tick over SSE;
// this file renders it. No framework: cards that hold buttons are created
// once per key and updated in place, so a click is never lost to a re-render.
'use strict';

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

// h builds an element. Text always goes in through textContent: ids,
// errors and job payloads come from the cluster and are untrusted.
function h(tag, attrs = {}, ...kids) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null || v === false) continue;
    if (k === 'class') el.className = v;
    else if (k === 'style') el.style.cssText = v;
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v === true ? '' : v);
  }
  for (const kid of kids.flat()) {
    if (kid == null || kid === false) continue;
    el.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
  }
  return el;
}

const S = {
  snap: null,
  series: [],
  feed: [],
  skew: 0, // server clock minus browser clock
  connected: false,
  tlWindow: 30,
};
const now = () => Date.now() + S.skew;
const css = name => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

// Job states: color token, label, and what the state means.
const STATES = {
  PENDING: { c: '--st-pending', label: 'pending' },
  LEASED: { c: '--st-leased', label: 'leased' },
  SUCCEEDED: { c: '--st-ok', label: 'succeeded' },
  FAILED: { c: '--st-failed', label: 'failed' },
  DEAD: { c: '--st-dead', label: 'dead-lettered' },
  RETRY: { c: '--st-retry', label: 'retry' },
  REQUEUED: { c: '--st-requeue', label: 'requeued' },
  RELEASED: { c: '--st-idle', label: 'released' },
  SUPERSEDED: { c: '--st-idle', label: 'superseded' },
  SUBMITTED: { c: '--muted', label: 'submitted' },
};
const stColor = k => css((STATES[k] || {}).c || '--muted');

// ---------- formatting ----------
const fmtInt = n => n == null ? '–' : Math.round(n).toLocaleString();
function fmtRate(r) {
  if (r == null) return '–';
  if (r === 0) return '0';
  if (r < 10) return r.toFixed(1);
  return Math.round(r).toLocaleString();
}
function fmtMs(ms) {
  if (ms == null) return '–';
  if (ms < 1) return ms.toFixed(2) + ' ms';
  if (ms < 10) return ms.toFixed(1) + ' ms';
  if (ms < 1000) return Math.round(ms) + ' ms';
  return (ms / 1000).toFixed(ms < 10000 ? 2 : 1) + ' s';
}
function fmtAge(ms) {
  if (ms < 0) ms = 0;
  if (ms < 1000) return ms.toFixed(0) + 'ms';
  if (ms < 60000) return (ms / 1000).toFixed(1) + 's';
  return Math.floor(ms / 60000) + 'm' + String(Math.floor(ms / 1000) % 60).padStart(2, '0') + 's';
}
const clock = t => new Date(t).toLocaleTimeString([], { hour12: false });
// Worker ids are w-<hostname>-<random>; every incarnation of a host shares
// the prefix, so a worker is named by its random suffix. Job ids are
// time-ordered UUIDs, so a job is named by its last digits.
const short = id => !id ? '' : id.startsWith('w-') ? id.slice(id.lastIndexOf('-') + 1)
  : id.includes('-') ? id.slice(-8) : id.slice(0, 8);

// ---------- server calls ----------
async function post(path, body) {
  const res = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || res.statusText);
  return data;
}
function toast(msg, bad = false) {
  const t = h('div', { class: 'toast' + (bad ? ' bad' : '') }, msg);
  $('#toasts').append(t);
  setTimeout(() => t.remove(), bad ? 6000 : 3000);
}
async function act(path, body, okMsg) {
  try {
    const r = await post(path, body);
    if (okMsg) toast(okMsg);
    return r;
  } catch (e) {
    toast(e.message, true);
    throw e;
  }
}

// ---------- stream ----------
function connect() {
  const es = new EventSource('/api/stream');
  es.addEventListener('init', e => {
    const m = JSON.parse(e.data);
    S.connected = true;
    S.series = m.series || [];
    S.feed = m.feed || [];
    S.feedSeq = S.feed.length ? S.feed[S.feed.length - 1].seq : 0;
    $('#feed').replaceChildren();
    accept(m.snapshot);
    renderFeed(S.feed, false);
  });
  es.addEventListener('tick', e => {
    const s = JSON.parse(e.data);
    if (s.point) {
      S.series.push(s.point);
      if (S.series.length > 600) S.series.splice(0, S.series.length - 600);
    }
    // An item added between a tick and this browser's init arrives in both.
    const fresh = (s.feed || []).filter(f => f.seq > S.feedSeq);
    if (fresh.length) {
      S.feedSeq = fresh[fresh.length - 1].seq;
      S.feed.push(...fresh);
      if (S.feed.length > 300) S.feed.splice(0, S.feed.length - 300);
      renderFeed(fresh, true);
    }
    accept(s);
  });
  es.onerror = () => { S.connected = false; renderChips(); };
}

function accept(snap) {
  S.snap = snap;
  S.skew = snap.now - Date.now();
  render();
}

function render() {
  const s = S.snap;
  if (!s) return;
  renderChips();
  renderKpis();
  renderTopology();
  renderFlow();
  renderJobs();
  renderCharts();
  renderControls();
  $('#tl-sampled').textContent = s.jobs && s.jobs.untracked
    ? `${fmtInt(s.jobs.followed)} followed; ${fmtInt(s.jobs.untracked)} more were submitted but not followed, to keep polling light`
    : 'all of them';
}

// ---------- header ----------
function chip(label, value, cls = '', icon = '') {
  return h('span', { class: 'chip ' + cls }, icon ? h('span', { class: 'ico' }, icon) : null, label, value != null ? h('b', {}, value) : null);
}
function renderChips() {
  const s = S.snap;
  const el = $('#chips');
  const kids = [];
  if (!S.connected) kids.push(chip('dashboard', 'reconnecting…', 'b-warn', '!'));
  if (s) {
    if (s.busy) kids.push(chip('cluster', s.busy + '…', '', '◌'));
    else if (s.phase === 'error') kids.push(chip('last operation', 'failed', '', '✕'));
    if (s.backend) {
      kids.push(chip('backend', s.backend === 'raft' ? 'Raft (3 coordinators)' : s.backend === 'redis' ? 'Redis' : 'external'));
      if (s.backend !== 'redis') {
        kids.push(s.leader ? chip('leader', `${s.leader} · term ${s.term}`, '', '★') : chip('leader', 'none (election)', '', '!'));
      }
      if (s.config) kids.push(chip('dead after', fmtAge(s.dead_after_ms)));
    } else if (!s.busy) {
      kids.push(chip('cluster', 'none'));
    }
    if (s.load && s.load.running) kids.push(chip('load', fmtRate(s.load.config.rate) + '/s', '', '▶'));
    if (s.scenario && s.scenario.running) kids.push(chip('scenario', s.scenario.title, '', '▶'));
  }
  el.replaceChildren(...kids);
}

// ---------- KPIs ----------
function kpi(label, value, unit, foot, color) {
  return h('div', { class: 'kpi' },
    h('div', { class: 'label' }, color ? h('i', { class: 'key', style: `background:var(${color})` }) : null, label),
    h('div', { class: 'value' }, value, unit ? h('small', {}, unit) : null),
    h('div', { class: 'foot', title: foot }, foot));
}
function renderKpis() {
  const s = S.snap;
  const r = s.rates || {}, t = s.totals || {}, g = s.gauges || {}, lat = (s.latency || {}).dispatch || {};
  const alive = (s.workers || []).filter(w => w.state === 'ALIVE');
  const slots = alive.reduce((a, w) => a + w.concurrency, 0);
  $('#kpis').replaceChildren(
    kpi('Completed', fmtRate(r.succeeded), '/s', `${fmtInt(t.succeeded)} since watching`, '--st-ok'),
    kpi('Waiting', fmtInt(g.pending), '', 'PENDING, incl. retry backoff', '--st-pending'),
    kpi('In flight', fmtInt(g.leased), slots ? `/ ${slots}` : '', 'leased jobs / live slots', '--st-leased'),
    kpi('Dispatch p99', lat.p99 != null ? fmtMs(lat.p99) : '–', '', lat.count ? `p50 ${fmtMs(lat.p50)} · ${fmtInt(lat.count)} hand-offs in 10s` : 'no hand-offs in the last 10s'),
    kpi('Workers alive', `${alive.length}`, `/ ${(s.workers || []).length}`, `${fmtInt(t.workers_expired)} declared dead`),
    kpi('Fenced', fmtInt(t.stale), '', 'stale-token reports rejected', '--st-failed'),
    kpi('Requeued', fmtInt(t.requeued), '', `${fmtInt(t.expired)} by lease expiry`, '--st-requeue'),
    kpi('Retries', fmtInt(t.retried), '', `${fmtInt(t.dead)} dead-lettered`, '--st-retry'),
  );
}

// ---------- topology ----------
// keyed reuses one element per key so buttons inside survive re-renders.
function keyed(container, items, keyOf, create, update) {
  const existing = new Map($$(':scope > [data-key]', container).map(el => [el.dataset.key, el]));
  let prev = null;
  for (const item of items) {
    const k = keyOf(item);
    let el = existing.get(k);
    if (!el) { el = create(item); el.dataset.key = k; }
    existing.delete(k);
    update(el, item);
    const want = prev ? prev.nextSibling : container.firstChild;
    if (want !== el) container.insertBefore(el, want);
    prev = el;
  }
  for (const el of existing.values()) el.remove();
}

function containerBadge(c) {
  if (!c) return null;
  if (c.isolated) return h('span', { class: 'badge b-bad' }, 'isolated');
  const cls = { running: 'b-good', paused: 'b-warn', exited: 'b-bad', restarting: 'b-warn', created: 'b-warn' }[c.state] || '';
  return h('span', { class: 'badge ' + cls }, c.state === 'running' ? 'container up' : 'container ' + c.state);
}

// faultButtons sets a card's buttons for its container's state: each
// fault offers its own undo once applied.
function faultButtons(el, c, enabled) {
  const box = $('.actions', el);
  if (!c || !enabled) { box.replaceChildren(); return; }
  const want = [];
  if (c.state === 'exited' || c.state === 'created') want.push(['start', 'Start']);
  else {
    want.push(c.state === 'paused' ? ['resume', 'Resume'] : ['pause', 'Pause']);
    want.push(['kill', 'Kill']);
    want.push(c.isolated ? ['reconnect', 'Reconnect'] : ['isolate', 'Isolate']);
  }
  const sig = want.map(w => w[0]).join(',') + c.id;
  if (box.dataset.sig === sig) return;
  box.dataset.sig = sig;
  box.replaceChildren(...want.map(([a, label]) => h('button', {
    class: 'btn', 'data-action': a, 'data-target': c.id,
    title: FAULT_HELP[a],
  }, label)));
}
const FAULT_HELP = {
  pause: 'Freeze the process (docker pause): it keeps its state but sends nothing. Becomes a zombie once declared dead.',
  resume: 'Unfreeze the process.',
  kill: 'Kill the container (docker kill).',
  start: 'Start the container again.',
  isolate: 'Disconnect the container from the network: a partition.',
  reconnect: 'Reconnect it to the network.',
};
document.addEventListener('click', async e => {
  const b = e.target.closest('button[data-action]');
  if (!b) return;
  b.disabled = true;
  try { await act('/api/fault', { action: b.dataset.action, target: b.dataset.target }); } catch {}
  b.disabled = false;
});

function renderTopology() {
  const s = S.snap;
  const watching = !!s.backend;
  $('#empty').hidden = watching || !!s.busy;
  $('#empty-text').textContent = s.docker
    ? 'Launch one with your own settings, or start one with make up and the dashboard will attach.'
    : 'Docker is not available to the dashboard, so it cannot launch a cluster. Start one yourself and restart relay-dash with --coordinator.';
  $('#topology-hint').textContent = !watching ? '' : s.backend === 'raft'
    ? 'Three coordinators replicate every transition with Raft; only the leader serves. Followers redirect clients to it.'
    : s.backend === 'redis' ? 'One coordinator; every transition is an atomic Lua script on Redis.' : 'An external cluster (no docker controls).';

  const cards = [...(s.nodes || [])];
  if (s.store) cards.push({ store: true, service: s.store.service, container: s.store });
  keyed($('#coords'), cards, n => n.service, () => h('div', { class: 'card' },
    h('div', { class: 'card-head' }, h('span', { class: 'card-name' }), h('span', { class: 'role' }), h('span', { class: 'cbadge' })),
    h('div', { class: 'card-meta' }), h('div', { class: 'actions' })), (el, n) => {
    const isLeader = !n.store && n.reachable && (n.state === 'Leader' || !n.replicated);
    el.classList.toggle('leader', isLeader && n.replicated);
    el.classList.toggle('down', !n.store && !n.reachable);
    $('.card-name', el).textContent = n.store ? 'redis' : (n.id || n.service);
    let role;
    if (n.store) role = h('span', { class: 'badge plain' }, 'store');
    else if (!n.reachable) role = h('span', { class: 'badge b-bad' }, 'unreachable');
    else if (!n.replicated) role = h('span', { class: 'badge b-good' }, 'serving');
    else if (n.state === 'Leader') role = h('span', { class: 'badge b-accent' }, '★ leader');
    else role = h('span', { class: 'badge ' + (n.state === 'Follower' ? 'b-good' : 'b-warn') }, n.state.toLowerCase());
    $('.role', el).replaceChildren(role);
    $('.cbadge', el).replaceChildren(containerBadge(n.container) || '');
    const meta = $('.card-meta', el);
    if (n.store) meta.replaceChildren('AOF, fsync every second. Holds every job, lease and worker.');
    else if (!n.reachable) meta.replaceChildren(n.grpc);
    else if (!n.replicated) meta.replaceChildren(n.grpc);
    else {
      const leader = (s.nodes || []).find(x => x.state === 'Leader' && x.reachable);
      const lag = leader && n !== leader ? Math.max(0, leader.commit_index - n.applied_index) : 0;
      meta.replaceChildren(`term ${n.term} · commit `, h('span', { class: 'mono' }, fmtInt(n.commit_index)),
        h('br'), n.state === 'Leader' ? 'serving all clients' : (lag ? `${fmtInt(lag)} entries behind` : 'caught up'),
        n.state !== 'Leader' && n.leader_id ? ` · follows ${n.leader_id}` : '');
    }
    faultButtons(el, n.container, s.docker);
  });

  const ws = s.workers || [];
  const alive = ws.filter(w => w.state === 'ALIVE').length;
  $('#workers-hint').textContent = !watching ? '' : `${alive} alive of ${ws.length}. A worker is declared dead after ${fmtAge(s.dead_after_ms || 3000)} without a heartbeat.`;
  keyed($('#workers'), ws, w => w.host, () => h('div', { class: 'card' },
    h('div', { class: 'card-head' }, h('span', { class: 'card-name' }), h('span', { class: 'role' }), h('span', { class: 'cbadge' })),
    h('div', { class: 'slots' }),
    h('div', { class: 'hb' }, h('div', { class: 'hb-bar' }, h('i')), h('span', { class: 'hb-label' })),
    h('div', { class: 'prev' }), h('div', { class: 'actions' })), (el, w) => {
    el.classList.toggle('down', w.state !== 'ALIVE');
    $('.card-name', el).textContent = w.id ? short(w.id) : w.host;
    $('.card-name', el).title = w.id ? `worker ${w.id}\nhost ${w.host}` : `host ${w.host}`;
    const role = w.state === 'ALIVE' ? h('span', { class: 'badge b-good' }, 'alive')
      : w.state === 'DEAD' ? h('span', { class: 'badge b-bad' }, 'declared dead')
        : h('span', { class: 'badge b-warn' }, 'registering');
    $('.role', el).replaceChildren(role);
    $('.cbadge', el).replaceChildren(containerBadge(w.container) || '');
    el.dataset.hb = w.state === 'ALIVE' ? w.last_heartbeat : '';
    // Slots: one square per slot, filled for each lease the store says it
    // holds; jobs the dashboard follows are named and clickable.
    const n = Math.max(w.concurrency, w.active);
    const held = w.state === 'ALIVE' ? w.held : [];
    const sq = [];
    for (let i = 0; i < n; i++) {
      const j = held[i];
      if (j) sq.push(h('span', { class: 'slot busy', 'data-job': j.job, title: `${j.type} ${short(j.job)} · token ${j.token}` }));
      else if (w.state === 'ALIVE' && i < w.active) sq.push(h('span', { class: 'slot busy untracked', title: 'a lease on a job the dashboard does not follow' }));
      else sq.push(h('span', { class: 'slot' }));
    }
    $('.slots', el).replaceChildren(...sq);
    $('.prev', el).textContent = w.previous.length
      ? `previously ${w.previous.map(short).join(', ')} (declared dead, then re-registered)` : '';
    faultButtons(el, w.container, s.docker);
  });
}

// Heartbeat bars run on animation frames so they fill smoothly toward the
// failure detector's deadline between ticks.
function animateHeartbeats() {
  const deadAfter = (S.snap && S.snap.dead_after_ms) || 3000;
  for (const el of $$('#workers > .card')) {
    const bar = $('.hb-bar', el), label = $('.hb-label', el), fill = $('i', bar);
    const last = Number(el.dataset.hb);
    if (!el.dataset.hb || !last) {
      fill.style.width = '0';
      bar.className = 'hb-bar';
      label.textContent = el.classList.contains('down') ? 'no heartbeats' : '';
      continue;
    }
    const age = Math.max(0, now() - last);
    const f = Math.min(1, age / deadAfter);
    fill.style.width = (f * 100).toFixed(1) + '%';
    bar.className = 'hb-bar' + (f >= 1 ? ' bad' : f > 0.5 ? ' warn' : '');
    label.textContent = f >= 1 ? `silent ${fmtAge(age)}` : `beat ${fmtAge(age)} ago`;
    bar.title = `last heartbeat ${fmtAge(age)} ago; declared dead at ${fmtAge(deadAfter)}`;
  }
}

$('#workers').addEventListener('pointerdown', e => {
  const sl = e.target.closest('[data-job]');
  if (sl) openJob(sl.dataset.job);
});

// ---------- flow diagram ----------
const SVGNS = 'http://www.w3.org/2000/svg';
function sv(tag, attrs = {}, text) {
  const el = document.createElementNS(SVGNS, tag);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
  if (text != null) el.textContent = text;
  return el;
}
const FLOW = {
  nodes: [
    { id: 'submit', x: 6, y: 106, w: 74, hgt: 44, title: 'SUBMIT' },
    { id: 'PENDING', x: 118, y: 100, w: 112, hgt: 56, title: 'PENDING' },
    { id: 'LEASED', x: 300, y: 100, w: 112, hgt: 56, title: 'LEASED' },
    { id: 'SUCCEEDED', x: 462, y: 22, w: 94, hgt: 50, title: 'SUCCEEDED' },
    { id: 'FAILED', x: 462, y: 103, w: 94, hgt: 50, title: 'FAILED' },
    { id: 'DEAD', x: 462, y: 184, w: 94, hgt: 50, title: 'DEAD' },
  ],
  edges: [
    { id: 'submitted', d: 'M80 128 H116', c: '--muted', lx: 43, ly: 98, name: '' },
    { id: 'leased', d: 'M230 128 H298', c: '--st-leased', lx: 264, ly: 120, name: 'lease' },
    { id: 'succeeded', d: 'M412 116 C 436 116, 436 47, 460 47', c: '--st-ok', lx: 509, ly: 88, name: '' },
    { id: 'failed', d: 'M412 128 H460', c: '--st-failed', lx: 509, ly: 168, name: '' },
    { id: 'dead', d: 'M412 140 C 436 140, 436 209, 460 209', c: '--st-dead', lx: 509, ly: 249, name: '' },
    { id: 'retried', d: 'M340 156 C 340 200, 200 200, 200 158', c: '--st-retry', lx: 270, ly: 212, name: 'retry after backoff' },
    { id: 'requeued', d: 'M384 156 C 384 252, 150 252, 150 158', c: '--st-requeue', lx: 267, ly: 262, name: 'requeued: worker lost or lease expired' },
    { id: 'stale', d: 'M356 34 V 92', c: '--st-failed', lx: 348, ly: 62, name: '', anchor: 'end' },
  ],
};
function buildFlow() {
  const svg = $('#flow');
  const defs = sv('defs');
  for (const e of FLOW.edges) {
    // userSpaceOnUse keeps arrowheads one size however thick the edge gets.
    const m = sv('marker', { id: 'arr-' + e.id, viewBox: '0 0 10 10', refX: 8, refY: 5, markerWidth: 9, markerHeight: 9, markerUnits: 'userSpaceOnUse', orient: 'auto-start-reverse' });
    m.append(sv('path', { d: 'M0 0 L10 5 L0 10 z', style: `fill:var(${e.c})` }));
    defs.append(m);
  }
  svg.append(defs);
  for (const e of FLOW.edges) {
    svg.append(sv('path', { id: 'e-' + e.id, class: 'edge', d: e.d, style: `stroke:var(${e.c})`, 'stroke-width': 2, 'marker-end': `url(#arr-${e.id})` }));
  }
  // The stale-report edge ends in a barrier: the report never gets in.
  svg.append(sv('path', { d: 'M346 94 H366', style: 'stroke:var(--st-failed)', 'stroke-width': 3, 'stroke-linecap': 'round' }));
  svg.append(sv('text', { x: 356, y: 14, 'text-anchor': 'middle', class: 'elabel name' }, 'late report from a zombie'));
  for (const n of FLOW.nodes) {
    const g = sv('g', { class: 'node', id: 'n-' + n.id });
    g.append(sv('rect', { x: n.x, y: n.y, width: n.w, height: n.hgt, rx: 8 }));
    if (STATES[n.id]) g.append(sv('rect', { x: n.x + 10, y: n.y + n.hgt - 6, width: n.w - 20, height: 3, rx: 1.5, style: `fill:var(${STATES[n.id].c});stroke:none` }));
    g.append(sv('text', { x: n.x + n.w / 2, y: n.y + 17, 'text-anchor': 'middle', class: 'ttl' }, n.title));
    g.append(sv('text', { x: n.x + n.w / 2, y: n.y + 38, 'text-anchor': 'middle', class: 'cnt' }, '–'));
    svg.append(g);
  }
  for (const e of FLOW.edges) {
    const t = sv('text', { x: e.lx, y: e.ly, 'text-anchor': e.anchor || 'middle', class: 'elabel', id: 'l-' + e.id });
    svg.append(t);
  }
  svg.append(sv('text', { x: 6, y: 296, class: 'note' }, 'terminal counts are since the dashboard started watching'));
}
function renderFlow() {
  const s = S.snap, r = s.rates || {}, t = s.totals || {}, g = s.gauges || {};
  const counts = { submit: t.submitted, PENDING: g.pending, LEASED: g.leased, SUCCEEDED: t.succeeded, FAILED: t.failed, DEAD: t.dead };
  for (const n of FLOW.nodes) $(`#n-${n.id} .cnt`).textContent = fmtInt(counts[n.id]);
  for (const e of FLOW.edges) {
    const rate = r[e.id] || 0;
    const path = $('#e-' + e.id), label = $('#l-' + e.id);
    const live = rate > 0.05;
    path.classList.toggle('live', live);
    path.setAttribute('stroke-width', live ? Math.min(8, 2 + Math.log2(1 + rate)).toFixed(1) : 2);
    path.style.animationDuration = live ? Math.max(0.25, 2.4 / Math.sqrt(rate)).toFixed(2) + 's' : '';
    label.classList.toggle('live', live);
    let text = live ? `${fmtRate(rate)}/s` : '';
    if (e.id === 'stale') text = `fenced: ${fmtInt(t.stale)} rejected`;
    if (e.name) text = text ? `${e.name} · ${text}` : e.name;
    label.textContent = text;
  }
  const dup = t.duplicates ? `${fmtInt(t.duplicates)} dup keys` : '';
  let dl = $('#l-dup');
  if (!dl) { dl = sv('text', { id: 'l-dup', x: 43, y: 166, 'text-anchor': 'middle', class: 'elabel name' }); $('#flow').append(dl); }
  dl.textContent = dup;
}

// ---------- timeline ----------
const OUTCOMES = ['LEASED', 'SUCCEEDED', 'RETRY', 'REQUEUED', 'FAILED', 'DEAD', 'RELEASED'];
function buildTimelineLegend() {
  $('#tl-legend').replaceChildren(...OUTCOMES.map(k => h('span', {}, h('i', { style: `background:var(${STATES[k].c})` }),
    k === 'LEASED' ? 'held now' : 'ended: ' + STATES[k].label)),
  h('span', {}, h('i', { class: 'line', style: 'background:var(--text-2);width:2px;height:10px' }), 'event (hover)'));
}
const TL = { hits: [], marks: [] };
function drawTimeline() {
  const cv = $('#timeline');
  const s = S.snap;
  const W = cv.clientWidth;
  const left = 96, right = 8, top = 18, laneGap = 6;
  const workers = (s && s.workers) || [];
  // Lanes by host; a host's earlier worker ids map to the same lane.
  const laneOf = new Map();
  const lanes = workers.map((w, i) => {
    laneOf.set(w.id, i);
    for (const p of w.previous) laneOf.set(p, i);
    return { label: w.id ? short(w.id) : w.host, sub: w.state, rows: Math.min(Math.max(w.concurrency, 1), 8), spans: [] };
  });
  const other = { label: 'other', sub: 'gone', rows: 2, spans: [] };
  const end = now(), span = S.tlWindow * 1000, start = end - span;
  for (const sp of (s && s.jobs && s.jobs.spans) || []) {
    if (sp.end && sp.end < start) continue;
    const i = laneOf.get(sp.worker);
    (i == null ? other : lanes[i]).spans.push(sp);
  }
  if (other.spans.length) lanes.push(other);
  const rowH = 9, rowGap = 2;
  let H = top;
  for (const l of lanes) { l.y = H; l.h = l.rows * (rowH + rowGap) + 4; H += l.h + laneGap; }
  H = Math.max(H + 16, 80);
  const dpr = window.devicePixelRatio || 1;
  if (cv.width !== Math.round(W * dpr) || cv.height !== Math.round(H * dpr)) {
    cv.width = Math.round(W * dpr); cv.height = Math.round(H * dpr); cv.style.height = H + 'px';
  }
  const ctx = cv.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, W, H);
  const x = t => left + (t - start) / span * (W - left - right);
  const font = '11px system-ui, sans-serif';
  ctx.font = font;

  // Grid: one line every 5s (10s for the 60s window), labeled relative to now.
  const step = S.tlWindow > 30 ? 10000 : 5000;
  ctx.fillStyle = css('--muted');
  ctx.strokeStyle = css('--grid');
  ctx.lineWidth = 1;
  ctx.textAlign = 'center';
  for (let k = 0; k * step <= span; k++) {
    const t = end - k * step, xx = Math.round(x(t)) + 0.5;
    ctx.beginPath(); ctx.moveTo(xx, top - 4); ctx.lineTo(xx, H - 14); ctx.stroke();
    ctx.textAlign = k === 0 ? 'right' : 'center';
    ctx.fillText(k === 0 ? 'now' : `−${k * step / 1000}s`, k === 0 ? W - right : xx, H - 2);
  }

  TL.hits = [];
  if (!lanes.length) {
    ctx.textAlign = 'left';
    ctx.fillStyle = css('--muted');
    ctx.fillText(s && s.backend ? 'No workers yet.' : 'No cluster.', left, top + 14);
  }
  for (const l of lanes) {
    ctx.textAlign = 'left';
    ctx.fillStyle = css('--text');
    ctx.font = '600 11px ' + css('--mono');
    ctx.fillText(l.label, 4, l.y + 10);
    ctx.font = font;
    ctx.fillStyle = css('--muted');
    ctx.fillText(l.sub === 'ALIVE' ? '' : (l.sub || '').toLowerCase(), 4, l.y + 23);
    ctx.fillStyle = css('--surface-2');
    ctx.fillRect(left, l.y, W - left - right, l.h - 2);
    // Greedy row assignment: each lease takes the first row free at its start.
    const rowsEnd = new Array(l.rows).fill(-Infinity);
    l.spans.sort((a, b) => a.start - b.start);
    for (const sp of l.spans) {
      const se = sp.end || end;
      let r = rowsEnd.findIndex(e => e <= sp.start);
      if (r < 0) r = rowsEnd.indexOf(Math.min(...rowsEnd));
      rowsEnd[r] = se;
      const x0 = Math.max(left, x(sp.start)), x1 = Math.max(x0 + 2, x(se));
      const y0 = l.y + 2 + r * (rowH + rowGap);
      const key = sp.end ? sp.outcome : 'LEASED';
      ctx.fillStyle = stColor(key);
      roundRect(ctx, x0, y0, x1 - x0, rowH, 2);
      ctx.fill();
      if (!sp.end) { // still held: a bright leading edge
        ctx.fillStyle = css('--text');
        ctx.fillRect(x1 - 1.5, y0, 1.5, rowH);
      }
      if (x1 - x0 > 46) {
        ctx.fillStyle = 'rgba(0,0,0,.78)'; // dark ink reads on every state color, light or dark mode
        ctx.font = '10px ' + css('--mono');
        ctx.textAlign = 'left';
        ctx.fillText('#' + sp.token, x0 + 3, y0 + rowH - 1.5);
        ctx.font = font;
      }
      TL.hits.push({ x0, x1, y0, y1: y0 + rowH, sp });
    }
  }
  // Marks: actions and incidents from the feed.
  TL.marks = [];
  ctx.strokeStyle = css('--text-2');
  for (const f of S.feed) {
    if (!f.mark || f.at < start) continue;
    const xx = Math.round(x(f.at)) + 0.5;
    ctx.setLineDash([2, 3]);
    ctx.beginPath(); ctx.moveTo(xx, top - 6); ctx.lineTo(xx, H - 14); ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = f.level === 'bad' ? css('--critical') : f.level === 'good' ? css('--good') : css('--text-2');
    ctx.beginPath(); ctx.moveTo(xx - 4, top - 10); ctx.lineTo(xx + 4, top - 10); ctx.lineTo(xx, top - 4); ctx.fill();
    TL.marks.push({ x: xx, f });
  }
}
function roundRect(ctx, x, y, w, hh, r) {
  r = Math.min(r, w / 2, hh / 2);
  ctx.beginPath();
  ctx.moveTo(x + r, y); ctx.arcTo(x + w, y, x + w, y + hh, r); ctx.arcTo(x + w, y + hh, x, y + hh, r);
  ctx.arcTo(x, y + hh, x, y, r); ctx.arcTo(x, y, x + w, y, r); ctx.closePath();
}
function timelineHit(e) {
  const r = $('#timeline').getBoundingClientRect();
  const px = e.clientX - r.left, py = e.clientY - r.top;
  const span = TL.hits.find(b => px >= b.x0 - 2 && px <= b.x1 + 2 && py >= b.y0 - 2 && py <= b.y1 + 2);
  const mark = !span && py < 20 ? TL.marks.find(m => Math.abs(m.x - px) < 6) : null;
  return { span, mark };
}
$('#timeline').addEventListener('pointermove', e => {
  const { span, mark } = timelineHit(e);
  if (span) {
    const sp = span.sp;
    const dur = (sp.end || now()) - sp.start;
    showTip(e, `${sp.type} job ${short(sp.job)}`, [
      [STATES[sp.end ? sp.outcome : 'LEASED'].c, sp.end ? STATES[sp.outcome].label : 'held now', `lease ${sp.lease}`],
      ['--text-2', '#' + sp.token, 'fencing token'],
      ['--text-2', fmtAge(dur), 'held for'],
      ['--text-2', short(sp.worker), 'worker'],
    ]);
  } else if (mark) {
    showTip(e, clock(mark.f.at), [], mark.f.text);
  } else hideTip();
});
$('#timeline').addEventListener('pointerleave', hideTip);
$('#timeline').addEventListener('pointerdown', e => {
  const { span } = timelineHit(e);
  if (span) openJob(span.sp.job);
});
$('#tl-window').addEventListener('click', e => {
  const b = e.target.closest('button[data-w]');
  if (!b) return;
  S.tlWindow = Number(b.dataset.w);
  $$('#tl-window button').forEach(x => x.classList.toggle('on', x === b));
});

// ---------- tooltip ----------
function showTip(e, title, rows, text) {
  const tip = $('#tip');
  tip.replaceChildren(h('div', { class: 'tt' }, title),
    ...rows.map(([c, v, label]) => h('div', { class: 'tr' }, h('i', { style: `background:var(${c})` }), h('b', {}, v), h('span', {}, label))),
    text ? h('div', {}, text) : null);
  tip.hidden = false;
  const pad = 14, w = tip.offsetWidth, hh = tip.offsetHeight;
  let x = e.clientX + pad, y = e.clientY + pad;
  if (x + w > innerWidth - 8) x = e.clientX - w - pad;
  if (y + hh > innerHeight - 8) y = e.clientY - hh - pad;
  tip.style.left = x + 'px'; tip.style.top = y + 'px';
}
function hideTip() { $('#tip').hidden = true; }

// ---------- charts ----------
// Each chart: one y-axis, 2px lines, a legend, direct end labels, a
// crosshair tooltip that lists every series, and the feed's marked events
// as vertical hairlines.
const CHARTS = {
  throughput: {
    unit: '/s', fmt: fmtRate,
    series: [
      { k: 'submitted', label: 'submitted', c: '--text-2', dash: true },
      { k: 'succeeded', label: 'succeeded', c: '--st-ok' },
      { k: 'retried', label: 'retried', c: '--st-retry' },
      { k: 'requeued', label: 'requeued', c: '--st-requeue' },
    ],
  },
  queue: {
    unit: '', fmt: fmtInt,
    series: [
      { k: 'pending', label: 'waiting (pending)', c: '--st-pending' },
      { k: 'leased', label: 'in flight (leased)', c: '--st-leased' },
    ],
  },
  dispatch: {
    unit: 'ms', fmt: fmtMs, log: true,
    series: [
      { k: 'dispatch_p50', label: 'p50', c: '--st-leased' },
      { k: 'dispatch_p99', label: 'p99', c: '--st-pending' },
    ],
  },
  store: {
    unit: 'ms', fmt: fmtMs, log: true,
    series: [
      { k: 'commit_p50', label: 'commit p50', c: '--st-leased' },
      { k: 'commit_p99', label: 'commit p99', c: '--st-pending' },
    ],
  },
};
function configureStoreChart() {
  const raft = S.snap && S.snap.backend === 'raft';
  const c = CHARTS.store;
  const want = raft ? 'raft' : 'redis';
  if (c.mode === want) return;
  c.mode = want;
  $('#store-title').textContent = raft ? 'Raft commit latency' : 'Store round trip';
  $('#store-hint').textContent = raft ? 'propose → committed and applied on the leader · last 10s · ms, log scale'
    : 'one acquire Lua script, coordinator → Redis → back · p99 over the last 10s · ms, log scale';
  c.series = raft
    ? [{ k: 'commit_p50', label: 'p50', c: '--st-leased' }, { k: 'commit_p99', label: 'p99', c: '--st-pending' }]
    : [{ k: 'acquire_p99', label: 'acquire p99', c: '--st-leased' }];
  buildLegend('store');
}
function buildLegend(name) {
  const c = CHARTS[name];
  const el = $(`[data-chart="${name}"] .legend`);
  // A single series is named by the chart title; no legend box.
  el.replaceChildren(...(c.series.length < 2 ? [] : c.series.map(s => h('span', {},
    h('i', { class: 'line' + (s.dash ? ' dash' : ''), style: s.dash ? `border-color:var(${s.c})` : `background:var(${s.c})` }), s.label))));
}
const chartHover = {};
function renderCharts() {
  configureStoreChart();
  for (const name of Object.keys(CHARTS)) drawChart(name);
}
function niceMax(v) {
  if (v <= 0) return 1;
  const p = Math.pow(10, Math.floor(Math.log10(v)));
  for (const m of [1, 2, 2.5, 5, 10]) if (v <= m * p) return m * p;
  return 10 * p;
}
function drawChart(name) {
  const c = CHARTS[name];
  const cv = $(`[data-chart="${name}"] canvas`);
  const W = cv.clientWidth, H = cv.clientHeight;
  if (!W) return;
  const dpr = window.devicePixelRatio || 1;
  if (cv.width !== Math.round(W * dpr) || cv.height !== Math.round(H * dpr)) { cv.width = Math.round(W * dpr); cv.height = Math.round(H * dpr); }
  const ctx = cv.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, W, H);
  const pts = S.series;
  const L = 44, R = 64, T = 8, B = 18;
  const end = pts.length ? pts[pts.length - 1].t : now(), start = end - 5 * 60 * 1000;
  const x = t => L + (t - start) / (end - start) * (W - L - R);
  let lo = Infinity, hi = 0;
  for (const p of pts) for (const s of c.series) {
    const v = p[s.k];
    if (v == null) continue;
    hi = Math.max(hi, v);
    if (v > 0) lo = Math.min(lo, v);
  }
  let y, ticks;
  if (c.log) {
    const a = Math.floor(Math.log10(isFinite(lo) ? lo : 0.1)), b = Math.max(a + 1, Math.ceil(Math.log10(hi || 1)));
    y = v => T + (1 - (Math.log10(Math.max(v, Math.pow(10, a))) - a) / (b - a)) * (H - T - B);
    ticks = []; for (let e = a; e <= b; e++) ticks.push(Math.pow(10, e));
  } else {
    const m = niceMax(hi * 1.1);
    y = v => T + (1 - v / m) * (H - T - B);
    ticks = [0, m / 2, m];
  }
  ctx.font = '11px system-ui, sans-serif';
  ctx.lineWidth = 1;
  ctx.strokeStyle = css('--grid');
  ctx.fillStyle = css('--muted');
  ctx.textAlign = 'right';
  for (const tv of ticks) {
    const yy = Math.round(y(tv)) + 0.5;
    ctx.beginPath(); ctx.moveTo(L, yy); ctx.lineTo(W - R, yy); ctx.stroke();
    ctx.fillText(c.log ? fmtMs(tv).replace(' ', '') : compact(tv), L - 6, yy + 4);
  }
  ctx.textAlign = 'center';
  for (let k = 0; k <= 5; k++) {
    const t = end - k * 60000;
    ctx.fillText(k === 0 ? 'now' : `−${k}m`, Math.min(Math.max(x(t), L + 10), W - R - 10), H - 4);
  }
  // Event marks.
  ctx.strokeStyle = css('--muted');
  ctx.setLineDash([2, 3]);
  for (const f of S.feed) {
    if (!f.mark || f.at < start) continue;
    const xx = Math.round(x(f.at)) + 0.5;
    ctx.beginPath(); ctx.moveTo(xx, T); ctx.lineTo(xx, H - B); ctx.stroke();
  }
  ctx.setLineDash([]);
  // Lines.
  ctx.lineJoin = 'round'; ctx.lineCap = 'round';
  const ends = [];
  for (const s of c.series) {
    ctx.strokeStyle = css(s.c);
    ctx.lineWidth = 2;
    ctx.setLineDash(s.dash ? [4, 4] : []);
    ctx.beginPath();
    let pen = false, last = null;
    for (const p of pts) {
      const v = p[s.k];
      if (v == null || p.t < start) { pen = false; continue; }
      const xx = x(p.t), yy = y(c.log ? Math.max(v, 1e-9) : v);
      if (pen) ctx.lineTo(xx, yy); else ctx.moveTo(xx, yy);
      pen = true; last = { yy, v };
    }
    ctx.stroke();
    if (last) ends.push({ s, ...last });
  }
  ctx.setLineDash([]);
  // Direct end labels, nudged apart so they never overlap.
  ends.sort((a, b) => a.yy - b.yy);
  for (let i = 1; i < ends.length; i++) if (ends[i].yy - ends[i - 1].yy < 12) ends[i].yy = ends[i - 1].yy + 12;
  ctx.textAlign = 'left';
  for (const e of ends) {
    ctx.fillStyle = css(e.s.c);
    ctx.fillRect(W - R + 4, e.yy - 1, 8, 2);
    ctx.fillStyle = css('--text-2');
    ctx.fillText(c.fmt(e.v) + (c.unit === '/s' ? '/s' : ''), W - R + 15, e.yy + 4);
  }
  // Crosshair.
  const hv = chartHover[name];
  if (hv != null && pts.length) {
    let best = null;
    for (const p of pts) if (p.t >= start && (!best || Math.abs(x(p.t) - hv) < Math.abs(x(best.t) - hv))) best = p;
    if (best) {
      const xx = Math.round(x(best.t)) + 0.5;
      ctx.strokeStyle = css('--text-2');
      ctx.beginPath(); ctx.moveTo(xx, T); ctx.lineTo(xx, H - B); ctx.stroke();
      for (const s of c.series) {
        const v = best[s.k];
        if (v == null) continue;
        ctx.fillStyle = css('--surface');
        ctx.beginPath(); ctx.arc(xx, y(c.log ? Math.max(v, 1e-9) : v), 6, 0, 7); ctx.fill();
        ctx.fillStyle = css(s.c);
        ctx.beginPath(); ctx.arc(xx, y(c.log ? Math.max(v, 1e-9) : v), 4, 0, 7); ctx.fill();
      }
      c.hoverPoint = best;
    }
  }
}
function compact(v) {
  if (v >= 1000) return (v / 1000).toFixed(v >= 10000 ? 0 : 1).replace(/\.0$/, '') + 'K';
  return Number.isInteger(v) ? String(v) : v.toFixed(1);
}
for (const name of Object.keys(CHARTS)) {
  const cv = $(`[data-chart="${name}"] canvas`);
  cv.addEventListener('pointermove', e => {
    const r = cv.getBoundingClientRect();
    chartHover[name] = e.clientX - r.left;
    drawChart(name);
    const c = CHARTS[name], p = c.hoverPoint;
    if (!p) return;
    const marks = S.feed.filter(f => f.mark && Math.abs(f.at - p.t) < 1500);
    showTip(e, clock(p.t), c.series.map(s => [s.c, p[s.k] == null ? '–' : c.fmt(p[s.k]) + (c.unit === '/s' ? '/s' : ''), s.label]),
      marks.length ? marks.map(m => m.text).join(' ') : null);
  });
  cv.addEventListener('pointerleave', () => { chartHover[name] = null; hideTip(); drawChart(name); });
}

// ---------- feed ----------
const ICON = { good: '✓', warn: '!', bad: '✕', info: '•' };
function renderFeed(items, fresh) {
  const ol = $('#feed');
  for (const f of items) {
    ol.prepend(h('li', { class: `lv-${f.level}` + (fresh ? ' fresh' : '') },
      h('span', { class: 't' }, clock(f.at)),
      h('span', { class: 'ico', 'aria-label': f.level }, ICON[f.level] || '•'),
      h('span', {}, h('span', { class: 'kind' }, f.kind), f.text)));
  }
  while (ol.children.length > 200) ol.lastChild.remove();
}

// ---------- jobs ----------
function stBadge(status) {
  return h('span', { class: 'st', style: `--c:var(${(STATES[status] || {}).c || '--muted'})` }, (STATES[status] || {}).label || status.toLowerCase());
}
function renderJobs() {
  const rows = ((S.snap.jobs || {}).recent || []).map(j => h('tr', { 'data-job': j.id },
    h('td', { class: 'mono' }, short(j.id)),
    h('td', {}, j.type),
    h('td', {}, stBadge(j.status)),
    h('td', { class: 'num' }, j.max_attempts ? `${j.attempts}/${j.max_attempts}` : '–'),
    h('td', { class: 'mono' }, short(j.worker)),
    h('td', { class: 'num mono' }, j.token ? '#' + j.token : ''),
    h('td', { class: 'num' }, fmtAge((j.finished || now()) - j.submitted))));
  $('#jobs').replaceChildren(...(rows.length ? rows : [h('tr', {}, h('td', { colspan: 7, class: 'hint' }, 'Nothing submitted yet. Start the load generator or submit a job.'))]));
}
$('#jobs').addEventListener('pointerdown', e => {
  const tr = e.target.closest('tr[data-job]');
  if (tr) openJob(tr.dataset.job);
});

// What each history event means, for the job view.
const EXPLAIN = {
  SUBMITTED: e => 'Accepted and stored. The job is PENDING and eligible to run.',
  LEASED: e => `Leased to worker ${short(e.worker)} under fencing token #${e.token}. Only a report carrying #${e.token} from this worker can move the job on.`,
  SUCCEEDED: e => `Worker ${short(e.worker)} reported success under #${e.token}; the result was written once. Terminal.`,
  RETRY: e => `The attempt failed with a retryable error${e.detail ? ` (${e.detail})` : ''}. Back to PENDING after an exponential backoff.`,
  FAILED: e => `A non-retryable failure${e.detail ? ` (${e.detail})` : ''}. Terminal.`,
  DEAD: e => `No attempts left${e.detail ? ` (${e.detail})` : ''}: dead-lettered. Terminal.`,
  REQUEUED: e => `The lease was revoked${e.detail ? ` (${e.detail})` : ''}. The job is PENDING again and token #${e.token} is now stale: any report under it is rejected.`,
  RELEASED: e => 'The acquire reply could not be delivered, so the lease was handed straight back. The attempt does not count.',
};
let openJobId = null, jobTimer = null;
async function openJob(id) {
  openJobId = id;
  $('#job-modal').hidden = false;
  await loadJob();
  clearInterval(jobTimer);
  jobTimer = setInterval(loadJob, 1000);
}
function closeJob() {
  openJobId = null;
  clearInterval(jobTimer);
  $('#job-modal').hidden = true;
}
$('#job-modal').addEventListener('click', e => { if (e.target.id === 'job-modal' || e.target.closest('.close')) closeJob(); });
document.addEventListener('keydown', e => { if (e.key === 'Escape' && openJobId) closeJob(); });
async function loadJob() {
  const id = openJobId;
  if (!id) return;
  let j;
  try {
    const res = await fetch('/api/job?id=' + encodeURIComponent(id));
    j = await res.json();
    if (!res.ok) throw new Error(j.error);
  } catch (e) {
    $('#job-body').replaceChildren(h('p', { class: 'error' }, e.message));
    return;
  }
  if (id !== openJobId) return;
  if (['SUCCEEDED', 'FAILED', 'DEAD'].includes(j.status)) clearInterval(jobTimer);
  const t0 = j.created_at, t1 = j.finished_at || now();
  const dur = Math.max(1, t1 - t0);
  const gantt = h('div', { class: 'gantt' }, ...j.spans.map(sp => {
    const a = (sp.start - t0) / dur * 100, b = ((sp.end || t1) - t0) / dur * 100;
    const k = sp.end ? sp.outcome : 'LEASED';
    return h('div', { class: 'g' },
      h('i', { style: `left:${a}%;width:${Math.max(0.5, b - a)}%;background:var(${STATES[k].c})` }),
      h('em', { style: b < 62 ? `left:calc(${b}% + 6px)` : `right:calc(${100 - a}% + 6px)` },
        `#${sp.token} · ${short(sp.worker)} · ${sp.end ? STATES[sp.outcome].label : 'held'} after ${fmtAge((sp.end || t1) - sp.start)}`));
  }));
  const fact = (k, v) => h('div', {}, h('span', {}, k), h('b', {}, v || '–'));
  $('#job-body').replaceChildren(
    h('h2', { id: 'job-title' }, `${j.type} job `, h('span', { class: 'mono' }, j.id)),
    h('div', { style: 'margin-top:6px' }, stBadge(j.status)),
    h('div', { class: 'facts' },
      fact('Attempts', `${j.attempts} of ${j.max_attempts}`),
      fact('Current token', j.lease_token ? '#' + j.lease_token : ''),
      fact('Lease owner', short(j.lease_owner)),
      fact('Lease expires', j.status === 'LEASED' && j.lease_expires_at ? `in ${fmtAge(j.lease_expires_at - now())}` : ''),
      fact('Payload', j.payload),
      fact('Idempotency key', j.idempotency_key),
      fact('Age', fmtAge(t1 - t0) + (j.finished_at ? ' (finished)' : ''))),
    j.spans.length ? h('h3', {}, 'Leases') : null, j.spans.length ? gantt : null,
    h('h3', { style: 'margin-bottom:8px' }, 'History'),
    h('ol', { class: 'events' }, ...j.events.map(e => h('li', { style: `--c:var(${(STATES[e.kind] || {}).c || '--muted'})` },
      h('span', { class: 'ek' }, e.kind), h('span', { class: 'em' }, `${clock(e.at)} · +${fmtAge(e.at - t0)}`),
      h('p', {}, (EXPLAIN[e.kind] || (() => e.detail))(e))))),
    j.last_error ? h('div', {}, h('h3', {}, 'Last error'), h('pre', { class: 'result' }, j.last_error)) : null,
    j.result ? h('div', {}, h('h3', {}, 'Result'), h('pre', { class: 'result' }, j.result)) : null,
  );
}

// ---------- controls ----------
$$('.tabs button').forEach(b => b.addEventListener('click', () => selectTab(b.dataset.tab)));
function selectTab(name) {
  $$('.tabs button').forEach(b => b.classList.toggle('on', b.dataset.tab === name));
  $$('.tab').forEach(p => { p.hidden = p.dataset.pane !== name; });
}
document.addEventListener('click', e => {
  const g = e.target.closest('[data-goto]');
  if (g) selectTab(g.dataset.goto);
  const c = e.target.closest('[data-cancel]');
  if (c) c.closest('.confirm').hidden = true;
});

// Forms fill from the server until the user edits them.
function fillForm(form, values) {
  for (const [k, v] of Object.entries(values)) {
    const els = $$(`[name="${k}"]`, form);
    for (const el of els) {
      if (el.type === 'radio') el.checked = el.value === v;
      else if (el.type === 'checkbox') el.checked = !!v;
      else el.value = v;
    }
  }
}
function readForm(form, numeric) {
  const out = {};
  for (const el of form.elements) {
    if (!el.name || el.name.endsWith('_range')) continue;
    if (el.type === 'radio') { if (el.checked) out[el.name] = el.value; continue; }
    if (el.type === 'checkbox') { out[el.name] = el.checked; continue; }
    out[el.name] = numeric.includes(el.name) ? Number(el.value) : el.value;
  }
  return out;
}

// Load generator.
const LOAD_PRESETS = {
  'Steady sleep jobs': { rate: 20, mix_noop: 0, mix_sleep: 1, mix_fail: 0, sleep_min_ms: 200, sleep_max_ms: 1500, fail_n: 1, max_attempts: 3, dup_pct: 0 },
  'Long jobs': { rate: 4, mix_noop: 0, mix_sleep: 1, mix_fail: 0, sleep_min_ms: 2000, sleep_max_ms: 5000, fail_n: 1, max_attempts: 3, dup_pct: 0 },
  'Flaky': { rate: 15, mix_noop: 1, mix_sleep: 1, mix_fail: 2, sleep_min_ms: 100, sleep_max_ms: 800, fail_n: 2, max_attempts: 3, dup_pct: 0 },
  'Duplicates': { rate: 20, mix_noop: 1, mix_sleep: 1, mix_fail: 0, sleep_min_ms: 100, sleep_max_ms: 500, fail_n: 1, max_attempts: 3, dup_pct: 30 },
  'Saturate (noop)': { rate: 1500, mix_noop: 1, mix_sleep: 0, mix_fail: 0, sleep_min_ms: 0, sleep_max_ms: 0, fail_n: 1, max_attempts: 3, dup_pct: 0 },
};
const LOAD_NUM = ['rate', 'mix_noop', 'mix_sleep', 'mix_fail', 'sleep_min_ms', 'sleep_max_ms', 'fail_n', 'max_attempts', 'dup_pct'];
const loadForm = $('#load-form');
let loadDirty = false;
loadForm.addEventListener('input', e => {
  loadDirty = true;
  if (e.target.name === 'rate_range') loadForm.rate.value = e.target.value;
  if (e.target.name === 'rate') loadForm.rate_range.value = e.target.value;
});
$('#load-presets').append(...Object.entries(LOAD_PRESETS).map(([name, cfg]) => h('button', { type: 'button', onclick: () => {
  fillForm(loadForm, cfg); loadForm.rate_range.value = cfg.rate; loadDirty = true;
} }, name)));
loadForm.addEventListener('submit', async e => {
  e.preventDefault();
  const cfg = readForm(loadForm, LOAD_NUM);
  try { await act('/api/load', { running: true, config: cfg }); loadDirty = false; } catch {}
});
$('#load-stop').addEventListener('click', () => act('/api/load', { running: false, config: readForm(loadForm, LOAD_NUM) }).catch(() => {}));

// Manual submit.
const submitForm = $('#submit-form');
const PAYLOADS = { sleep: '{"duration_ms":2000}', noop: '', 'fail-n-times': '{"n":2}' };
submitForm.type.addEventListener('change', () => { submitForm.payload.value = PAYLOADS[submitForm.type.value]; });
submitForm.addEventListener('submit', async e => {
  e.preventDefault();
  const body = readForm(submitForm, ['count', 'max_attempts']);
  try {
    const r = await act('/api/submit', body);
    $('#submit-stat').replaceChildren(
      r.ids.length === 1 ? 'Submitted ' : `Submitted ${r.ids.length} jobs; first `,
      h('a', { href: '#', onclick: ev => { ev.preventDefault(); openJob(r.ids[0]); } }, short(r.ids[0])),
      r.duplicates ? ` · ${r.duplicates} answered from the existing idempotency key (same job id, nothing new created)` : '');
  } catch {}
});

// Cluster form.
const CLUSTER_PRESETS = {
  'Compose defaults': {},
  'Fast failure detection': { heartbeat_interval: '250ms', missed_heartbeats: 3, lease_duration: '3s' },
  'Forgiving': { heartbeat_interval: '1s', missed_heartbeats: 8, lease_duration: '30s' },
  'Big pool': { workers: 8, concurrency: 8 },
  'Twitchy Raft': { raft_heartbeat_timeout: '300ms', raft_election_timeout: '300ms', raft_leader_lease_timeout: '150ms' },
};
const DEFAULT_CLUSTER = { backend: 'raft', workers: 3, concurrency: 4, sleep_duration: '20ms', lease_duration: '10s', heartbeat_interval: '1s',
  missed_heartbeats: 3, poll_timeout: '5s', backoff_base: '200ms', raft_heartbeat_timeout: '1s', raft_election_timeout: '1s',
  raft_leader_lease_timeout: '500ms', observability: false, rebuild: false };
const CLUSTER_NUM = ['workers', 'concurrency', 'missed_heartbeats'];
const clusterForm = $('#cluster-form');
let clusterDirty = false, clusterFilled = false;
fillForm(clusterForm, DEFAULT_CLUSTER);
clusterForm.addEventListener('input', () => { clusterDirty = true; validateCluster(); });
$('#cluster-presets').append(...Object.entries(CLUSTER_PRESETS).map(([name, cfg]) => h('button', { type: 'button', onclick: () => {
  const backend = readForm(clusterForm, []).backend;
  fillForm(clusterForm, { ...DEFAULT_CLUSTER, backend, ...cfg }); clusterDirty = true; validateCluster();
} }, name)));

function parseDur(s) {
  const m = /^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|us|µs|ns)\s*$/.exec(s || '');
  if (!m) return null;
  return Number(m[1]) * { ns: 1e-6, us: 1e-3, 'µs': 1e-3, ms: 1, s: 1000, m: 60000, h: 3600000 }[m[2]];
}
// Mirrors ClusterConfig.Validate so mistakes show before launching; the
// server checks again.
function validateCluster() {
  const c = readForm(clusterForm, CLUSTER_NUM);
  $$('.raft-only', clusterForm).forEach(el => { el.hidden = c.backend !== 'raft'; });
  const hb = parseDur(c.heartbeat_interval), lease = parseDur(c.lease_duration);
  const deadAfter = hb != null ? hb * c.missed_heartbeats : null;
  $('#dead-after').textContent = deadAfter != null
    ? `A silent worker is declared dead after ${fmtAge(deadAfter)} (missed heartbeats × interval), and its jobs are requeued.` : '';
  let err = '';
  for (const k of ['sleep_duration', 'lease_duration', 'heartbeat_interval', 'poll_timeout', 'backoff_base', 'raft_heartbeat_timeout', 'raft_election_timeout', 'raft_leader_lease_timeout']) {
    if (parseDur(c[k]) == null) { err = `${k.replaceAll('_', ' ')}: "${c[k]}" is not a duration (try 500ms or 2s)`; break; }
  }
  if (!err && lease <= deadAfter) err = `The lease (${c.lease_duration}) must outlast the failure-detection window (${fmtAge(deadAfter)}), or leases would expire under live workers.`;
  if (!err && c.backend === 'raft') {
    if (parseDur(c.raft_leader_lease_timeout) > parseDur(c.raft_heartbeat_timeout)) err = 'The Raft leader lease timeout cannot exceed the Raft heartbeat timeout.';
    else if (parseDur(c.raft_election_timeout) < parseDur(c.raft_heartbeat_timeout)) err = 'The Raft election timeout must be at least the Raft heartbeat timeout.';
  }
  $('#cluster-error').textContent = err;
  $('#cluster-error').hidden = !err;
  return !err;
}
clusterForm.addEventListener('submit', e => {
  e.preventDefault();
  if (!validateCluster()) return;
  $('#launch-confirm').hidden = false;
});
$('#launch-yes').addEventListener('click', async () => {
  $('#launch-confirm').hidden = true;
  try { await act('/api/cluster/launch', readForm(clusterForm, CLUSTER_NUM), 'Launching…'); clusterDirty = false; } catch {}
});
$('#cluster-stop').addEventListener('click', () => { $('#stop-confirm').hidden = false; });
$('#stop-yes').addEventListener('click', async () => {
  $('#stop-confirm').hidden = true;
  try { await act('/api/cluster/stop', {}, 'Stopping…'); } catch {}
});
let scaleDirty = false;
$('#scale-n').addEventListener('input', () => { scaleDirty = true; });
$('#scale-apply').addEventListener('click', async () => {
  try { await act('/api/cluster/scale', { workers: Number($('#scale-n').value) }, 'Scaling…'); scaleDirty = false; } catch {}
});

// Scenarios.
$('#scenario-stop').addEventListener('click', () => act('/api/scenario', { stop: true }).catch(() => {}));
function renderScenarios() {
  const s = S.snap;
  const running = s.scenario && s.scenario.running;
  const list = $('#scenarios');
  const sig = JSON.stringify((s.scenarios || []).map(x => [x.name, x.disabled])) + running;
  if (list.dataset.sig !== sig) {
    list.dataset.sig = sig;
    list.replaceChildren(...(s.scenarios || []).map(x => h('div', { class: 'scen' },
      h('h4', {}, x.title),
      h('p', {}, x.blurb),
      h('p', { class: 'watch' }, 'Watch for: ' + x.watch),
      h('div', { class: 'row-end' },
        h('span', { class: 'hint' }, x.disabled ? 'Unavailable: ' + x.disabled : ''),
        h('button', { class: 'btn primary small', disabled: !!x.disabled || running, onclick: () => act('/api/scenario', { name: x.name }).catch(() => {}) }, 'Run')))));
  }
  const box = $('#scenario-run');
  box.hidden = !s.scenario;
  if (s.scenario) {
    $('#scenario-title').textContent = s.scenario.title + (s.scenario.running ? '' : ' (finished)');
    $('#scenario-stop').hidden = !s.scenario.running;
    const lines = $('#scenario-lines');
    if (lines.children.length !== s.scenario.lines.length) {
      lines.replaceChildren(...s.scenario.lines.map(l => h('li', { class: 'lv-' + l.level },
        h('span', { class: 'ico' }, ICON[l.level] || '•'), h('span', {}, l.text))));
    }
  }
}

function renderControls() {
  const s = S.snap;
  // Load.
  const lv = s.load;
  if (lv) {
    if (!loadDirty) { fillForm(loadForm, lv.config); loadForm.rate_range.value = lv.config.rate; }
    $('#load-start').textContent = lv.running ? 'Apply changes' : 'Start load';
    $('#load-stop').disabled = !lv.running;
    $('#load-start').disabled = !s.backend;
    const achieved = lv.seconds > 1 ? (lv.submitted + lv.duplicates) / lv.seconds : 0;
    $('#load-stat').textContent = !s.backend ? 'Needs a cluster.' : lv.running
      ? `Running ${fmtRate(achieved)}/s of ${lv.config.rate}/s requested · ${fmtInt(lv.submitted)} submitted · ${fmtInt(lv.duplicates)} deduplicated · ${fmtInt(lv.errors)} failed${lv.errors ? ' (a submit whose outcome is unknown, e.g. mid-election, is reported rather than blindly retried)' : ''}${lv.dropped ? ` · ${fmtInt(lv.dropped)} dropped (submitters behind)` : ''}${lv.last_error ? ` · last error: ${lv.last_error}` : ''}`
      : lv.submitted ? `Stopped after ${fmtInt(lv.submitted)} submits.` : 'Stopped.';
  }
  renderScenarios();
  // Cluster.
  const stat = $('#cluster-stat');
  if (s.busy) stat.textContent = `${s.busy}…`;
  else if (s.backend) stat.textContent = `${s.project || 'external cluster'} · ${s.backend}` + (s.config ? ` · ${s.config.workers} workers × ${s.config.concurrency} slots · lease ${s.config.lease_duration} · heartbeat ${s.config.heartbeat_interval} × ${s.config.missed_heartbeats}` : ' · configuration unknown');
  else stat.textContent = s.phase === 'error' ? `Last operation failed: ${s.phase_error}` : 'No cluster.';
  $('#scale-row').hidden = !s.docker || !s.project;
  if (s.config && !scaleDirty) $('#scale-n').value = s.config.workers;
  $('#scale-apply').disabled = $('#cluster-stop').disabled = !!s.busy;
  $('button[type=submit]', clusterForm).disabled = !s.docker || !!s.busy;
  if (!s.docker) {
    $('#cluster-error').textContent = 'Docker controls are off: docker is unavailable, or the dashboard is watching an external cluster.';
    $('#cluster-error').hidden = false;
  }
  if (s.config && !clusterDirty && !clusterFilled) {
    fillForm(clusterForm, { ...s.config, rebuild: false });
    clusterFilled = true;
    validateCluster();
  }
  const log = $('#oplog');
  log.hidden = !(s.op_log && s.op_log.length);
  const text = (s.op_log || []).join('\n');
  if (log.textContent !== text) {
    const atEnd = log.scrollTop + log.clientHeight >= log.scrollHeight - 4;
    log.textContent = text;
    if (atEnd) log.scrollTop = log.scrollHeight;
  }
}

// ---------- start ----------
buildFlow();
buildTimelineLegend();
for (const name of Object.keys(CHARTS)) buildLegend(name);
validateCluster();
connect();
let lastTl = 0;
(function frame(t) {
  animateHeartbeats();
  if (t - lastTl > 80) { lastTl = t; drawTimeline(); }
  requestAnimationFrame(frame);
})(0);
new ResizeObserver(() => renderCharts()).observe($('main'));
matchMedia('(prefers-color-scheme: dark)').addEventListener('change', () => { if (S.snap) render(); });

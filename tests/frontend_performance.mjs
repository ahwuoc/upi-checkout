// Offline regression check: node tests/frontend_performance.mjs
// No server, account tokens, payment requests, browser, or dependencies required.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

class Node {
  constructor() {
    this.children = [];
    this.refs = new Map();
    this.dataset = {};
    this.style = {};
    this.hidden = false;
    this.value = '';
    this.textContent = '';
    this.options = [];
    this.attributes = {};
    this.classList = { add() {}, remove() {}, toggle() {} };
  }
  set innerHTML(value) { this.html = value; this.children = []; this.refs.clear(); }
  get innerHTML() { return this.html || ''; }
  appendChild(node) {
    if (node.fragment) {
      for (const child of [...node.children]) this.appendChild(child);
    } else {
      node.remove();
      node.parent = this;
      this.children.push(node);
    }
    return node;
  }
  remove() {
    if (this.parent) {
      this.parent.children = this.parent.children.filter(child => child !== this);
      this.parent = null;
    }
  }
  querySelector(key) {
    if (!this.refs.has(key)) this.refs.set(key, new Node());
    return this.refs.get(key);
  }
  querySelectorAll() { return []; }
  setAttribute(key, value) { this.attributes[key] = value; }
  getAttribute(key) { return this.attributes[key]; }
  removeAttribute(key) { delete this.attributes[key]; }
  addEventListener() {}
  focus() {}
}

function dashboard() {
  const ids = new Map();
  const timers = new Map();
  const intervals = new Map();
  const requests = [];
  let nextTimer = 1;
  const context = vm.createContext({
    document: {
      body: new Node(),
      hidden: false,
      getElementById(id) {
        if (!ids.has(id)) ids.set(id, new Node());
        return ids.get(id);
      },
      createElement() { return new Node(); },
      createDocumentFragment() { const node = new Node(); node.fragment = true; return node; },
      querySelectorAll() { return []; },
      addEventListener() {},
    },
    localStorage: { getItem() { return null; }, setItem() {}, removeItem() {} },
    setTimeout(callback, delay) { const id = nextTimer++; timers.set(id, { callback, delay }); return id; },
    clearTimeout(id) { timers.delete(id); },
    setInterval(callback, delay) { const id = nextTimer++; intervals.set(id, { callback, delay }); return id; },
    clearInterval(id) { intervals.delete(id); },
    EventSource: class { close() { this.closed = true; } },
    fetch(url) { requests.push(url); return Promise.resolve({ ok: true, json: async () => ({ ok: true, status: 'waiting' }) }); },
    window: {}, navigator: {}, console, performance,
  });
  vm.runInContext(readFileSync(new URL('../web/static/app.js', import.meta.url), 'utf8'), context);
  vm.runInContext(`
    const metrics = { rows: 0, counts: 0, sorts: 0 };
    for (const [name, key] of [['renderRow', 'rows'], ['countByStatus', 'counts'], ['orderRows', 'sorts']]) {
      const original = globalThis[name];
      globalThis[name] = (...args) => { metrics[key]++; return original(...args); };
    }
    toast = () => {};
    playNotify = () => {};
    fillJobIdList = () => {};
    loadGlobalStats = () => {};
  `, context);
  return {
    context, ids, timers, intervals, requests,
    run(source) { return vm.runInContext(source, context); },
    flush() {
      for (const [id, timer] of [...timers]) {
        if (timer.delay <= 100) { timers.delete(id); timer.callback(); }
      }
    },
  };
}

const app = dashboard();
app.run(`
  state.jobId = '000000000001';
  state.total = 1000;
  state.running = true;
  for (let i = 0; i < 1000; i++) handleEvent({
    type: 'task_init', task_id: 'task-' + i, index: i,
    email: 'account' + i + '@example.test', steps: [{ key: 'work', label: 'Work' }]
  });
`);
app.flush();
app.run('metrics.rows = metrics.counts = metrics.sorts = 0;');
const start = performance.now();
app.run(`
  for (let update = 0; update < 5; update++) {
    for (let i = 0; i < 1000; i++) handleEvent({
      type: 'step', task_id: 'task-' + i, key: 'work',
      status: 'active', detail: 'progress ' + update
    });
  }
`);
app.flush();
const metrics = JSON.parse(app.run('JSON.stringify(metrics)'));
console.log('1000 tasks / 5000 step events:', metrics, (performance.now() - start).toFixed(1) + ' ms');
assert.ok(metrics.rows <= 1000, 'each dirty row should render at most once per event burst');
assert.ok(metrics.counts <= 2, 'status counters must not rescan every task on every step');
assert.ok(metrics.sorts <= 1, 'unchanged task order must not be sorted on every step');
assert.equal(app.ids.get('c-running').textContent.toString(), '1000');
assert.equal(app.run("tasks.get('task-999').steps[0].detail"), 'progress 4');
assert.equal(app.ids.get('task-list').children.length, 1000);
assert.equal(app.requests.length, 0, 'step replay must not make network requests');
console.log('PASS: batched rendering and final state');

// Detail-only progress on already-running tasks must not scan or sort the queue.
app.run(`
  metrics.rows = metrics.counts = metrics.sorts = 0;
  handleEvent({ type: 'step', task_id: 'task-0', key: 'work', status: 'active', detail: 'new detail' });
`);
app.flush();
assert.equal(app.run('metrics.rows'), 1);
assert.equal(app.run('metrics.counts'), 0);
assert.equal(app.run('metrics.sorts'), 0);

app.run(`
  handleEvent({ type: 'task_done', task_id: 'task-4', status: 'LINK', ok: true, done_at: 20 });
  handleEvent({ type: 'task_done', task_id: 'task-2', status: 'LINK', ok: true, done_at: 10 });
  handleEvent({ type: 'task_done', task_id: 'task-7', status: 'FAIL', ok: false });
  handleEvent({ type: 'task_done', task_id: 'task-8', status: 'STOPPED', ok: false });
  setFilter('success');
`);
app.flush();
const visibleIds = () => app.ids.get('task-list').children.filter(node => !node.hidden).map(node => node.dataset.taskId);
assert.deepEqual(visibleIds(), ['task-2', 'task-4']);
app.run(`state.search = 'account4'; updateCounters();`);
app.flush();
assert.deepEqual(visibleIds(), ['task-4']);
assert.equal(app.run("tasks.get('task-4').el.idx.textContent"), '#1');
assert.equal(app.ids.get('c-success').textContent.toString(), '2');
app.run(`state.search = ''; setFilter('stopped');`);
app.flush();
assert.deepEqual(visibleIds(), ['task-8']);
app.run(`handleEvent({ type: 'job_progress', total: 1100 });`);
app.flush();
assert.equal(app.run('state.total'), 1100);
console.log('PASS: status transitions, success order, search, numbering, append totals');

const snapshots = dashboard();
snapshots.run(`
  state.jobId = '000000000001';
  const fixture = {
    job_id: state.jobId, status: 'running', workers: 16, elapsed_ms: 120000, total: 1,
    tasks: [{ task_id: 'same', index: 0, email: 'same@example.test', status: 'done', raw_status: 'LINK', steps: [],
      artifact: { upi_link: 'https://payments.stripe.com/upi/instructions/fake' } }]
  };
  applySnapshot(fixture);
`);
snapshots.flush();
await new Promise(resolve => setImmediate(resolve));
snapshots.run(`
  const originalTask = tasks.get('same');
  const originalRow = originalTask.el.wrap;
  const originalMonitor = originalTask._monitor;
  originalTask.expanded = true;
  applySnapshot(fixture);
`);
snapshots.flush();
assert.equal(snapshots.run("tasks.get('same') === originalTask"), true);
assert.equal(snapshots.run("tasks.get('same').el.wrap === originalRow"), true);
assert.equal(snapshots.run("tasks.get('same').expanded"), true);
assert.equal(snapshots.run("tasks.get('same')._monitor === originalMonitor"), true);
assert.equal(snapshots.requests.length, 1, 'a repeated snapshot must not trigger duplicate probes');
assert.equal(snapshots.ids.get('runtime-workers').textContent, '16');
assert.equal(snapshots.ids.get('runtime-elapsed').textContent, '2m 00s');
snapshots.run(`
  handleEvent({ type: 'task_link', task_id: 'same', link_state: 'succeeded' });
`);
snapshots.flush();
assert.equal(snapshots.run("tasks.get('same')._monitor"), null);
assert.equal(snapshots.ids.get('btn-copy-emails').disabled, false);
snapshots.run(`handleEvent({ type: 'task_init', task_id: 'same', steps: [], run: 2 });`);
snapshots.flush();
assert.equal(snapshots.run("tasks.get('same').artifact"), null);
assert.equal(snapshots.run("tasks.get('same').linkState"), '');
assert.equal(snapshots.run("tasks.get('same').run"), 2);
snapshots.run(`applySnapshot({ ...fixture, tasks: [] });`);
snapshots.flush();
assert.equal(snapshots.ids.get('task-list').children.length, 0);
assert.equal(snapshots.intervals.size, 2, 'only the two dashboard clocks should remain');
console.log('PASS: snapshot row/probe reuse, runtime metrics, retry reset, monitor cleanup');

const probes = dashboard();
probes.run(`
  const pendingFetches = [];
  fetch = url => new Promise(resolve => pendingFetches.push({ url, resolve }));
  const shared = ensureTask('shared');
  shared.artifact = { upi_link: 'https://payments.stripe.com/upi/instructions/shared' };
  const sharedReads = Array.from({ length: 25 }, () => probeArtifact(shared, true));
`);
assert.equal(probes.run('pendingFetches.length'), 1, 'in-flight fresh reads must be deduplicated');
probes.run(`pendingFetches[0].resolve({ json: async () => ({ ok: true, status: 'waiting' }) });`);
await probes.run('Promise.all(sharedReads)');
probes.run(`
  const anotherFreshRead = probeArtifact(shared, true);
`);
assert.equal(probes.run('pendingFetches.length'), 2, 'fresh must not reuse a completed read');
probes.run(`pendingFetches[1].resolve({ json: async () => ({ ok: true, status: 'waiting' }) });`);
await probes.run('anotherFreshRead');
probes.run(`
  pendingFetches.length = 0;
  const bulkReads = [];
  for (let i = 0; i < 20; i++) {
    const t = ensureTask('probe-' + i);
    t.artifact = { upi_link: 'https://payments.stripe.com/upi/instructions/fake-' + i };
    bulkReads.push(probeArtifact(t, true));
  }
`);
assert.equal(probes.run('pendingFetches.length'), 6, 'at most six probe requests may start simultaneously');
assert.equal(probes.run('_probeQueue.length'), 14);
probes.run(`
  const removed = tasks.get('probe-0');
  tasks.delete('probe-0');
  detachMonitors();
  for (const request of pendingFetches) request.resolve({ json: async () => ({ ok: true, status: 'waiting' }) });
`);
await probes.run('Promise.all(bulkReads)');
await new Promise(resolve => setImmediate(resolve));
assert.equal(probes.run('removed.probe'), undefined, 'removed tasks must ignore late probe responses');
assert.equal(probes.run('_probeQueue.length'), 0);
assert.equal(probes.run('_activeProbes'), 0);
console.log('PASS: fresh probe deduplication, bounded concurrency, cancelled queue, stale-result guards');

const streams = dashboard();
streams.run(`attachJob('000000000001'); const firstConnection = state.es;`);
assert.equal(streams.requests.length, 0, 'attach must use the SSE snapshot without a redundant state GET');
streams.run(`firstConnection.onopen(); firstConnection.onerror(); firstConnection.onopen();`);
assert.equal(streams.requests.length, 0, 'reconnect must use the new SSE snapshot');
streams.run(`
  attachJob('000000000002');
  firstConnection.onmessage({ data: JSON.stringify({ type: 'job_start', job_id: '000000000001', total: 99 }) });
`);
assert.equal(streams.run('state.jobId'), '000000000002');
streams.run(`
  let resolveState;
  fetch = () => new Promise(resolve => { resolveState = resolve; });
  const oldRefresh = loadState(state.jobId);
  state.es.onmessage({ data: JSON.stringify({ type: 'job_progress', total: 20 }) });
  resolveState({ ok: true, json: async () => ({ job_id: state.jobId, total: 1, status: 'running', tasks: [] }) });
`);
await streams.run('oldRefresh');
assert.equal(streams.run('state.total'), 20, 'a late state GET must not overwrite newer stream events');
streams.run(`
  closeES();
  fetch = async () => ({ ok: true });
  const retry = retryTask({ task_id: 'retry-me' });
`);
await streams.run('retry');
assert.equal(streams.run('state.es !== null'), true, 'retry after job completion must reopen SSE');
console.log('PASS: single snapshot connection, reconnect, job switch, stale refresh, retry subscription');

const tokenSafety = dashboard();
tokenSafety.run(`
  state.jobId = '000000000001';
  state.tokens = ['token-from-unrelated-form'];
  applySnapshot({ job_id: state.jobId, tasks: [{ task_id: 'old', index: 0, status: 'done', raw_status: 'LINK' }] });
`);
tokenSafety.flush();
assert.equal(tokenSafety.run("tasks.get('old').token"), '');
assert.equal(tokenSafety.run("tasks.get('old').el.successCopyAt.disabled"), true);
tokenSafety.run(`
  state.tokensJobId = state.jobId;
  state.tokens = ['token-known-to-this-job'];
  handleEvent({ type: 'task_init', task_id: 'known', index: 0 });
`);
assert.equal(tokenSafety.run("tasks.get('known').token"), 'token-known-to-this-job');
tokenSafety.run(`
  attachJob('000000000002');
  handleEvent({ type: 'task_init', task_id: 'other', index: 0 });
`);
assert.equal(tokenSafety.run("tasks.get('other').token"), '');
console.log('PASS: Copy AT never uses tokens from an unrelated form or historical job');

for (const terminalViaStream of [true, false]) {
  const stopping = dashboard();
  stopping.run(`
    attachJob('000000000003');
    let resolveStopState;
    fetch = url => url.startsWith('/api/stop/')
      ? Promise.resolve({ ok: true })
      : new Promise(resolve => { resolveStopState = resolve; });
    const stoppingRequest = stopJob();
  `);
  await new Promise(resolve => setImmediate(resolve));
  if (terminalViaStream) stopping.run(`
    state.es.onmessage({ data: JSON.stringify({ type: 'job_done', job_id: state.jobId, status: 'stopped' }) });
  `);
  stopping.run(`
    resolveStopState({ ok: true, json: async () => ({
      job_id: state.jobId, status: '${terminalViaStream ? 'running' : 'stopped'}', stop_requested: true, tasks: []
    }) });
  `);
  await stopping.run('stoppingRequest');
  stopping.flush();
  assert.equal(stopping.run('state.running'), false,
    terminalViaStream ? 'late stop confirmation must not resurrect an SSE-completed job' : 'terminal stop snapshot must complete the UI');
  assert.equal(stopping.run('state.stopping'), false);
  assert.equal(stopping.ids.get('runtime-status').textContent, 'Đã dừng');
}
console.log('PASS: terminal stop state wins over late stop confirmation');

const restarted = dashboard();
restarted.run(`
  attachJob('000000000004');
  let resolveOldStop;
  fetch = url => url.startsWith('/api/stop/') ? Promise.resolve({ ok: true })
    : new Promise(resolve => { resolveOldStop = resolve; });
  const oldStop = stopJob();
`);
await new Promise(resolve => setImmediate(resolve));
restarted.run(`
  state.es.onmessage({ data: JSON.stringify({ type: 'job_start', job_id: state.jobId, total: 3 }) });
  resolveOldStop({ ok: true, json: async () => ({ job_id: state.jobId, status: 'stopped', total: 1, tasks: [] }) });
`);
await restarted.run('oldStop');
assert.equal(restarted.run('state.running'), true, 'stale terminal GET must not overwrite a newer restarted job');
assert.equal(restarted.run('state.stopping'), false);
assert.equal(restarted.run('state.total'), 3);
console.log('PASS: stale terminal stop snapshots cannot overwrite newer stream state');

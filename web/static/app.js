'use strict';

/* =====================================================================
 * UPI Checkout dashboard — plain JS, no framework, no build step.
 * Tasks live in a Map keyed by task_id; rows are created once and then
 * updated in place so a 200-task job stays responsive.
 * ===================================================================== */

/* ------------------------------ state ------------------------------ */

const state = {
  jobId: null,
  total: 0,
  mode: 'auto',
  view: 'list',
  running: false,
  stopping: false,
  starting: false, // đang POST /api/run (kể cả lúc server quét/lọc proxy) — chưa có job
  startingAt: 0,
  filter: 'all', // tab đang chọn: all | running | queued | success | fail
  search: '',
  workers: 0,
  retries: 0,   // số lần chạy tối đa mỗi acc (để hiện 'Run 3 / 3')
  elapsedMs: 0,
  elapsedAt: 0,
  jobStatus: '',
  connecting: false,
  streamRevision: 0,
  counts: null,
  tokens: [], // token order for matching each task result to its account
  tokensJobId: null, // only associate tokens with the job created from them in this tab
  es: null, // active EventSource, if any
};

// task_id -> task object. This is the single source of truth for tasks.
const tasks = new Map();

const MODE_HINTS = {
  auto: 'Detects the provider and picks the right flow for each account.',
  oaics: 'OAICS flow: confirmation_tokens → checkout/confirm → intent confirm.',
  cs: 'CS flow: payment_pages + approve (extract_cs).',
};

// Backend gửi 2 loại status khác nhau:
//   - task status (snapshot + task_done): pending | running | done | fail | stopped
//   - raw status của luồng (task_done.status / snapshot.raw_status): LINK | FAIL |
//     TIMEOUT | error | no_promo | taxes_fail | ct_fail | ...
// CHỈ 'LINK' là thành công (có link/QR để quét). Mọi raw status khác đều là thất bại.
// Trần token mỗi batch — phải khớp MAX_TOKENS_PER_BATCH bên web/engine.py.
const MAX_TOKENS = 1000;
// Trần worker — phải khớp validation trong web/app.py (server là nơi phán cuối).
const MAX_WORKERS = 200;

// Số nhiều tiếng Anh: 1 task / 2 tasks. Dùng cho mọi chuỗi có đếm.
function plural(n) { return n === 1 ? '' : 's'; }

const QUEUE_FILTERS = [
  { key: 'all', label: 'All' },
  { key: 'running', label: 'Running' },
  { key: 'queued', label: 'Queued' },
  { key: 'success', label: 'Success' },
  { key: 'fail', label: 'Failed' },
  { key: 'stopped', label: 'Stopped' },
];

// Inline SVG icons for step states (no icon library, works offline).
const STEP_ICONS = {
  pending: '<svg viewBox="0 0 24 24" width="15" height="15" aria-hidden="true"><circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" stroke-width="2"/></svg>',
  active: '<svg class="spin" viewBox="0 0 24 24" width="15" height="15" aria-hidden="true"><circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" stroke-width="2.5" stroke-dasharray="42 16" stroke-linecap="round"/></svg>',
  done: '<svg viewBox="0 0 24 24" width="15" height="15" aria-hidden="true"><circle cx="12" cy="12" r="11" fill="#22c55e"/><path d="M7 12.5l3 3 7-7" fill="none" stroke="#fff" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/></svg>',
  fail: '<svg viewBox="0 0 24 24" width="15" height="15" aria-hidden="true"><circle cx="12" cy="12" r="11" fill="#ef4444"/><path d="M9 9l6 6M15 9l-6 6" fill="none" stroke="#fff" stroke-width="2.5" stroke-linecap="round"/></svg>',
  skip: '<svg viewBox="0 0 24 24" width="15" height="15" aria-hidden="true"><circle cx="12" cy="12" r="9" fill="none" stroke="currentColor" stroke-width="2" stroke-dasharray="4 4"/></svg>',
};

/* --------------------------- DOM handles --------------------------- */

const els = {
  jobIdInput: document.getElementById('job-id-input'),
  jobIdLoad: document.getElementById('job-id-load'),
  jobIdList: document.getElementById('job-id-list'),
  overallCount: document.getElementById('overall-count'),
  btnSuccess: document.getElementById('btn-success'),
  btnRefresh: document.getElementById('btn-refresh'),
  pushQueue: document.getElementById('push-queue'),
  pushQueueCount: document.getElementById('push-queue-count'),
  btnCopyEmails: document.getElementById('btn-copy-emails'),
  btnCopyEmailsLabel: document.getElementById('btn-copy-emails-label'),
  btnClearTab: document.getElementById('btn-clear-tab'),
  btnClearTabLabel: document.getElementById('btn-clear-tab-label'),
  btnNotify: document.getElementById('btn-notify'),
  btnNotifyLabel: document.getElementById('btn-notify-label'),
  notifyAudio: document.getElementById('notify-audio'),
  modal: document.getElementById('modal'),
  modalTitle: document.getElementById('modal-title'),
  modalText: document.getElementById('modal-text'),
  modalOk: document.getElementById('modal-ok'),
  modalCancel: document.getElementById('modal-cancel'),
  listEmpty: document.getElementById('list-empty'),
  queueLoading: document.getElementById('queue-loading'),
  queueLoadingText: document.getElementById('queue-loading-text'),
  queueLoadingFill: document.getElementById('queue-loading-fill'),
  tabCounts: {
    all: document.getElementById('c-all'),
    running: document.getElementById('c-running'),
    queued: document.getElementById('c-queued'),
    success: document.getElementById('c-success'),
    fail: document.getElementById('c-fail'),
    stopped: document.getElementById('c-stopped'),
  },
  overallBar: document.getElementById('overall-bar'),
  overallFill: document.getElementById('overall-fill'),
  overallPct: document.getElementById('overall-pct'),
  tokens: document.getElementById('tokens'),
  tokenCount: document.getElementById('token-count'),
  modeHint: document.getElementById('mode-hint'),
  useProxy: document.getElementById('use-proxy'),
  proxies: document.getElementById('proxies'),
  poolStatus: document.getElementById('pool-status'),
  poolCount: document.getElementById('pool-count'),
  poolClear: document.getElementById('pool-clear'),
  country: document.getElementById('country'),
  promo: document.getElementById('promo'),
  workers: document.getElementById('workers'),
  retries: document.getElementById('retries'),
  summaryTasks: document.getElementById('summary-tasks'),
  summaryTotal: document.getElementById('summary-total'),
  warning: document.getElementById('warning'),
  submit: document.getElementById('submit'),
  stopNote: document.getElementById('stop-note'),
  gstatsCodes: document.getElementById('gstats-codes'),
  gstatsMeta: document.getElementById('gstats-meta'),
  gstatsChart: document.getElementById('gstats-chart'),
  historyBox: document.getElementById('history-box'),
  historyList: document.getElementById('history-list'),
  historyCount: document.getElementById('history-count'),
  emptyState: document.getElementById('empty-state'),
  taskList: document.getElementById('task-list'),
  taskSearch: document.getElementById('task-search'),
  runtimeRunning: document.getElementById('runtime-running'),
  runtimeQueued: document.getElementById('runtime-queued'),
  runtimeWorkers: document.getElementById('runtime-workers'),
  runtimeElapsed: document.getElementById('runtime-elapsed'),
  runtimeThroughput: document.getElementById('runtime-throughput'),
  runtimeStatus: document.getElementById('runtime-status'),
  toasts: document.getElementById('toasts'),
};

/* ---------------------------- helpers ------------------------------ */

// Escape untrusted text for safe insertion into innerHTML (covers attribute context too).
function esc(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// Only allow http(s) URLs in href/src to block javascript: etc.
function safeUrl(u) {
  if (typeof u !== 'string') return '#';
  const v = u.trim();
  return /^https?:\/\//i.test(v) ? v : '#';
}

function fmtDuration(ms) {
  if (ms == null || isNaN(ms) || ms < 0) return '—';
  const totalSec = Math.round(ms / 1000);
  if (totalSec < 60) return totalSec + 's';
  return Math.floor(totalSec / 60) + 'm ' + (totalSec % 60) + 's';
}

function fmtAmount(minor) {
  if (minor == null || isNaN(minor)) return null;
  const major = minor / 100;
  return '₹' + major.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

/* --------------------------- task model ---------------------------- */

function newTask(taskId) {
  return {
    task_id: taskId,
    index: 0,
    email: '',
    token: '',
    flow: null,
    flow_label: null,
    steps: [],              // [{key,label,status,detail,err}]
    status: 'pending',      // pending | running | success | fail
    rawStatus: null,        // raw backend terminal status (LINK/FAIL/...)
    error: null,
    duration_ms: null,
    run: 1,
    egress: null,           // {ip, location, probed_at}
    artifact: null,         // {upi_link, qr_png, qr_svg, amount_minor, intent}
    // Acc đã lên Plus chưa. Server chỉ bắt đầu dò khi khách DUYỆT MANDATE
    // (link_state = succeeded), rồi dò lại vài lần vì khách còn phải trả tiền.
    plusState: '',          // '' | checking | plus | not_plus
    plusNote: '',           // chi tiết thô: "AT 401 · promo active"
    plusCheckedAt: 0,
    doneAt: 0,              // epoch lúc task xong (server gửi) -> sắp tab Success
    created_at: null,
    updated_at: null,
    logs: [],
    el: null,               // cached DOM refs
    expanded: false,
  };
}

function normalizeStatus(taskStatus, rawStatus) {
  const ts = String(taskStatus || '').toLowerCase();
  const rs = String(rawStatus || '').trim().toUpperCase();
  if (ts === 'running') return 'running';
  if (rs === 'LINK') return 'success';
  // Dừng theo yêu cầu KHÔNG phải thất bại. Task bị kill giữa chừng (raw STOPPED)
  // và task chưa hề chạy (status stopped) đều là "đã dừng". Trước đây gộp cả vào
  // 'fail' nên 92 task chưa chạy bị tô đỏ "Thất bại" — nhìn như 92 account lỗi.
  if (ts === 'stopped' || rs === 'STOPPED') return 'stopped';
  // done/fail la trang thai task da ket thuc
  if (ts === 'done' || ts === 'fail') return 'fail';
  // co raw status nhung khong phai LINK -> luong da chay va that bai
  if (rs) return 'fail';
  return 'pending';
}

// Done-or-failed steps / total steps.
function taskProgress(t) {
  if (!t.steps || !t.steps.length) return 0;
  let finished = 0;
  for (const s of t.steps) {
    if (s.status === 'done' || s.status === 'fail') finished++;
  }
  return Math.round((finished / t.steps.length) * 100);
}

// Ly do that bai: err that -> rawStatus -> dong log cuoi -> "(không có thông tin)".
// Backend co the tra ve error rong, nen phai co fallback de khong hien task fail tron khong.
function failReason(t) {
  if (t.error) return t.error;
  if (t.rawStatus && t.rawStatus !== 'UNKNOWN') return t.rawStatus;
  const logs = t.logs || [];
  const lastLog = [...logs].reverse().find(l => l && l.trim());
  if (lastLog) return 'no error detail | last log: ' + lastLog.trim().slice(0, 160);
  return t.rawStatus ? t.rawStatus + ' (no detail)' : 'no info';
}

// AT 的 JWT 有 1~2 KB，整条铺在卡片里没法看。只留首尾做识别。
function maskToken(token) {
  const t = String(token || '');
  if (!t) return '—';
  if (t.length <= 26) return t;
  return t.slice(0, 12) + '…' + t.slice(-8) + '  (' + t.length + ')';
}

// Xoá task của một tab. Ưu tiên xoá ở server trước —— 只删 DOM 的话，
// F5 一下整批又回来了，比没有这个按钮更让人困惑。
async function clearTab(scope, btn) {
  const label = (QUEUE_FILTERS.find(f => f.key === scope) || {}).label || scope;
  const c = countByStatus();
  const n = scope === 'all' ? tasks.size : (c[scope] || 0);
  if (!n) { toast('"' + label + '" is empty'); return; }
  const okClear = await confirmDialog({
    title: 'Remove ' + n + ' tasks from "' + label + '"?',
    text: 'The server copy is deleted too.\nThis cannot be undone.',
    okLabel: 'Remove ' + n + ' tasks',
  });
  if (!okClear) return;

  if (btn) btn.classList.add('busy');
  try {
    if (state.jobId) {
      const res = await fetch('/api/clear/' + state.jobId, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ scope }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) { toast(data.detail || 'Could not remove'); return; }
    }
    // 服务端删完再删本地，避免中间态
    for (const [tid, t] of [...tasks.entries()]) {
      const bucket = t.status === 'running' ? 'running'
                   : t.status === 'success' ? 'success'
                   : t.status === 'fail' ? 'fail'
                   : t.status === 'stopped' ? 'stopped' : 'queued';
      if (scope === 'all' || bucket === scope) {
        if (t.el && t.el.wrap) t.el.wrap.remove();
        if (t._monitor) { clearInterval(t._monitor); t._monitor = null; }
        tasks.delete(tid);
      }
    }
    updateCounters();
    toast('Removed ' + n + ' tasks from "' + label + '"');
  } catch (err) {
    toast('Remove failed: ' + err.message);
  } finally {
    if (btn) btn.classList.remove('busy');
  }
}

function rowStatusText(t) {
  // 停止期间要让用户看见「停止真的在做事情」。以前只把按钮变灰，
  // 任务行照旧写着「Đang chạy…」，看着就像没反应。
  // 现在 engine 会真的 kill 掉子进程，通常 1~2 秒内这些行就会翻成 Fail。
  if (state.stopping) {
    if (t.status === 'running') return '⏹ Cancelling…';
    if (t.status === 'pending') return '⏹ Waiting to cancel';
  }
  if (t.status === 'stopped') {
    // Phân biệt rõ: bị kill giữa chừng (token đã dùng một phần) vs chưa hề chạy
    // (token còn nguyên, chạy lại được) — trước đây cả hai đều ghi "Fail".
    const raw = String(t.rawStatus || '').toUpperCase();
    return raw === 'STOPPED'
      ? 'Stopped mid-run — the token may be partially used'
      : 'Never ran — job stopped before its turn (token intact)';
  }
  if (t.status === 'success') return 'QR link created';
  if (t.status === 'fail') return 'Failed: ' + failReason(t);
  if (t.status === 'running') return 'Running…';
  // Vị trí trong queue = số thứ tự dòng token, để biết task này là token thứ mấy
  // trong 100 dòng đã dán (trước đây chỉ ghi "Chờ xử lý", không biết đang ở đâu).
  const total = state.total || tasks.size;
  if (total > 0 && t.index != null) return 'Queued · ' + (t.index + 1) + '/' + total;
  return 'Queued';
}

// Chip nói kết quả **thật** của link, không phải "máy đã trích xuất xong".
// Trích xuất xong mới chỉ là có link QR; tiền/uỷ nhiệm chưa về thì không được
// xanh — trước đây chip luôn xanh "Thành công" nên nhìn như đã thu được tiền,
// trong khi thực tế khách chưa quét.
const TERMINAL = ['succeeded', 'failed', 'canceled', 'expired'];

// Trạng thái HIỆU DỤNG của link. Hai nguồn nói về cùng một link:
//   • `t.linkState` — server dò ở nền (engine._link_monitor_loop) và đẩy xuống,
//     còn nguyên trong snapshot sau F5;
//   • `t.probe.status` — lần đọc trang instructions gần nhất của chính tab này.
// Probe có thể CŨ HƠN server: request bắt đầu lúc khách chưa quét, trả về sau khi
// server đã thấy `succeeded` —apply nó là kéo card về "đang chờ" (đúng bug 2 card
// augers.fonder / clinger_alpacas hiện "expired" dù uỷ nhiệm đã thành công).
// Nên: chỉ khi server đã kết luận CUỐI mà probe chưa, lấy server.
function effStatus(t, p) {
  p = p || t.probe || {};
  const srv = t.linkState || '';
  if (!TERMINAL.includes(p.status) && TERMINAL.includes(srv)) return srv;
  return p.status || '';
}

function chipFor(t) {
  if (t.status === 'running') return ['Running', 'chip-blue'];
  if (t.status === 'stopped') return ['Stopped', 'chip-gray'];
  if (t.status === 'fail') return ['Failed', 'chip-red'];
  if (t.status !== 'success') return ['Queued', 'chip-gray'];
  const p = t.probe || {};
  const st = effStatus(t, p);
  if (!p.ok && !st) return ['Awaiting payment', 'chip-amber'];   // chưa dò được -> vẫn coi là chưa xong
  const byProbe = {
    succeeded: ['Customer approved', 'chip-green'],
    failed: ['Stripe declined', 'chip-red'],
    canceled: ['Cancelled', 'chip-red'],
    expired: ['Expired', 'chip-amber'],
    waiting: ['Awaiting payment', 'chip-amber'],
  };
  return byProbe[st] || ['Awaiting payment', 'chip-amber'];
}

// Nhãn chip trạng thái ở hàng chân card (chế độ lưới — kiểu card gọn). Ngắn, khớp
// cách gọi trong bản thiết kế: "Payment successful" / "Awaiting payment".
const CHIP_STATE = {
  succeeded: ['Payment successful', 'chip-green'],
  waiting: ['Awaiting payment', 'chip-amber'],
  failed: ['Failed', 'chip-red'],
  canceled: ['Cancelled', 'chip-red'],
  expired: ['Expired', 'chip-amber'],
};
// Nhãn chip Plus ở cùng hàng. Chưa duyệt mandate thì KHÔNG có chip này (đúng như
// thiết kế: card "Awaiting payment" không hiện gì về Plus).
const CHIP_PLUS = {
  plus: ['Plus Success', 'chip-green'],
  checking: ['Checking Plus…', 'chip-amber'],
  not_plus: ['Not Plus', 'chip-gray'],
};

function setChip(t) {
  if (!t.el) return;
  const [label, cls] = chipFor(t);
  if (t.el.chip) {
    t.el.chip.textContent = label;
    t.el.chip.className = 'chip ' + cls;
  }
  // chip trên card (chế độ lưới) giữ chữ "Link đã tạo" như cũ, chỉ đổi **màu**
  // theo tình trạng link — chép y chữ của chip hàng thì 1 card có 2 chip giống hệt.
  if (t.el.successCardStatus) {
    t.el.successCardStatus.textContent = 'Link created';
    t.el.successCardStatus.className = 'success-card-status chip ' + cls;
  }
  // Chip ở hàng chân card (chế độ lưới) thì nói thẳng TRẠNG THÁI LINK — trùng ý với
  // chip hàng nên chỉ hiện ở chế độ lưới (CSS ẩn ở danh sách).
  if (t.el.footState) {
    const [stateLabel, stateCls] = CHIP_STATE[t.linkState || ''] || ['Link created', cls];
    t.el.footState.textContent = stateLabel;
    t.el.footState.className = 'chip success-chip-state ' + stateCls;
  }
}

function chipHtml(t) {
  const [label, cls] = chipFor(t);
  return '<span class="chip ' + cls + '">' + label + '</span>';
}

function stepIcon(status) {
  return STEP_ICONS[status] || STEP_ICONS.pending;
}

function stepHtml(s) {
  const cls = 'step step-' + (s.status || 'pending');
  const detailText = s.err || s.detail;
  const detail = detailText ? '<div class="step-detail muted">' + esc(detailText) + '</div>' : '';
  // Bước bị BỎ QUA (không chạy) -> ghi rõ "(Skipped)" như tool tham khảo, thay vì
  // hiện ✓ khiến tưởng nó đã gọi API.
  const label = s.status === 'skip'
    ? '<div class="step-label muted strike">' + esc(s.label) + ' (Skipped)</div>'
    : '<div class="step-label">' + esc(s.label) + '</div>';
  return '<div class="' + cls + '"><span class="step-icon">' + stepIcon(s.status) + '</span>' +
    '<div class="step-text">' + label + detail + '</div></div>';
}

function flowBadgeHtml(t) {
  if (t.flow === 'oaics') return '<span class="flow-badge flow-oaics">FLOW OAICS</span>';
  if (t.flow === 'cs') return '<span class="flow-badge flow-cs">FLOW CS</span>';
  if (t.status === 'running') return '<span class="flow-badge flow-detect">DETECTING FLOW</span>';
  return '<span class="flow-badge flow-none">—</span>';
}

function flowCardHtml(t) {
  let html =
    '<div class="flow-card">' +
      '<div class="flow-card-title">Backend flow</div>' +
      '<div class="flow-badge-row"><span class="muted">Backend payment</span> ' + flowBadgeHtml(t) + '</div>';
  if (t.egress) {
    const e = t.egress;
    const ex = e.exit || {};
    const bl = e.billing || {};
    html += '<div class="egress"><div class="egress-title muted">Proxy egress</div>';
    if (ex.ip || e.ip) {
      html += '<div class="egress-row"><span>Exit IP</span><span class="mono">' +
              esc(ex.ip || e.ip || '') + (ex.country ? ' <span class="muted">(' + esc(ex.country) + ')</span>' : '') +
              '</span></div>';
      const loc = fmtGeo(ex) || e.location || '';
      if (loc) html += '<div class="egress-row"><span>Exit location</span><span>' + esc(loc) + '</span></div>';
    }
    if (bl.ip) {
      html += '<div class="egress-row"><span>Billing IP</span><span class="mono">' +
              esc(bl.ip) + (e.billing_country ? ' <span class="muted">(' + esc(e.billing_country) + ')</span>' : '') +
              (e.same_ip ? ' <span class="muted">— same exit</span>' : '') +
              '</span></div>';
      const bloc = fmtGeo(bl);
      if (bloc) html += '<div class="egress-row"><span>Billing location</span><span>' + esc(bloc) + '</span></div>';
    }
    if (e.billing_geo && e.billing_geo.location) {
      html += '<div class="egress-row"><span>Billing geo</span><span>' +
              esc(e.billing_geo.location) + (e.billing_geo.approx ? ' <span class="muted">(approx)</span>' : '') +
              '</span></div>';
    }
    if (ex.risk != null || ex.verdict) {
      // ippure chấm IP exit: risk (fraudScore 0..100) + verdict + user_type
      // (residential vs non-residential). Có từ probe_egress() ở cli.py.
      const bits = [];
      if (ex.risk != null) bits.push('risk ' + ex.risk);
      if (ex.verdict) bits.push(ex.verdict);
      if (ex.user_type) bits.push(ex.user_type);
      if (ex.isp) bits.push(ex.isp);
      if (ex.latency_ms != null) bits.push(ex.latency_ms + 'ms');
      html += '<div class="egress-row"><span>IP quality</span><span>' +
              esc(bits.join(' · ')) + '</span></div>';
    }
    if (e.probed_at) {
      html += '<div class="egress-row"><span>Probed at</span><span class="mono">' + esc(e.probed_at) + '</span></div>';
    }
    html += '</div>';
  }
  return html + '</div>';
}

function artifactHtml(t) {
  const a = t.artifact;
  if (!a) return '';
  let html = '<div class="artifact"><div class="artifact-title">UPI artifact</div>';
  if (a.upi_link) {
    html += '<a class="upi-link mono" href="' + esc(safeUrl(a.upi_link)) + '" target="_blank" rel="noopener noreferrer" title="' + esc(a.upi_link) + '">' + esc(a.upi_link) + '</a>';
  }
  if (a.amount_minor != null) {
    // Ghi rõ đây là số CHỐT LÚC TRÍCH XUẤT — card ở trên hiện số LIVE của trang
    // Stripe, hai số có thể khác nhau (ví dụ promo áp được ở lần chạy sau).
    html += '<div class="amount" title="Amount recorded when this link was extracted">'
      + esc(fmtAmount(a.amount_minor)) + '</div>';
  }
  // QR là ảnh dùng-một-lần và Stripe XOÁ nó khi link không còn dùng được: đo thật
  // thấy cả succeeded lẫn failed đều trả HTTP 410 Gone -> render ảnh chỉ còn icon vỡ.
  // Chỉ hiện khi link chưa có kết luận cuối (khách còn có thể quét).
  const qrUsable = a.qr_png && !TERMINAL.includes(t.linkState);
  if (qrUsable) {
    html +=
      '<div class="qr-wrap"><img class="qr" src="' + esc(safeUrl(a.qr_png)) + '" alt="QR UPI" loading="lazy"></div>' +
      '<a class="qr-download" href="' + esc(safeUrl(a.qr_png)) + '" download>⭳ Download QR</a>';
  }
  return html + '</div>';
}

/* --------------------------- rendering ----------------------------- */

// Apply events to the model immediately, then paint each changed row once.
// A short timer also works in background tabs where animation frames pause.
const dirtyTasks = new Set();
let renderTimer = null;
let countersDirty = false;
let orderDirty = false;

function scheduleRender() {
  if (renderTimer === null) renderTimer = setTimeout(flushRender, 32);
}

function queueTaskRender(t, countersChanged = false, orderChanged = false) {
  dirtyTasks.add(t.task_id);
  countersDirty = countersDirty || countersChanged;
  orderDirty = orderDirty || orderChanged;
  scheduleRender();
}

function flushRender() {
  if (renderTimer !== null) clearTimeout(renderTimer);
  renderTimer = null;
  const fragment = document.createDocumentFragment();
  for (const id of dirtyTasks) {
    const t = tasks.get(id);
    if (!t) continue;
    if (!t.el) fragment.appendChild(createTaskEl(t).wrap);
    renderRow(t);
    renderDetailIfOpen(t);
  }
  dirtyTasks.clear();
  if (fragment.children.length) els.taskList.appendChild(fragment);
  if (countersDirty) {
    countersDirty = false;
    renderCounters();
  }
}

// Build a task's row + detail container once and cache the row refs.
// Icon copy (dùng cho 2 nút ở chế độ lưới). Inline SVG cho khớp style các nút khác.
const CARD_ICON = '<svg viewBox="0 0 24 24" aria-hidden="true">'
  + '<rect x="9" y="9" width="11" height="11" rx="2"/>'
  + '<path d="M5 15V5.5A1.5 1.5 0 0 1 6.5 4H15"/></svg>';
// Icon cho hàng chân card ở chế độ lưới (xem .success-footer).
const LINK_ICON = '<svg viewBox="0 0 24 24" aria-hidden="true">'
  + '<path d="M10.5 13.5a4 4 0 0 0 5.7 0l2.6-2.6a4 4 0 0 0-5.7-5.7l-1.1 1.1"/>'
  + '<path d="M13.5 10.5a4 4 0 0 0-5.7 0l-2.6 2.6a4 4 0 0 0 5.7 5.7l1.1-1.1"/></svg>';
const OPEN_ICON = '<svg viewBox="0 0 24 24" aria-hidden="true">'
  + '<path d="M14 4h6v6"/><path d="M20 4l-8 8"/>'
  + '<path d="M18 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h5"/></svg>';

function createTaskEl(t) {
  const wrap = document.createElement('div');
  wrap.className = 'task';
  wrap.dataset.taskId = t.task_id;

  const row = document.createElement('div');
  row.className = 'task-row';
  row.innerHTML =
    '<div class="task-row-top">' +
      // Số thứ tự = dòng token trong ô dán (1-based), không phải thứ tự hiển thị —
      // để đối chiếu kết quả về đúng dòng AT nào.
      '<span class="task-idx mono"></span>' +
      '<button type="button" class="task-email" title="Copy email"></button>' +
      // Số tiền nằm ở header CHỈ trong chế độ lưới (card gọn kiểu "UPI · ₹0.00") —
      // ngoài ra ẩn, vì chế độ danh sách đã có ô Amount riêng.
      '<span class="task-amount mono" hidden></span>' +
      '<span class="task-pct"></span>' +
      '<span class="chip chip-gray"></span>' +
      '<button type="button" class="task-remove" aria-label="Remove task" title="Remove this task (also deletes the server copy)">✕</button>' +
      '<button type="button" class="task-toggle" aria-expanded="false" aria-label="View task details" title="View task details"></button>' +
    '</div>' +
    '<div class="task-row-status muted"></div>' +
    '<div class="task-row-egress mono muted"></div>' +
    '<div class="task-bar" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0" aria-label="Progress">' +
      '<div class="task-bar-fill"></div>' +
    '</div>';

  const detail = document.createElement('div');
  detail.className = 'task-detail';
  detail.id = 'task-detail-' + t.task_id;
  detail.hidden = true;
  row.querySelector('.task-toggle').setAttribute('aria-controls', detail.id);

  const successResult = document.createElement('div');
  successResult.className = 'success-result';
  successResult.hidden = true;
  successResult.innerHTML =
    '<span class="success-card-status chip chip-green">Link created</span>' +
    '<div class="success-fields">' +
      '<div class="success-field"><span class="success-label">Email</span><strong class="success-email"></strong></div>' +
      '<div class="success-field"><span class="success-label">Access token</span>' +
        '<span class="success-tokenrow">' +
          '<code class="success-token mono"></code>' +
          '<button type="button" class="success-copyat" title="Copy the full access token">Copy AT</button>' +
        '</span></div>' +
      '<div class="success-field"><span class="success-label">Payment link</span>' +
        '<span class="success-linkrow">' +
          '<a class="success-link mono" target="_blank" rel="noopener noreferrer" title="Click to copy · Ctrl/Cmd-click or double-click to open"></a>' +
          '<button type="button" class="success-open" title="Open in a new tab">Open ↗</button>' +
        '</span></div>' +
      '<div class="success-field success-amount-field" hidden><span class="success-label">Amount</span><strong class="success-amount"></strong><span class="success-expiry"></span><span class="success-verdict"></span></div>' +
      // Kết luận acc đã lên Plus chưa — chỉ hiện khi server đã bắt đầu dò (tức khách
      // đã duyệt mandate). Kết luận lấy từ chính AT của acc (xem at_check.py).
      '<div class="success-field success-plus-field" hidden><span class="success-label">Upgrade</span><strong class="success-plus"></strong></div>' +
      // Link Stripe (return_url của session) — hiện NGAY DƯỚI link UPI, chiếm cả
      // 2 cột. Dùng lại class .success-link/.success-open nên bấm-copy và
      // bấm-đúp-mở hoạt động sẵn, không cần handler mới.
      '<div class="success-field success-stripe-field" hidden>' +
        '<span class="success-label">Stripe checkout</span>' +
        '<span class="success-linkrow">' +
          '<a class="success-link success-stripe mono" target="_blank" rel="noopener noreferrer" title="Click to copy · Ctrl/Cmd-click or double-click to open"></a>' +
          '<button type="button" class="success-open" title="Open in a new tab">Open ↗</button>' +
        '</span></div>' +
      // Chế độ lưới chỉ giữ MỘT nút copy: AT. Link UPI đã bấm-là-copy ngay trên
      // chính nó, còn QR thì bấm vào là mở/tải ảnh — thêm nút copy cho chúng là thừa.
      // Link checkout Stripe (pay.openai.com/c/pay/…, 500-700 ký tự) không hiện và
      // không có nút Open; vẫn tra/copy được ở chế độ danh sách + drawer.
      '<div class="card-tools">' +
        '<button type="button" class="card-tool card-copyat" title="Copy access token">' +
          CARD_ICON + '<span>AT</span></button>' +
      '</div>' +
      // Hàng chân card cho chế độ lưới (card gọn): chip trạng thái + chip Plus +
      // đếm ngược + nút icon. Ẩn hoàn toàn ở chế độ danh sách — ở đó thông tin
      // đã có dạng field có nhãn.
      '<div class="success-footer">' +
        '<span class="chip chip-gray success-chip-state">Link created</span>' +
        '<span class="chip chip-blue success-chip-plus" hidden></span>' +
        '<span class="success-foot-expiry"></span>' +
        '<span class="success-foot-tools">' +
          '<button type="button" class="card-tool card-copylink" title="Copy payment link">' +
            LINK_ICON + '<span></span></button>' +
          '<button type="button" class="card-tool card-openlink" title="Open payment link">' +
            OPEN_ICON + '<span></span></button>' +
          '<button type="button" class="card-tool card-copyat" title="Copy access token">' +
            CARD_ICON + '<span></span></button>' +
        '</span>' +
      '</div>' +
    '</div>' +
    '<a class="success-qr-link" target="_blank" rel="noopener noreferrer" download>' +
      '<img class="success-qr" alt="QR UPI" loading="lazy">' +
      '<span>Download QR</span>' +
    '</a>';

  wrap.appendChild(row);
  wrap.appendChild(successResult);
  wrap.appendChild(detail);

  // Copy and remove have their own native buttons; only the summary toggles details.
  row.addEventListener('click', (ev) => {
    if (ev.target.closest('.task-email, .detail-email')) return;
    toggleDetail(t);
  });

  // ✕ trên từng task: chặn nổi bọt, không thì vừa xoá vừa mở/đóng detail
  const rm = row.querySelector('.task-remove');
  if (rm) {
    rm.addEventListener('click', (ev) => {
      ev.stopPropagation();
      ev.preventDefault();
      removeTask(t);
    });
  }

  t.el = {
    wrap,
    row,
    toggle: row.querySelector('.task-toggle'),
    detail,
    successResult,
    successEmail: successResult.querySelector('.success-email'),
    successToken: successResult.querySelector('.success-token'),
    successCopyAt: successResult.querySelector('.success-copyat'),
    cardCopyAt: successResult.querySelector('.card-copyat'),
    successLink: successResult.querySelector('.success-link'),
    successAmountField: successResult.querySelector('.success-amount-field'),
    successAmount: successResult.querySelector('.success-amount'),
    successExpiry: successResult.querySelector('.success-expiry'),
    successVerdict: successResult.querySelector('.success-verdict'),
    successPlusField: successResult.querySelector('.success-plus-field'),
    successPlus: successResult.querySelector('.success-plus'),
    successCardStatus: successResult.querySelector('.success-card-status'),
    taskAmount: row.querySelector('.task-amount'),
    footState: successResult.querySelector('.success-chip-state'),
    footPlus: successResult.querySelector('.success-chip-plus'),
    footExpiry: successResult.querySelector('.success-foot-expiry'),
    successStripeField: successResult.querySelector('.success-stripe-field'),
    successStripe: successResult.querySelector('.success-stripe'),
    successQrLink: successResult.querySelector('.success-qr-link'),
    successQr: successResult.querySelector('.success-qr'),
    email: row.querySelector('.task-email'),
    idx: row.querySelector('.task-idx'),
    pct: row.querySelector('.task-pct'),
    chip: row.querySelector('.chip'),
    remove: row.querySelector('.task-remove'),
    status: row.querySelector('.task-row-status'),
    egress: row.querySelector('.task-row-egress'),
    bar: row.querySelector('.task-bar'),
    barFill: row.querySelector('.task-bar-fill'),
  };

  // Tra ve chinh object refs (KHONG phai `wrap`) de
  // `t.el = createTaskEl(t)` roi `t.el.wrap` hoat dong dung.
  // `renumber()` duyệt thẳng children của #task-list, cần với tới badge số thứ tự
  // mà không phải querySelector lại từng row (1000 row × mỗi lần update = chậm).
  wrap._idxEl = t.el.idx;
  return t.el;
}

// ---- 指引页监视：金额 / 倒计时 / 状态 --------------------------------
// 落盘的 artifact 里 amount_minor 和 expires_at 都是 null（cs 流程不写），
// 只能现抓 https://payments.stripe.com/upi/instructions/... 解出来。
//
// 注意这是**监视**语义，不是 zero-link 的「交付前核验」：
// 那边 succeeded 表示「链已被用掉，别重复交付」，
// 这里 succeeded 是**最好的结果** —— 客户成功签了委托。判据混用会把成功链报成死链。
const STATUS_UI = {
  waiting: ['⏳ Waiting for customer to scan', 'wait'],
  succeeded: ['Customer approved the mandate ✓', 'ok'],
  failed: ['❌ Failed — Stripe declined', 'bad'],
  canceled: ['⛔ Cancelled', 'bad'],
  expired: ['⌛ Expired — never scanned', 'warn'],
  unknown: ['❔ Unknown state', 'warn'],
};

// "waiting" gộp 4 state khác nhau của intent, và chúng KHÁC NHAU về việc khách đã
// làm gì: requires_action = chưa quét (không có tiền), processing = khách quét rồi
// và Stripe đang xử lý. Ghi chung một câu "đang chờ khách duyệt" là nói sai một nửa.
const WAIT_DETAIL = {
  requires_action: 'Waiting for customer to scan',
  requires_confirmation: 'Waiting for customer to confirm in their UPI app',
  processing: 'Scanned — Stripe processing',
  requires_capture: 'Mandate set — awaiting capture',
};

function verdictFor(p) {
  if (p.status === 'waiting') {
    return ['⏳ ' + (WAIT_DETAIL[p.intent_state] || 'Waiting for customer to scan'), 'wait'];
  }
  return STATUS_UI[p.status] || STATUS_UI.unknown;
}
const MONITOR_MS = Number(localStorage.getItem('upi-monitor-ms')) || 120000;   // lưới an toàn; server dò mỗi 30s và đẩy xuống
const _probeCache = new Map();

// Luôn kèm giây, kể cả khi còn > 1h: "23h 58m" đứng yên suốt 60 giây nên
// trông như đồng hồ chết, user không biết countdown có chạy hay không.
function fmtCountdown(secs) {
  const h = Math.floor(secs / 3600), m = Math.floor((secs % 3600) / 60), s = secs % 60;
  if (h > 0) return h + 'h ' + String(m).padStart(2, '0') + 'm ' + String(s).padStart(2, '0') + 's';
  if (m > 0) return m + 'm ' + String(s).padStart(2, '0') + 's';
  return s + 's';
}

// Mốc hết hạn tuyệt đối theo giờ máy user — để "còn 23h" có nghĩa cụ thể
// (mấy giờ, ngày nào) chứ không phải một con số trôi nổi.
function fmtDeadline(unixSecs) {
  const d = new Date(unixSecs * 1000);
  const p = (n) => String(n).padStart(2, '0');
  return p(d.getHours()) + ':' + p(d.getMinutes()) + ' ' + p(d.getDate()) + '/' + p(d.getMonth() + 1);
}

function applyProbe(t, probe) {
  if (!t.el) return;
  const e = t.el;
  const p = probe || {};

  const st = effStatus(t, p);   // server kết luận cuối thì thắng probe cũ

  if (p.ok) {
    // Số tiền: phải là SỐ PHẢI TRẢ, không phải hạn mức.
    //  • payload của uỷ nhiệm ₹0 KHÔNG có trường `amount` -> amount_minor=None.
    //    Rơi vào nhánh dự phòng `p.am` là in ra ₹1,999.00 trong khi thực tế ₹0 —
    //    đúng cái sai vừa bắt được trên job e0a538cbabcb.
    //  • Thứ tự đúng: amount của probe -> amount lưu lúc chạy (đọc từ log) ->
    //    0 nếu judge() nói là uỷ nhiệm ₹0 -> cuối cùng mới tới am, kèm title nói rõ
    //    đó là hạn mức chứ không phải số phải trả.
    const art = t.artifact || {};
    // Uỷ nhiệm ₹0 (fam < am): probe KHÔNG trả boolean `zero_mandate` — nó trả
    // `kind: 'zero_mandate'`. Trước đây UI kiểm tra `p.zero_mandate` (không tồn tại)
    // nên nhánh này không bao giờ chạy, và khi thiếu amount thì rơi xuống in HẠN MỨC
    // `am` = ₹1,999.00 trong khi uỷ nhiệm thực tế là ₹0 — đúng cái gây nhầm.
    const zeroKind = p.kind === 'zero_mandate' || art.zero_mandate === true;
    let minor = p.amount_minor;
    if (minor == null && art.amount_minor != null) minor = art.amount_minor;
    if (minor == null && zeroKind) minor = 0;

    if (minor != null) {
      e.successAmount.textContent = fmtAmount(minor) || '';
      // Nói rõ số này lấy từ đâu: card đọc trang Stripe LIVE, còn drawer hiện giá trị
      // đã chốt lúc trích xuất — hai thời điểm khác nhau nên có thể khác nhau.
      e.successAmount.title = p.amount_minor != null
        ? 'Live from the Stripe instructions page (now)'
        : (zeroKind
            ? '₹0 mandate — nothing charged now (cap ₹' + (p.am || '?') + ')'
            : 'Recorded when this link was extracted');
    } else if (p.am && !zeroKind) {
      // Chỉ dùng `am` khi KHÔNG phải uỷ nhiệm ₹0: lúc đó am mới là số phải trả.
      e.successAmount.textContent = fmtAmount(Math.round(parseFloat(p.am) * 100)) || '';
      e.successAmount.title = 'Mandate cap (am=' + p.am + ') — charge amount unreadable';
    } else {
      e.successAmount.textContent = '';
    }
    // Đã bỏ dòng phụ chú "UỶ NHIỆM ₹0 · kỳ đầu ₹1.00 · hạn mức ₹1,999.00" theo yêu
    // cầu — trên card chỉ cần đúng một con số phải trả (₹0.00 = uỷ nhiệm ₹0,
    // ₹1,999.00 = chuỗi thu tiền). Số kỳ đầu / hạn mức vẫn tra được qua tooltip
    // `title` của chính con số đó.

    // 倒计时只在「还在等」的时候有意义 —— 它回答的是「我还能用多久」。
    // 已经成功/已取消/已失败的链再显示「已过期」是误导：
    // expires_at 是**指引页**的寿命，不是委托的寿命（委托的有效期在 URI 的
    // validitystart/validityend 里，通常是十年）。
    if (st === 'waiting' && p.expires_at) {
      e.successExpiry.dataset.expires = p.expires_at;
      e.successExpiry.hidden = false;
    } else {
      e.successExpiry.hidden = true;
      e.successExpiry.textContent = '';
    }

    const [text, kind] = (st !== p.status && STATUS_UI[st])
      ? STATUS_UI[st]          // server đã kết luận (succeeded/failed/canceled/expired)
      : verdictFor(p);
    e.successVerdict.textContent = text;
    e.successVerdict.className = 'success-verdict ' + kind;
    // 链的类型只在不是 ₹0 委托时戳出来，免得每张卡都多一行
    e.successVerdict.title = (p.kind_label || '')
      + (p.intent_state ? ' · state=' + p.intent_state : '')
      + (st !== p.status ? ' · server=' + st : '');
    e.successVerdict.hidden = false;

    // CSS 描边选的是 .success-result[data-status]，以前写在 wrap(.task) 上，
    // 选择器永远匹配不到 -> 左边那道颜色条一直没出现过。
    if (e.successResult) {
      e.successResult.dataset.status = st || '';
    }
  } else if (p.error) {
    e.successVerdict.textContent = 'Could not read page: ' + p.error;
    e.successVerdict.className = 'success-verdict warn';
    e.successVerdict.hidden = false;
  }

  e.successAmountField.hidden = e.successAmount.textContent === ''
    && e.successExpiry.hidden && e.successVerdict.hidden;

  // Link Stripe: chỉ hiện khi probe đọc được `return_url`. Link dài (501–701 ký tự)
  // nên để trong 1 dòng cuộn được, không làm vỡ layout.
  const stripe = String(p.stripe_link || '');
  if (e.successStripeField) {
    e.successStripeField.hidden = !stripe;
    if (stripe) {
      e.successStripe.textContent = stripe;
      e.successStripe.href = safeUrl(stripe);
      e.successStripe.title = stripe;
    }
  }

  setChip(t);   // probe về mới biết link sống/chết -> đổi màu chip theo kết quả đó
  tickExpiry(e.successExpiry);
}

// Trước đây khoá cache là `url + '#fresh'` và KHÔNG bao giờ bị xoá -> gọi
// `probeArtifact(t, true)` (ý nghĩa: hỏi lại Stripe) thật ra chỉ đọc lại đúng
// promise cũ, không ra mạng lần nào. Hệ quả: card giữ mãi kết quả của lần dò đầu
// (thường là "khách chưa quét") và lưới an toàn 2 phút lại ghi đè kết quả đúng do
// server đẩy xuống. Giờ `fresh` không đọc và không ghi cache; cache chỉ dùng cho
// lần đọc rẻ (không fresh) để render hàng loạt card lúc mới load.
async function probeArtifact(t, fresh) {
  const url = (t.artifact || {}).upi_link || '';
  if (!/^https:\/\/payments\.stripe\.com\/upi\/instructions\//.test(url)) return null;
  if (t._probePending && t._probeUrl === url) return t._probePending;
  let entry;
  if (!fresh) entry = _probeCache.get(url);
  if (!entry) {
    entry = requestProbe(url, fresh);
    if (!fresh) _probeCache.set(url, entry);
  }
  t._probeUrl = url;
  const pending = entry.then(probe => {
    // A removed task, switched job, or retried artifact must not receive an old response.
    if (probe && tasks.get(t.task_id) === t && (t.artifact || {}).upi_link === url) {
      t.probe = probe;
      applyProbe(t, probe);
    }
    return probe;
  }).finally(() => {
    if (t._probePending === pending) t._probePending = null;
  });
  t._probePending = pending;
  return pending;
}

// Bound the browser safety-net as well as deduplicating requests for the same URL.
// A fresh read skips the completed cache, but can share an already in-flight read.
const _probeInFlight = new Map();
const _probeQueue = [];
let _activeProbes = 0;
const PROBE_CONCURRENCY = 6;

function requestProbe(url, fresh) {
  if (_probeInFlight.has(url)) return _probeInFlight.get(url);
  const entry = new Promise(resolve => {
    _probeQueue.push({ url, fresh, resolve });
  }).finally(() => {
    if (_probeInFlight.get(url) === entry) _probeInFlight.delete(url);
  });
  _probeInFlight.set(url, entry);
  drainProbes();
  return entry;
}

function drainProbes() {
  while (_activeProbes < PROBE_CONCURRENCY && _probeQueue.length) {
    const request = _probeQueue.shift();
    _activeProbes++;
    fetch('/api/artifact?url=' + encodeURIComponent(request.url) + (request.fresh ? '&fresh=1' : ''))
      .then(res => res.json())
      .catch(() => ({ ok: false, error: 'request failed' }))
      .then(request.resolve)
      .finally(() => { _activeProbes--; drainProbes(); });
  }
}

// 每 20 秒回访一次：过期 / 取消 / 成功都要及时看到。
// 到了终态就停 —— 已经成功或已取消的链再问也没意义。
function startMonitor(t) {
  const st = effStatus(t);
  // 一开始就落在终态就别启 —— 否则要等第一个 tick 才自己关掉
  if (TERMINAL.includes(st)) {
    if (t._monitor) clearInterval(t._monitor);
    t._monitor = null;
    return;
  }
  if (t._monitor) return;
  // Việc dò chính giờ do SERVER làm nền và đẩy xuống qua event `task_link`
  // (engine._link_monitor_loop). Ở đây chỉ giữ một lưới an toàn chậm, phòng khi
  // vòng nền bị tắt (UPI_MONITOR_SECS=0) — không còn là cơ chế chính, nên để thưa
  // hẳn: trước đây mỗi card tự hỏi mỗi 20s, 500 card là 25 request/giây từ một tab.
  t._monitor = setInterval(async () => {
    const live = tasks.get(t.task_id);
    if (live !== t || TERMINAL.includes(effStatus(t))) {
      clearInterval(t._monitor);
      t._monitor = null;
      return;
    }
    await probeArtifact(live, true);
  }, MONITOR_MS);
}

async function refreshArtifactNow(t) {
  const probe = await probeArtifact(t, true);
  if (probe && probe.ok && tasks.get(t.task_id) === t) startMonitor(t);
  return probe;
}

function tickExpiry(target) {
  if (document.hidden) return;
  const now = Math.floor(Date.now() / 1000);
  const elements = target ? [target]
    : document.querySelectorAll('.task:not([hidden]) .success-expiry[data-expires]:not([hidden])');
  elements.forEach((el) => {
    if (el.hidden) return;
    const exp = parseInt(el.dataset.expires, 10) || 0;
    if (!exp) { el.textContent = ''; return; }
    const left = exp - now;
    el.textContent = left > 0 ? 'expires in ' + fmtCountdown(left) : 'expired';
    el.title = 'Expires ' + fmtDeadline(exp) + ' (your local time)';
    el.classList.toggle('expired', left <= 0);
  });
}
setInterval(tickExpiry, 1000);

/* ---- Payment link: bấm = copy, có nút riêng để mở ----------------------
   Trước đây .success-link là <a href> nên bấm vào là mở luôn, rất dễ mở nhầm
   khi đang chỉ muốn lấy link đem đi dùng. Giờ:
     • bấm             -> copy
     • Ctrl/Cmd+bấm    -> mở tab mới (giữ nguyên thói quen của browser)
     • bấm đúp         -> mở tab mới
     • nút "Mở ↗"      -> mở tab mới (đường hiển thị rõ ràng)
*/
/* Email của các acc ĐÃ QUÉT THÀNH CÔNG (khách duyệt mandate), xếp theo dòng token.

   Vì sao chỉ lấy `linkState === 'succeeded'`: tab Success gồm cả link VỪA TẠO nhưng
   chưa ai quét. Link chưa quét thì chưa dùng được, nên copy hết sẽ lẫn acc chưa xong.
   Muốn biết chắc thì nhìn `linkZero`/`linkNote` — 'succeeded' là mốc duy nhất nghĩa
   là tiền/mandate đã thực sự qua.
*/
function approvedEmails() {
  const rows = [];
  for (const t of tasks.values()) {
    if (t.linkState === 'succeeded' && t.email) rows.push(t);
  }
  rows.sort((a, b) => (a.index == null ? 0 : a.index) - (b.index == null ? 0 : b.index));
  return rows.map(t => t.email);
}

async function copyToClipboard(text) {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch (err) {
    // 非安全上下文 / 没权限时退回 execCommand
    try {
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.setAttribute('readonly', '');
      ta.style.position = 'fixed';
      ta.style.top = '-1000px';
      document.body.appendChild(ta);
      ta.select();
      const okDone = document.execCommand('copy');
      ta.remove();
      return okDone;
    } catch (err2) {
      return false;
    }
  }
}

/* ---- Hộp thoại xác nhận của app -----------------------------------------
   Thay `confirm()` mặc định: confirm() chặn cứng luồng JS (mọi interval/SSE bị
   treo trong lúc chờ), không tạo kiểu được, và vài browser chặn luôn.
   Dùng:  const ok = await confirmDialog({title, text, okLabel, danger});
*/
let _modalResolve = null;

function confirmDialog(opts) {
  const o = opts || {};
  return new Promise((resolve) => {
    _modalResolve = resolve;
    els.modalTitle.textContent = o.title || 'Confirm';
    els.modalText.textContent = o.text || '';
    els.modalOk.textContent = o.okLabel || 'Remove';
    els.modalOk.classList.toggle('safe', o.danger === false);
    els.modal.hidden = false;
    els.modalOk.focus();
  });
}

function closeModal(answer) {
  if (els.modal.hidden) return;
  els.modal.hidden = true;
  const fn = _modalResolve;
  _modalResolve = null;
  if (fn) fn(!!answer);
}

function bindModal() {
  els.modalOk.addEventListener('click', () => closeModal(true));
  els.modalCancel.addEventListener('click', () => closeModal(false));
  // Bấm ra ngoài = huỷ (thói quen chung), Esc = huỷ, Enter = đồng ý
  els.modal.addEventListener('click', (ev) => { if (ev.target === els.modal) closeModal(false); });
  document.addEventListener('keydown', (ev) => {
    if (els.modal.hidden) return;
    if (ev.key === 'Escape') { ev.preventDefault(); closeModal(false); }
    else if (ev.key === 'Enter') { ev.preventDefault(); closeModal(true); }
  });
}

function openLink(url) {
  if (!url) return;
  window.open(url, '_blank', 'noopener,noreferrer');
}

/* ---- Âm báo: task ra link QR, và khách quét uỷ nhiệm ---------------------
   Browser chặn autoplay khi trang chưa có thao tác của người dùng. Trước khi bấm
   "Chạy" thì chưa có gesture -> lần kêu đầu tiên có thể bị chặn. Nên khi bị chặn
   ta ghi nhớ và mở khoá ở lần click/keydown kế tiếp, rồi kêu bù 1 tiếng.
*/
let notifyWanted = false;

function notifyOn() {
  return localStorage.getItem('upi-notify-off') !== '1';
}

function setNotify(on) {
  if (on) localStorage.removeItem('upi-notify-off');
  else localStorage.setItem('upi-notify-off', '1');
  if (els.btnNotifyLabel) {
    els.btnNotifyLabel.textContent = on ? 'Sound' : 'Muted';
    els.btnNotify.classList.toggle('is-off', !on);
  }
}

function playNotify() {
  if (!notifyOn() || !els.notifyAudio) return;
  try {
    els.notifyAudio.currentTime = 0;
    const p = els.notifyAudio.play();
    if (p && typeof p.catch === 'function') {
      p.catch(() => { notifyWanted = true; });   // chưa có gesture -> kêu bù sau
    }
  } catch (e) { /* bỏ qua: âm thanh là phụ trợ, không được làm vỡ luồng render */ }
}

// Mở khoá + kêu bù ở thao tác đầu tiên của người dùng
function unlockNotify() {
  if (!notifyWanted || !els.notifyAudio) return;
  notifyWanted = false;
  playNotify();
}
document.addEventListener('click', unlockNotify, { once: false });
document.addEventListener('keydown', unlockNotify, { once: false });

function toggleNotify() {
  const on = !notifyOn();
  setNotify(on);
  toast(on ? '🔔 Sound on' : '🔕 Sound off');
  if (on) playNotify();          // nghe thử luôn cho biết tiếng nào
}

document.addEventListener('click', async (ev) => {
  // Email: bấm là copy (giống link / Copy AT), có hồi báo ✓ copied / ✗ copy failed.
  const em = ev.target.closest('.task-email, .detail-email');
  if (em) {
    const text = (em.textContent || '').trim();
    if (!text || text === '—') return;
    ev.preventDefault();
    const okEm = await copyToClipboard(text);
    em.classList.remove('copied', 'copyfail');
    void em.offsetWidth;
    em.classList.add(okEm ? 'copied' : 'copyfail');
    // Hậu tố "✓ copied" của .task-email là ::after nằm TRONG nút, mà nút bị cắt bằng
    // text-overflow: ellipsis + overflow: hidden -> email dài là hậu tố bị cắt mất,
    // không thấy gì. Nên phải báo bằng toast.
    toast(okEm ? ('Copied ' + text) : 'Copy failed — try again');
    setTimeout(() => em.classList.remove('copied', 'copyfail'), 1400);
    return;
  }
  const btn = ev.target.closest('.success-open');
  if (btn) {
    ev.preventDefault();
    const field = btn.closest('.success-field');
    const link = field && field.querySelector('.success-link');
    openLink(link && (link.getAttribute('href') || link.textContent));
    return;
  }
  // 2 nút icon ở chế độ lưới: copy AT và copy Link Stripe (cùng hồi báo ✓ copied).
  const tool = ev.target.closest('.card-tool');
  if (tool) {
    ev.preventDefault();
    let val = '';
    if (tool.classList.contains('card-copyat')) {
      val = tool.dataset.token || '';
    } else {
      const st = tool.closest('.success-result');
      const link = st && st.querySelector('.success-link:not(.success-stripe)');
      val = link ? (link.getAttribute('href') || link.textContent || '') : '';
    }
    if (!val) return;
    const okTool = await copyToClipboard(val);
    tool.classList.remove('copied', 'copyfail');
    void tool.offsetWidth;
    tool.classList.add(okTool ? 'copied' : 'copyfail');
    setTimeout(() => tool.classList.remove('copied', 'copyfail'), 1400);
    return;
  }
  const at = ev.target.closest('.success-copyat');
  if (at) {
    ev.preventDefault();
    const token = at.dataset.token || '';
    if (!token) return;
    const okAt = await copyToClipboard(token);
    at.classList.remove('copied', 'copyfail');
    void at.offsetWidth;
    at.classList.add(okAt ? 'copied' : 'copyfail');
    setTimeout(() => at.classList.remove('copied', 'copyfail'), 1400);
    return;
  }
  const link = ev.target.closest('.success-link');
  if (!link) return;
  const url = link.getAttribute('href') || link.textContent || '';
  if (!url || url === 'No link') return;
  // 让 Ctrl/Cmd/Shift+点击、中键走浏览器默认行为（新标签打开）
  if (ev.ctrlKey || ev.metaKey || ev.shiftKey || ev.button === 1) return;
  ev.preventDefault();
  const okDone = await copyToClipboard(url);
  link.classList.remove('copied', 'copyfail');
  void link.offsetWidth;
  link.classList.add(okDone ? 'copied' : 'copyfail');
  setTimeout(() => link.classList.remove('copied', 'copyfail'), 1400);
});

document.addEventListener('dblclick', (ev) => {
  const link = ev.target.closest('.success-link');
  if (!link) return;
  openLink(link.getAttribute('href') || link.textContent);
});



// Nhãn số lần chạy: 'Run 3 / 3' khi job cho chạy nhiều lần, chỉ 'Run 1' khi chạy 1 lần.
// Trước đây chỉ ghi 'Run 3' nên khó biết đó là lần cuối hay còn lượt nữa.
function runLabel(t) {
  const n = t.run || 1;
  // Job chạy bằng code CŨ (retries = số lần thử lại) có thể đã tới lần 4 trong khi
  // cấu hình ghi 3 -> lấy max của hai giá trị để nhãn không bao giờ hiện 'Run 4 / 3'.
  const max = Math.max(state.retries || 0, n);
  return 'Run ' + n + (max > 1 ? ' / ' + max : '');
}

function renderRow(t) {
  if (!t.el) return;
  t.el.wrap.dataset.status = t.status;
  t.el.email.textContent = t.email || '';
  t.el.email.title = (t.email || 'Account') + ' · Copy email';
  t.el.email.setAttribute('aria-label', 'Copy email ' + (t.email || ''));
  t.el.toggle.setAttribute('aria-label', 'Toggle details for ' + (t.email || t.task_id));
  t.el.remove.setAttribute('aria-label', 'Remove task for ' + (t.email || t.task_id));
  if (t.el.idx && t.index != null) {
    // Số hiển thị do `renumber()` đặt theo tab đang xem; ở đây chỉ ghi nhớ dòng
    // token để tooltip nói rõ task này ứng với dòng AT nào.
    t.el.idx.dataset.token = String(t.index + 1);
  }
  t.el.status.textContent = rowStatusText(t);
  t.el.egress.textContent = rowEgressText(t);
  const pct = taskProgress(t);
  t.el.pct.textContent = pct + '%';
  t.el.barFill.style.width = pct + '%';
  t.el.bar.setAttribute('aria-valuenow', String(pct));

  // Một nguồn duy nhất cho nhãn + màu chip (chipFor), không giữ bảng map thứ hai
  // dễ lệch nhau — tab "Đã dừng" vừa thêm là ví dụ.
  setChip(t);

  t.el.successResult.hidden = t.status !== 'success';
  if (t.status !== 'success') {
    if (t._monitor) clearInterval(t._monitor);
    t._monitor = null;
    return;
  }

  const a = t.artifact || {};
  t.el.successEmail.textContent = t.email || '—';
  // AT 是 1~2 KB 的 JWT，铺满整格既挤又难读。只显示首尾各一小段，
  // 需要完整值时点右边的「Copy AT」直接进剪贴板。
  const token = t.token || '';
  t.el.successToken.textContent = token ? maskToken(token) : '—';
  t.el.successToken.title = token
    ? 'Click "Copy AT" for the full token (' + token.length + ' chars)'
    : 'No confirmed token for this task in the current session';
  if (t.el.cardCopyAt) {
    t.el.cardCopyAt.dataset.token = token;
    t.el.cardCopyAt.disabled = !token;
  }
  // Nút copy AT ở hàng chân card (chế độ lưới) là bản thứ hai -> phải gán token cho
  // MỌI nút, không chỉ nút đầu tiên.
  for (const btn of t.el.wrap.querySelectorAll('.card-tool.card-copyat')) {
    btn.dataset.token = token;
    btn.disabled = !token;
  }
  if (t.el.successCopyAt) {
    t.el.successCopyAt.dataset.token = token;
    t.el.successCopyAt.disabled = !token;
  }
  const link = safeUrl(a.upi_link);
  t.el.successLink.textContent = a.upi_link || 'No link';
  t.el.successLink.href = link;
  t.el.successLink.hidden = !a.upi_link;
  t.el.successAmountField.hidden = a.amount_minor == null;
  t.el.successAmount.textContent = fmtAmount(a.amount_minor) || '';
  // 金额和到期时间在落盘的 artifact 里都是 null（cs 流程不写这两个字段），
  // 只能现抓指引页。抓到之后 startMonitor 会每 20 秒回访一次。
  applyProbe(t, t.probe);
  // Chế độ lưới: số tiền lên header ("UPI · ₹0.00") và đếm ngược xuống hàng chân
  // card, để card gọn không phải có ô Amount đứng riêng.
  if (t.el.taskAmount) {
    const amt = a.amount_minor != null ? fmtAmount(a.amount_minor) : '';
    t.el.taskAmount.textContent = amt ? ('UPI · ' + amt) : '';
    t.el.taskAmount.hidden = !amt;
  }
  if (t.el.footExpiry) {
    const ex = t.el.successExpiry;
    t.el.footExpiry.textContent = (ex && !ex.hidden) ? ex.textContent : '';
  }
  if (t.probe) {
    startMonitor(t);
  } else {
    refreshArtifactNow(t);
  }

  // Cùng lý do như trên: ảnh QR bị Stripe xoá khi link hết dùng được (410).
  const qrOk = (a.qr_png || a.qr_svg) && !TERMINAL.includes(t.linkState);
  const qr = safeUrl(a.qr_png || a.qr_svg);
  t.el.successQrLink.hidden = !qrOk;
  if (qrOk) {
    t.el.successQrLink.href = qr;
    t.el.successQrLink.download = 'upi-qr-' + (t.index + 1) + '.png';
    if (!t.el.successQr._qrGuard) {
      // Lưới an toàn: link còn "waiting" nhưng ảnh đã bị xoá (hết hạn 5 phút) ->
      // ẩn ô QR thay vì để lại icon vỡ.
      t.el.successQr._qrGuard = true;
      t.el.successQr.addEventListener('error', () => {
        t.el.successQrLink.hidden = true;
      });
    }
    t.el.successQr.src = qr;
  } else {    t.el.successQr.removeAttribute('src');
  }
  t.el.successResult.hidden = t.status !== 'success';
  renderPlus(t);
}

/* Kết luận acc đã lên Plus chưa (server dò bằng chính AT của acc — at_check.py).
   Chỉ có ý nghĩa SAU khi khách duyệt mandate nên server chỉ gửi plus_state từ lúc
   đó; chưa có thì ẩn ô này thay vì hiện "chưa lên" gây hiểu nhầm. */
function renderPlus(t) {
  const e = t.el;
  if (!e || !e.successPlusField) return;
  const state = t.plusState || '';
  // Chip Plus ở hàng chân card (chỉ hiện ở chế độ lưới). Chưa duyệt mandate thì không
  // có chip — giống thiết kế card gọn.
  if (e.footPlus) {
    const row = CHIP_PLUS[state];
    e.footPlus.hidden = !row;
    if (row) {
      e.footPlus.textContent = row[0];
      e.footPlus.className = 'chip success-chip-plus ' + row[1];
    }
  }
  if (!state) {
    // Chưa có gì để kết luận. Chỉ hiện dòng chờ khi task ĐÃ có link — để thấy tính
    // năng check Plus nằm ở đâu thay vì tưởng là thiếu: việc dò chỉ bắt đầu SAU khi
    // khách duyệt mandate (link_state = succeeded). Link chưa ai quét thì ẩn hẳn.
    const hasLink = !!(t.artifact && (t.artifact.upi_link || t.artifact.qr_png));
    e.successPlusField.hidden = !hasLink;
    if (!hasLink) return;
    e.successPlus.textContent = 'Waiting for mandate';
    e.successPlus.className = 'success-plus is-waiting';
    e.successPlus.title = (t.linkState === 'failed' || t.linkState === 'expired'
      || t.linkState === 'canceled')
      ? 'No mandate was approved on this link — nothing to check'
      : 'Starts polling the account AT once the customer approves the mandate';
    return;
  }
  e.successPlusField.hidden = false;
  e.successPlus.textContent = state === 'plus' ? 'Plus ✓'
    : state === 'checking' ? 'Checking AT…'
    : 'Not Plus';
  e.successPlus.className = 'success-plus is-' + state;
  const when = t.plusCheckedAt
    ? ' · checked ' + new Date(t.plusCheckedAt * 1000).toLocaleTimeString()
    : '';
  e.successPlus.title = (t.plusNote || 'AT / promo not readable') + when;
}

// Dòng IP hiện sẵn trên mỗi row: exit + billing (không cần click).
function rowEgressText(t) {
  const e = t.egress;
  if (!e) return '';
  const ex = e.exit || {};
  const bl = e.billing || {};
  const parts = [];
  if (ex.ip) parts.push('exit ' + ex.ip + (ex.country ? ' (' + ex.country + ')' : ''));
  if (bl.ip && bl.ip !== ex.ip) {
    parts.push('billing ' + bl.ip + (e.billing_country ? ' (' + e.billing_country + ')' : ''));
  }
  return parts.join('  ·  ');
}

function renderDetailIfOpen(t) {
  if (t.expanded) renderDetail(t);
}

function renderDetail(t) {
  if (!t.el) return;

  const pct = taskProgress(t);
  let finished = 0;
  for (const s of t.steps) {
    if (s.status === 'done' || s.status === 'fail') finished++;
  }
  const dur = 'Took ' + fmtDuration(t.duration_ms);
  const errHtml = t.status === 'fail'
    ? '<div class="error-banner">Task failed: ' + esc(failReason(t)) + '</div>'
    : '';
  const stepsHtml = t.steps.map(stepHtml).join('');

  t.el.detail.innerHTML =
    '<div class="detail-head">' +
      '<button type="button" class="detail-email" title="Copy email">' + esc(t.email || '') + '</button>' +
      '<div class="detail-actions">' +
        '<button type="button" class="btn btn-retry">↻ Retry</button>' +
        '<button type="button" class="btn btn-remove">✕ Remove</button>' +
      '</div>' +
    '</div>' +
    '<div class="detail-section">' +
      '<div class="detail-section-title">Task execution</div>' +
      '<div class="exec-row">' + chipHtml(t) + '<span class="muted">' + dur + '</span>' +
        '<span class="run-label">' + runLabel(t) + '</span></div>' +
      errHtml +
    '</div>' +
    '<div class="progress-section">' +
      '<div class="progress-head"><span class="muted">Overall progress</span><span class="pct">' + pct + '%</span></div>' +
      '<div class="progressbar" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="' + pct + '">' +
        '<div class="fill" style="width:' + pct + '%"></div></div>' +
      '<div class="muted steps-line">' + finished + ' / ' + t.steps.length + ' steps complete · ' + runLabel(t) + '</div>' +
    '</div>' +
    flowCardHtml(t) +
    '<div class="step-grid">' + stepsHtml + '</div>' +
    artifactHtml(t) +
    '<div class="detail-footer muted">Created ' + esc(t.created_at || '—') + ' · Updated ' + esc(t.updated_at || '—') + '</div>';

  t.el.detail.querySelector('.btn-retry').addEventListener('click', () => retryTask(t));
  t.el.detail.querySelector('.btn-remove').addEventListener('click', () => removeTask(t));
}

function toggleDetail(t) {
  t.expanded = !t.expanded;
  if (t.expanded) {
    renderDetail(t);
  } else {
    t.el.detail.innerHTML = '';
  }
  t.el.detail.hidden = !t.expanded;
  t.el.toggle.setAttribute('aria-expanded', String(t.expanded));
}

/* ------------------------- counters / UI --------------------------- */

// Dem task theo tung nhom trong hang doi.
function countByStatus() {
  const c = { all: tasks.size, running: 0, queued: 0, success: 0, fail: 0, stopped: 0 };
  for (const t of tasks.values()) {
    if (t.status === 'running') c.running++;
    else if (t.status === 'success') c.success++;
    else if (t.status === 'fail') c.fail++;
    else if (t.status === 'stopped') c.stopped++;
    else c.queued++;
  }
  return c;
}

function updateCounters() {
  countersDirty = true;
  scheduleRender();
}

function renderCounters() {
  const c = countByStatus();
  state.counts = c;

  const total = state.total || tasks.size || 0;
  const pct = total ? Math.round(((c.success + c.fail) / total) * 100) : 0;
  els.overallPct.textContent = pct + '%';
  els.overallFill.style.width = pct + '%';
  els.overallBar.setAttribute('aria-valuenow', String(pct));
  els.overallCount.textContent =
    (c.success + c.fail) + '/' + total + ' done · ' + c.running + ' running · ' + c.queued + ' queued'
    + (c.stopped ? ' · ' + c.stopped + ' stopped' : '');

  // so tren tung tab
  for (const f of QUEUE_FILTERS) {
    const el = els.tabCounts[f.key];
    if (el) el.textContent = c[f.key];
  }

  applyFilter();
  updateEmptyState();
  updateQueueLoading();
  updateRuntimeMetrics();

  // Nút "Dọn dẹp" nói rõ nó sắp xoá tab nào và bao nhiêu task — nút ✕ nhỏ trên tab
  // trước đây khó thấy, user phải đi xoá từng card một.
  // Nút copy email: đếm theo SỐ ACC ĐÃ QUÉT THÀNH CÔNG (không phải theo tab đang chọn)
  if (els.btnCopyEmailsLabel) {
    let nOk = 0;
    for (const t of tasks.values()) if (t.linkState === 'succeeded' && t.email) nOk++;
    els.btnCopyEmailsLabel.textContent = 'Copy emails' + (nOk ? ' (' + nOk + ')' : '');
    els.btnCopyEmails.disabled = nOk === 0;
  }
  if (els.btnClearTabLabel) {
    const label = (QUEUE_FILTERS.find(f => f.key === state.filter) || {}).label || '';
    const n = state.filter === 'all' ? tasks.size : (c[state.filter] || 0);
    els.btnClearTabLabel.textContent = 'Clear "' + label + '"' + (n ? ' (' + n + ')' : '');
    els.btnClearTab.disabled = n === 0;
  }
}

function updateRuntimeMetrics() {
  const c = state.counts || {};
  const elapsed = state.elapsedMs + (state.running && state.elapsedAt ? Date.now() - state.elapsedAt : 0);
  if (els.runtimeRunning) els.runtimeRunning.textContent = String(c.running || 0);
  if (els.runtimeQueued) els.runtimeQueued.textContent = String(c.queued || 0);
  if (els.runtimeWorkers) els.runtimeWorkers.textContent = String(state.workers || readWorkers().value);
  if (els.runtimeElapsed) els.runtimeElapsed.textContent = fmtElapsed(Math.floor(Math.max(0, elapsed) / 1000));
  if (els.runtimeThroughput) {
    const done = (c.success || 0) + (c.fail || 0);
    els.runtimeThroughput.textContent = (elapsed > 0 ? (done * 60000 / elapsed).toFixed(1) : '0.0') + ' / min';
  }
  if (els.runtimeStatus) {
    const kind = state.starting || (state.connecting && state.running) ? 'connecting'
      : state.stopping ? 'stopping' : state.running ? 'running' : state.jobId ? 'done' : 'idle';
    const labels = { connecting: 'Connecting', stopping: 'Stopping', running: 'Running', done: 'Done', idle: 'Ready' };
    els.runtimeStatus.dataset.state = kind;
    els.runtimeStatus.textContent = state.jobStatus === 'stopped' && kind === 'done' ? 'Stopped' : labels[kind];
  }
}

// Server đẩy task_init cho **toàn bộ** token ngay khi bắt đầu (engine._job_worker),
// nên đếm được đúng số task đã vào queue: N/total chạy từ 1..total rồi tự ẩn.
function updateQueueLoading() {
  const el = els.queueLoading;
  if (!el) return;

  // 1) Giai đoạn chờ /api/run trả về: server đang quét + lọc pool proxy (khi bật
  //    "Chỉ dùng proxy sạch") HOẶC chỉ đang tạo job. Lúc này CHƯA có job -> không
  //    có gì để đếm, nên hiện đồng hồ chạy giây thay cho thanh %.
  if (state.starting) {
    el.hidden = false;
    const secs = Math.max(0, Math.floor((Date.now() - (state.startingAt || Date.now())) / 1000));
    // KHÔNG in số proxy đã dán: số dòng trong textarea khác xa số proxy server
    // thực sự quét — parse_proxy_lines() bỏ dòng rác và chuẩn hoá host:port:user:pass,
    // đo được 6799 dòng dán vào nhưng server chỉ nhận 100. Ghi "quét 6799 proxy" là
    // nói sai và làm user tưởng phải chờ hàng giờ.
    els.queueLoadingText.textContent = 'Creating job… ' + fmtElapsed(secs);
    els.queueLoadingFill.style.width = '100%';   // thanh chạy mờ (indeterminate)
    el.classList.add('indeterminate');
    return;
  }
  el.classList.remove('indeterminate');

  // 2) Job đã tạo, task_init đang về: đếm thật N/total rồi tự ẩn khi nạp đủ.
  const total = state.total || 0;
  const loaded = tasks.size;
  const show = state.running && total > 0 && loaded < total;
  el.hidden = !show;
  if (!show) return;
  els.queueLoadingText.textContent = 'Loading queue… ' + loaded + '/' + total;
  els.queueLoadingFill.style.width = Math.round((loaded / total) * 100) + '%';
}

function fmtElapsed(secs) {
  const m = Math.floor(secs / 60), s = secs % 60;
  return m > 0 ? m + 'm ' + String(s).padStart(2, '0') + 's' : s + 's';
}

// Mỗi giây cập nhật lại banner "đang chuẩn bị" (đồng hồ chạy) cho tới khi có job.
setInterval(() => {
  if (state.starting) updateQueueLoading();
  if (!document.hidden) updateRuntimeMetrics();
}, 1000);

// An/hien tung hang theo tab dang chon. Chi doi hidden, khong render lai.
// Ten tab (bucket) khac ten status cua task: tab 'queued' ung voi task.status 'pending'.
const FILTER_TO_STATUS = {
  running: 'running',
  queued: 'pending',
  success: 'success',
  fail: 'fail',
  stopped: 'stopped',
};

function taskMatchesFilter(t) {
  if (state.filter !== 'all' && t.status !== FILTER_TO_STATUS[state.filter]) return false;
  return !state.search || (t.email + ' ' + t.task_id).toLowerCase().includes(state.search);
}

// Thứ tự row trong #task-list.
//   • tab Success  -> theo THỜI ĐIỂM THÀNH CÔNG (card thành công trước nằm trên).
//     Trước đây mọi tab đều theo dòng token nên card thành công sau nhưng có dòng
//     token sớm hơn lại nhảy lên trên, đẩy card thành công trước xuống.
//   • tab khác      -> theo dòng token như cũ.
// Chỉ chèn lại DOM khi thứ tự thật sự khác, để không phải sắp lại 1000 row mỗi event.
function orderRows() {
  const cur = [...els.taskList.children];
  if (cur.length < 2) return;
  const key = (w) => {
    const t = tasks.get(w.dataset.taskId) || {};
    return state.filter === 'success'
      ? [t.doneAt || 0, t.index == null ? 0 : t.index]
      : [t.index == null ? 0 : t.index, 0];
  };
  const want = cur.slice().sort((a, b) => {
    const ka = key(a), kb = key(b);
    return ka[0] - kb[0] || ka[1] - kb[1];
  });
  let same = true;
  for (let i = 0; i < cur.length; i++) {
    if (cur[i] !== want[i]) { same = false; break; }
  }
  if (same) return;
  const frag = document.createDocumentFragment();
  for (const w of want) frag.appendChild(w);
  els.taskList.appendChild(frag);
}

function applyFilter() {
  if (orderDirty) {
    orderRows();
    orderDirty = false;
  }
  let visible = 0;
  for (const t of tasks.values()) {
    const show = taskMatchesFilter(t);
    if (show) visible++;
    if (!t.el || !t.el.wrap) continue;
    if (t.el.wrap.hidden === show) t.el.wrap.hidden = !show;
  }
  renumber();
  els.listEmpty.hidden = !(tasks.size > 0 && visible === 0);
  if (els.listEmpty) {
    const label = (QUEUE_FILTERS.find(f => f.key === state.filter) || {}).label || '';
    els.listEmpty.textContent = state.search ? 'No matching tasks in "' + label + '".' : 'No tasks in "' + label + '".';
  }
}

// Đánh số thứ tự theo DANH SÁCH ĐANG HIỂN THỊ: đổi tab thì bắt đầu lại từ #1.
// (Trước đó lấy theo dòng token nên ở tab "Thành công" có 1 card vẫn hiện #397 —
// đúng về dòng AT nhưng vô nghĩa khi đang đếm trong tab.) Dòng token vẫn còn
// trong tooltip để tra ngược.
function renumber() {
  let n = 0;
  for (const wrap of els.taskList.children) {
    const idx = wrap._idxEl;
    if (!idx) continue;
    if (wrap.hidden) {
      if (idx.textContent) { idx.textContent = ''; idx.removeAttribute('title'); }
      continue;
    }
    n++;
    const want = '#' + n;
    if (idx.textContent !== want) idx.textContent = want;
    const token = idx.dataset.token || '';
    idx.title = token
      ? 'Task #' + n + ' in this tab · token line ' + token + ' in the tokens box'
      : 'Task #' + n + ' in this tab';
  }
}

function setFilter(key) {
  orderDirty = true;
  state.filter = key;
  document.body.dataset.filter = key;
  document.querySelectorAll('.tab').forEach(b => {
    const active = b.dataset.filter === key;
    b.classList.toggle('active', active);
    b.setAttribute('aria-selected', String(active));
  });
  updateCounters();
}

function setView(view, persist = true) {
  state.view = view === 'grid' ? 'grid' : 'list';
  document.body.dataset.view = state.view;
  document.querySelectorAll('.view-option').forEach(button => {
    const active = button.dataset.view === state.view;
    button.classList.toggle('active', active);
    button.setAttribute('aria-pressed', String(active));
  });
  if (persist) saveForm();
}

function updateEmptyState() {
  els.emptyState.hidden = tasks.size > 0;
}

function setJobBadge(jobId) {
  if (els.jobIdInput) {
    els.jobIdInput.value = jobId || '';
    els.jobIdInput.classList.remove('bad');
  }
  updatePushQueue();   // đổi job -> trạng thái nút Push phải theo
}

// Đổ danh sách job id đã biết vào <datalist> để gõ tới đâu gợi ý tới đó.
async function fillJobIdList() {
  if (!els.jobIdList) return;
  try {
    const res = await fetch('/api/jobs');
    if (!res.ok) return;
    const data = await res.json();
    renderJobHistory(data.jobs || []);
    els.jobIdList.innerHTML = '';
    for (const j of data.jobs || []) {
      const opt = document.createElement('option');
      // Nội dung gợi ý = job id (để dán/Enter là chạy), nhãn kèm trạng thái cho dễ chọn
      opt.value = j.job_id;
      opt.label = [j.status, j.done + '/' + j.total,
                   j.ok ? 'ok ' + j.ok : '', j.fail ? 'fail ' + j.fail : '',
                   j.started_at || ''].filter(Boolean).join(' · ');
      els.jobIdList.appendChild(opt);
    }
  } catch (e) { /* không có danh sách cũng không sao, vẫn dán tay được */ }
}

// "2026-10-04 11:01:20" -> "11:01 · 04/10". Rỗng thì trả "—".
function fmtJobTime(started) {
  const m = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})/.exec(String(started || ''));
  if (!m) return '—';
  return m[4] + ':' + m[5] + ' · ' + m[3] + '/' + m[2];
}

// Lịch sử job: server giữ file kết quả trên đĩa nên job cũ (kể cả job chết vì
// restart) vẫn mở lại được. Trước đây chỉ có datalist ẩn trong ô Job ID — phải
// biết id mới dùng được, nên thêm danh sách bấm-được ở đây.
function renderJobHistory(jobs) {
  if (!els.historyList) return;
  const all = jobs || [];
  // Server trả theo thứ tự RAM-trước / mtime file kết quả, không theo thời gian
  // chạy -> sắp lại, mới nhất lên đầu cho dễ tìm.
  const list = all.slice().sort((a, b) =>
    String(b.started_at || '').localeCompare(String(a.started_at || ''))).slice(0, 40);
  if (els.historyCount) els.historyCount.textContent = String(all.length);
  if (!list.length) {
    els.historyList.innerHTML = '<div class="history-empty">No jobs yet</div>';
    return;
  }
  els.historyList.innerHTML = list.map((j) => {
    const st = j.status === 'running' ? 'running'
             : j.status === 'done' ? 'done' : 'stopped';
    const stat = [];
    if (j.ok) stat.push('<b class="ok">' + j.ok + '</b> ok');
    if (j.fail) stat.push('<b class="bad">' + j.fail + '</b> fail');
    if (!stat.length) stat.push((j.done || 0) + '/' + (j.total || 0));
    const title = j.job_id + ' · ' + st + ' · ' + (j.done || 0) + '/' + (j.total || 0)
                + ' task' + (j.started_at ? ' · ' + j.started_at : '');
    return '<button type="button" class="history-row'
      + (j.job_id === state.jobId ? ' is-current' : '')
      + '" data-job="' + esc(j.job_id) + '" title="' + esc(title) + '">'
      + '<span class="history-when">' + esc(fmtJobTime(j.started_at)) + '</span>'
      + '<span class="history-id mono">' + esc(j.job_id) + '</span>'
      + '<span class="history-stat">' + stat.join(' · ') + '</span>'
      + '<span class="history-status is-' + st + '">' + ({ running: 'Running', done: 'Done', stopped: 'Stopped' }[st]) + '</span>'
      + '</button>';
  }).join('');
  markCurrentJob();
}

// Hiện/ẩn lý do job bị dừng sớm. Xoá khi bắt đầu job mới để không lẫn thông tin cũ.
function setStopNote(reason) {
  if (!els.stopNote) return;
  els.stopNote.hidden = !reason;
  els.stopNote.textContent = reason ? '⏹ Stopped before finishing — ' + reason : '';
}

// Đánh dấu dòng của job đang mở. Gọi lại mỗi lần đổi job (không fetch lại).
function markCurrentJob() {
  if (!els.historyList) return;
  for (const row of els.historyList.querySelectorAll('.history-row')) {
    row.classList.toggle('is-current', row.dataset.job === state.jobId);
  }
}

// Xem lịch sử của một job bất kỳ theo id dán vào.
async function loadJobById(raw) {
  const id = String(raw || '').trim().toLowerCase();
  if (!/^[0-9a-f]{12}$/.test(id)) {
    els.jobIdInput.classList.add('bad');
    toast('Job ID must be 12 hex chars (0-9a-f).');
    return;
  }
  // Kiểm tra trước rồi mới attach: attachJob() không báo lỗi, sẽ để lại bảng trống
  // mà không nói vì sao.
  let data;
  try {
    const res = await fetch('/api/state/' + id);
    if (!res.ok) { els.jobIdInput.classList.add('bad'); toast('Job not found: ' + id); return; }
    data = await res.json();
  } catch (e) {
    toast('Connection lost loading job ' + id);
    return;
  }
  attachJob(id, data);
  toast('Viewing job ' + id + ' · ' + (data.tasks || []).length + ' task'
        + (data.status ? ' · ' + data.status : ''));
}

// Tắt hết interval dò link của job cũ trước khi chuyển job
function detachMonitors() {
  for (const t of tasks.values()) {
    if (t._monitor) { clearInterval(t._monitor); t._monitor = null; }
  }
  for (const request of _probeQueue.splice(0)) request.resolve(null);
  _probeCache.clear();
  _probeInFlight.clear();
}

function getTokens() {
  return els.tokens.value.split('\n').map(s => s.trim()).filter(Boolean);
}

function getProxies() {
  const lines = els.proxies.value.split('\n').map(s => s.trim()).filter(Boolean);
  return lines.length ? lines.join('\n') : null;
}

// Đọc số worker từ input + nói rõ khi phải kẹp lại. Trước đây kẹp ÂM THẦM
// (`Math.min(32, ...)`) nên gõ 40 mà job chạy 32 và không ai biết vì sao.
function readWorkers() {
  const raw = parseInt(els.workers.value, 10);
  const want = Number.isNaN(raw) ? 4 : raw;
  return {
    raw: want,
    value: Math.max(1, Math.min(MAX_WORKERS, want)),
    clamped: want > MAX_WORKERS,
  };
}

function updateTokenCount() {
  const n = getTokens().length;
  els.tokenCount.textContent = 'Received ' + n + ' tokens';
  els.summaryTasks.textContent = n + ' tasks × 1 use';
  els.summaryTotal.textContent = 'Total uses: ' + n;
  // Chỉ còn cảnh báo token (đã bỏ cảnh báo RAM theo yêu cầu). Khi bị kẹp trần worker
  // thì toast ở runJob vẫn báo "Capped at N — you asked for M".
  const warn = n > 0 ? n + ' tokens ready. Tasks over quota may fail.' : '';
  els.warning.hidden = !warn;
  els.warning.textContent = warn;
  setSubmitState();
}

function setSubmitState() {
  const n = getTokens().length;
  // Ghi thẳng id job lên nút Stop: lỗi cũ là UI hiển thị một job mà lệnh Stop lại
  // gửi cho job khác, nên người dùng bấm Stop mà batch đang xem vẫn chạy.
  const jidTag = state.jobId ? (' · ' + state.jobId) : '';
  if (state.starting) {
    // POST /api/run còn treo vì server đang quét/lọc pool proxy — lúc này chưa có
    // job để "dừng", nên bấm nút chỉ gây lỗi; hiện trạng thái bận cho rõ.
    els.submit.textContent = 'Preparing…';
    els.submit.classList.remove('danger');
    els.submit.disabled = true;
  } else if (state.stopping) {
    // Vẫn bấm được: gửi lại lệnh dừng là vô hại, và nút chết cứng suốt vài phút
    // (task đang chạy phải unwound xong) chính là cảm giác "ấn Stop không dừng".
    els.submit.textContent = 'Stopping…' + jidTag;
    els.submit.classList.add('danger');
    els.submit.disabled = false;
  } else if (state.running) {
    els.submit.textContent = 'Stop job' + jidTag;
    els.submit.classList.add('danger');
    els.submit.disabled = false;
  } else {
    els.submit.textContent = 'Run ' + n + ' tasks';
    els.submit.classList.remove('danger');
    els.submit.disabled = n === 0;
  }
  // 停止期间给整个队列加个标记：正在跑的卡会显示「đang huỷ…」，
  // 让用户看到「停止确实在做事情」，而不是只看到按钮变灰、任务照旧在跑。
  updateCounters();
  updatePushQueue();   // trạng thái chạy đổi -> ẩn/hiện nút Push
}

function updateModeHint() {
  els.modeHint.textContent = MODE_HINTS[state.mode] || MODE_HINTS.auto;
}

function setMode(mode) {
  state.mode = mode;
  document.querySelectorAll('.seg').forEach(b => {
    const active = b.dataset.mode === mode;
    b.classList.toggle('active', active);
    b.setAttribute('aria-checked', String(active));
  });
  updateModeHint();
  if (typeof saveForm === 'function') saveForm();
}

function toast(msg) {
  const el = document.createElement('div');
  el.className = 'toast';
  el.textContent = msg;
  els.toasts.appendChild(el);
  setTimeout(() => {
    el.classList.add('out');
    setTimeout(() => el.remove(), 300);
  }, 4000);
}

/* --------------------------- SSE handling -------------------------- */

function ensureTask(taskId) {
  let t = tasks.get(taskId);
  if (!t) {
    t = newTask(taskId);
    tasks.set(taskId, t);
    queueTaskRender(t, true, true);
  }
  return t;
}

function handleEvent(evt) {
  if (!evt || typeof evt !== 'object' || !evt.type) return;
  switch (evt.type) {
    case 'job_start': onJobStart(evt); break;
    case 'task_init': onTaskInit(evt); break;
    case 'task_flow': onTaskFlow(evt); break;
    case 'step': onStep(evt); break;
    case 'egress': onEgress(evt); break;
    case 'task_artifact': onArtifact(evt); break;
    case 'task_link': onTaskLink(evt); break;
    case 'task_plus': onTaskPlus(evt); break;
    case 'task_done': onTaskDone(evt); break;
    case 'job_done': onJobDone(evt); break;
    case 'job_stopping': onJobStopping(evt); break;
    case 'error': toast(evt.message || 'Error'); break;
    case 'state': applySnapshot(evt.job || {}); break;
    case 'proxy_quality': onProxyQuality(evt); break;
    case 'task_start': onTaskStart(evt); break;
    case 'job_progress':
      if (evt.total != null) state.total = evt.total;
      updateCounters();
      break;
  }
}

function onJobStart(evt) {
  state.jobId = evt.job_id || state.jobId;
  state.total = evt.total || 0;
  state.mode = evt.mode || 'auto';
  state.running = true;
  state.stopping = false;
  state.jobStatus = 'running';
  state.workers = evt.workers || state.workers;
  if (evt.retries) state.retries = evt.retries;
  state.elapsedMs = evt.elapsed_ms || 0;
  state.elapsedAt = Date.now();
  setJobBadge(state.jobId);
  setSubmitState();
}

function onJobStopping() {
  state.running = true;
  state.stopping = true;
  for (const t of tasks.values()) {
    if (t.status === 'running' || t.status === 'pending') queueTaskRender(t);
  }
  setSubmitState();
}

function onTaskStart(evt) {
  const t = ensureTask(evt.task_id);
  t.status = 'running';
  if (evt.run != null) t.run = evt.run;
  queueTaskRender(t, true);
}

function onTaskInit(evt) {
  const t = ensureTask(evt.task_id);
  if (evt.index != null) t.index = evt.index;
  t.token = state.tokensJobId === state.jobId ? state.tokens[t.index] || '' : '';
  if (evt.email != null) t.email = evt.email;
  if (Object.prototype.hasOwnProperty.call(evt, 'flow')) t.flow = evt.flow || null;
  t.steps = (evt.steps || []).map(s => ({ key: s.key, label: s.label || s.key, status: 'pending', detail: '', err: '' }));
  t.status = 'pending';
  if (evt.run != null) t.run = evt.run;
  t.rawStatus = t.error = t.duration_ms = t.artifact = t.egress = t.probe = null;
  t.linkState = '';
  t.doneAt = 0;
  t.notified = false;
  queueTaskRender(t, true, true);
}

function onTaskFlow(evt) {
  const t = ensureTask(evt.task_id);
  if (evt.flow != null) t.flow = evt.flow;
  if (evt.flow_label != null) t.flow_label = evt.flow_label;
  // Flow became known: REPLACE the step list with the flow-specific steps.
  t.steps = (evt.steps || []).map(s => ({ key: s.key, label: s.label || s.key, status: 'pending', detail: '', err: '' }));
  queueTaskRender(t);
}

function onStep(evt) {
  const t = ensureTask(evt.task_id);
  const s = t.steps.find(x => x.key === evt.key);
  if (!s) return;
  if (evt.status) s.status = evt.status;
  if (evt.detail != null) s.detail = evt.detail;
  if (evt.err != null) s.err = evt.err;
  const statusChanged = evt.status === 'active' && t.status !== 'running';
  if (evt.status === 'active') t.status = 'running';
  queueTaskRender(t, statusChanged);
}

function onEgress(evt) {
  const t = ensureTask(evt.task_id);
  const ex = evt.exit || {};
  const bl = evt.billing || {};
  t.egress = {
    proxy: evt.proxy || '',
    exit: ex,
    billing: bl,
    billing_country: evt.billing_country || '',
    same_ip: evt.same_ip != null ? evt.same_ip : (ex.ip && ex.ip === bl.ip),
    probed_at: evt.probed_at || '',
    billing_geo: evt.billing_geo || null,
    // tuong thich nguoc voi du lieu cu chi co 1 IP
    ip: ex.ip || evt.ip || '',
    location: fmtGeo(ex) || evt.location || '',
  };
  queueTaskRender(t);
}

function fmtGeo(g) {
  if (!g) return '';
  return [g.city, g.region, g.country].filter(Boolean).join(', ');
}

function onProxyQuality(evt) {
  // Chat luong pool proxy cua job: diem/risk/user_type cua con tot nhat, so con bi
  // loai, va so proxy sinh bu khi pool thieu (xem ippure.py + proxy_pool.py).
  if (evt.error) { toast('Proxy quality: skipping (' + evt.error + ')'); return; }
  const b = evt.best || {};
  const g = evt.generated || {};
  const parts = [];
  if (b.grade) parts.push(b.grade + ' ' + (b.score != null ? b.score + ' pts' : ''));
  if (b.country) parts.push(b.country);
  if (b.risk != null) parts.push('risk ' + b.risk);
  if (b.user_type) parts.push(b.user_type);
  if (evt.scanned) parts.push('scored ' + evt.scanned + ' proxies');
  if (g.verified) parts.push('replaced ' + g.verified + ' proxies (' + g.sess_minutes + ' min)');
  else if (evt.excluded) parts.push('excluded ' + evt.excluded);
  if (parts.length) toast('Pool proxy: ' + parts.join(' · '));
}

// Server dò link ở NỀN và đẩy xuống đây mỗi khi trạng thái đổi (khách quét được /
// link chết). Nhờ vậy không cần browser tự setInterval 20s nữa — đóng tab vẫn có
// người theo dõi, và mở lại là thấy kết quả mới nhất.
function onTaskLink(evt) {
  const live = tasks.get(evt.task_id);
  if (!live) return;
  live.linkState = evt.link_state || '';
  live.linkFlipAt = evt.link_flip_at || 0;
  live.linkZero = !!evt.link_zero;
  queueTaskRender(live, true);
  if (evt.probe && evt.probe.ok) {
    live.probe = evt.probe;
    // Render lại: link đổi trạng thái thì phần QR phải vẽ lại theo — uỷ nhiệm vừa
    // được duyệt là Stripe xoá ảnh QR (410 Gone) nên ảnh cũ thành icon vỡ.
    const who = evt.link_state === 'succeeded' ? 'CUSTOMER SCANNED — mandate succeeded'
              : evt.link_state === 'failed' ? 'Link dead'
              : evt.link_state === 'expired' ? 'Link expired, never scanned'
              : evt.link_state === 'canceled' ? 'Link cancelled' : '';
    if (who) {
      // "Khách đã quét" đáng kêu hơn cả lúc ra link — đây mới là lúc có uỷ nhiệm thật
      if (evt.link_state === 'succeeded') {
        playNotify();
        toast('🔔 ' + (live.email || evt.task_id) + ' — ' + who
              + (evt.age ? ' (sau ' + evt.age + 's)' : ''));
      } else {
        toast(who + (evt.age ? ' (sau ' + evt.age + 's)' : ''));
      }
    }
  }
}

function onTaskPlus(evt) {
  const live = tasks.get(evt.task_id);
  if (!live) return;
  live.plusState = evt.plus_state || '';
  live.plusNote = evt.plus_note || '';
  live.plusCheckedAt = evt.plus_checked_at || 0;
  queueTaskRender(live, true);
  // Chỉ kêu khi có kết luận cuối: lúc này acc đã thực sự lên Plus.
  if (evt.plus_state === 'plus') {
    toast('⭐ ' + (live.email || evt.task_id) + ' — upgraded to Plus'
          + (evt.plus_note ? ' (' + evt.plus_note + ')' : ''));
  }
}

function onArtifact(evt) {
  const t = ensureTask(evt.task_id);
  if ((t.artifact || {}).upi_link !== evt.upi_link) t.probe = null;
  t.artifact = {
    upi_link: evt.upi_link || null,
    qr_png: evt.qr_png || null,
    qr_svg: evt.qr_svg || null,
    amount_minor: evt.amount_minor != null ? evt.amount_minor : null,
    intent: evt.intent || null,
  };
  queueTaskRender(t);
}

function onTaskDone(evt) {

  const t = ensureTask(evt.task_id);
  t.rawStatus = evt.status || null;
  t.status = normalizeStatus(evt.ok ? 'done' : 'fail', evt.status);
  t.error = evt.error || null;
  t.duration_ms = evt.duration_ms != null ? evt.duration_ms : null;
  if (evt.updated_at) t.updated_at = evt.updated_at;
  // Mốc xong: dùng số của server; server cũ không gửi thì lấy giờ máy.
  t.doneAt = evt.done_at || Math.floor(Date.now() / 1000);
  queueTaskRender(t, true, state.filter === 'success');
  // Task vừa ra link QR -> kêu + báo tên account. Mắt không thể canh 1000 dòng.
  if (t.status === 'success' && !t.notified) {
    t.notified = true;
    playNotify();
    toast('✅ ' + (t.email || t.task_id) + ' — QR link created');
  }
}

function onJobDone(evt) {
  state.elapsedMs = evt.duration_ms ?? (state.elapsedMs + (state.elapsedAt ? Date.now() - state.elapsedAt : 0));
  state.elapsedAt = Date.now();
  state.jobStatus = evt.status || 'done';
  state.running = false;
  state.stopping = false;
  state.jobId = evt.job_id || state.jobId;
  closeES();
  setSubmitState();
  toast(evt.status === 'stopped'
    ? 'Stopped · ' + (evt.done ?? 0) + '/' + (evt.total ?? 0) + ' tasks processed'
    : 'Job finished: ' + (evt.ok ?? 0) + ' ok · ' + (evt.fail ?? 0) + ' fail');
  // Lý do dừng: chỉ còn khi NGƯỜI DÙNG bấm Stop (đã bỏ circuit breaker tự dừng
  // theo yêu cầu) — hiện thường trực để không tưởng job chạy hết bình thường.
  if (evt.stop_reason) {
    toast('⏹ Stopped — ' + evt.stop_reason);
    setStopNote(evt.stop_reason);
  }
  fillJobIdList();   // job vừa xong đã có file kết quả -> cập nhật lại lịch sử
  loadGlobalStats(); // job xong mới ghi file kết quả -> số mã theo ngày vừa đổi
}

function closeES() {
  if (state.es) {
    state.es.close();
    state.es = null;
  }
  state.connecting = false;
}

function connectES(jobId) {
  closeES();
  state.connecting = true;
  let reconnecting = false;
  let hadConnection = false;
  const es = new EventSource('/api/stream/' + jobId);
  es.onmessage = e => {
    if (state.es !== es || state.jobId !== jobId) return;
    let event;
    try { event = JSON.parse(e.data); } catch (err) { return; }
    state.streamRevision++;
    handleEvent(event);
  };
  es.onopen = () => {
    if (state.es !== es) return;
    if (reconnecting && hadConnection) {
      toast('Reconnected');
    }
    // Every stream connection starts with a server snapshot; another GET races it.
    state.connecting = false;
    updateRuntimeMetrics();
    reconnecting = false;
    hadConnection = true;
  };
  es.onerror = () => {
    if (state.es !== es) return;
    if (!reconnecting) toast('Disconnected, retrying…');
    reconnecting = true;
    state.connecting = true;
    updateRuntimeMetrics();
  };
  state.es = es;
}

/* -------------------------- snapshot reload ------------------------ */

async function loadState(jobId) {
  const revision = state.streamRevision;
  const connection = state.es;
  let data;
  try {
    const res = await fetch('/api/state/' + jobId);
    if (!res.ok) return;
    data = await res.json();
  } catch (e) {
    return;
  }
  // Do not roll back newer stream events or replace a job selected while GET was pending.
  if (state.jobId === jobId && state.es === connection && state.streamRevision === revision) applySnapshot(data);
}

function applySnapshot(data) {
  if (data.job_id && state.jobId && data.job_id !== state.jobId) return;
  state.jobId = data.job_id || state.jobId;
  setStopNote(data.stop_reason || '');   // job cũ mở lại vẫn thấy lý do dừng
  state.total = data.total || 0;
  state.mode = data.mode || 'auto';
  state.running = data.status === 'running';
  state.jobStatus = data.status || '';
  state.workers = data.workers || state.workers;
  if (data.retries) state.retries = data.retries;
  state.elapsedMs = data.elapsed_ms ?? data.duration_ms ?? 0;
  state.elapsedAt = Date.now();
  state.stopping = !!data.stop_requested && state.running;
  setJobBadge(state.jobId);
  setSubmitState();

  const seen = new Set();
  for (const td of data.tasks || []) {
    const t = ensureTask(td.task_id);
    seen.add(t.task_id);
    if ((t.artifact || {}).upi_link !== (td.artifact || {}).upi_link) {
      t.probe = null;
      if (t._monitor) clearInterval(t._monitor);
      t._monitor = null;
    }
    t.index = td.index != null ? td.index : 0;
    t.email = td.email || '';
    t.token = state.tokensJobId === state.jobId ? state.tokens[t.index] || '' : '';
    t.flow = td.flow || null;
    t.flow_label = td.flow_label || null;
    t.steps = (td.steps || []).map(s => ({
      key: s.key,
      label: s.label || s.key,
      status: s.status || 'pending',
      detail: s.detail || '',
      err: s.err || '',
    }));
    t.rawStatus = td.raw_status || td.status || null;
    t.status = normalizeStatus(td.status, td.raw_status);
    t.error = td.error || null;
    t.duration_ms = td.duration_ms != null ? td.duration_ms : null;
    // mốc xong -> tab Success sắp theo thứ tự thành công, không theo dòng token
    t.doneAt = td.done_at || 0;
    t.run = td.run || 1;
    t.egress = td.egress || null;
    t.artifact = td.artifact || null;
    // Kết luận của server về link (succeeded/failed/...) — snapshot mang sẵn,
    // trước đây bị bỏ nên sau F5 card chỉ còn biết kết quả lần dò của chính nó.
    t.linkState = td.link_state || '';
    t.linkZero = !!td.link_zero;
    t.linkFlipAt = td.link_flip_at || 0;
    t.linkNote = td.link_note || '';
    // Kết luận Plus cũng phải đọc từ snapshot: chỉ nhận qua event `task_plus` thì
    // F5 xong badge biến mất, mà khi đã dò xong (plus/not_plus) thì không còn event
    // nào để vẽ lại — badge mất vĩnh viễn.
    t.plusState = td.plus_state || '';
    t.plusNote = td.plus_note || '';
    t.plusCheckedAt = td.plus_checked_at || 0;
    t.created_at = td.created_at || null;
    t.updated_at = td.updated_at || null;
    t.logs = Array.isArray(td.logs) ? td.logs.slice() : [];

    queueTaskRender(t, true, true);
  }
  for (const [id, t] of tasks) {
    if (seen.has(id)) continue;
    if (t._monitor) clearInterval(t._monitor);
    if (t.el) t.el.wrap.remove();
    tasks.delete(id);
  }
  updateCounters();
}

/* --------------------------- job actions --------------------------- */

function attachJob(jobId, snapshot) {
  closeES();
  detachMonitors();
  tasks.clear();
  dirtyTasks.clear();
  els.taskList.innerHTML = '';
  state.jobId = jobId;
  state.total = 0;
  state.workers = 0;
  state.elapsedMs = 0;
  state.elapsedAt = Date.now();
  state.search = '';
  if (els.taskSearch) els.taskSearch.value = '';
  setStopNote('');                 // job mới -> bỏ lý do dừng của job trước
  state.running = true;
  state.stopping = false;
  setJobBadge(jobId);
  markCurrentJob();
  // Bắt đầu theo dõi job -> quay về tab "Tất cả" trước khi đổ task vào.
  // Không làm bước này thì task mới (pending/running) nằm đúng hàng nhưng bị bộ
  // lọc cũ che hết: bấm Chạy xong list trông như trống, phải F5 mới thấy — vì
  // state.filter không lưu vào localStorage nên F5 luôn quay về 'all'.
  setFilter('all');
  setSubmitState();
  els.emptyState.hidden = true;
  updateCounters();
  if (snapshot) applySnapshot(snapshot);
  connectES(jobId);
}

async function runJob() {
  const tokens = getTokens();
  if (!tokens.length) return;
  if (tokens.length > MAX_TOKENS) {
    toast('Max ' + MAX_TOKENS + ' tokens per batch.');
    return;
  }
  const w = readWorkers();
  const workers = w.value;
  if (w.clamped) toast('Worker limit is ' + MAX_WORKERS + ' — you entered ' + w.raw + '.');
  const retriesInput = parseInt(els.retries.value, 10);
  const retries = Math.max(1, Math.min(5, Number.isNaN(retriesInput) ? 3 : retriesInput));
  const payload = {
    tokens: tokens.join('\n'),
    mode: state.mode,
    country: els.country.value.trim() || 'IN',
    promo: els.promo.value,
    workers,
    retries,
    proxies: els.useProxy.checked ? getProxies() : null,
  };

  state.running = true;
  // Đánh dấu đang "chuẩn bị": server sẽ quét + lọc pool proxy trước khi tạo job
  // (nếu bật "Chỉ dùng proxy sạch"), khoảng này có thể kéo dài cả phút mà trước đây
  // không có bất kỳ phản hồi nào -> user tưởng đứng máy. Hiện banner chạy giây.
  state.starting = true;
  state.startingAt = Date.now();
  setSubmitState();
  updateQueueLoading();

  let data;
  try {
    const res = await fetch('/api/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      let msg = 'Failed to start job';
      try { const j = await res.json(); if (j.detail) msg = j.detail; } catch (e) { /* ignore */ }
      throw new Error(msg);
    }
    data = await res.json();
  } catch (err) {
    state.starting = false;
    state.running = false;
    setSubmitState();
    updateQueueLoading();
    toast(err.message || 'Failed to start job');
    return;
  }
  state.starting = false;
  state.tokens = tokens.slice();
  state.tokensJobId = data.job_id;
  attachJob(data.job_id);
}

async function stopJob() {
  // Trước đây hàm này `return` im lặng khi thiếu jobId hoặc khi đang stopping —
  // bấm Stop mà không thấy gì xảy ra, không biết là lệnh không gửi được hay đã gửi
  // nhầm job. Giờ mọi nhánh đều phải nói ra.
  if (!state.jobId) {
    toast('No job attached to stop — press Refresh or pick a job from history');
    return;
  }
  const jid = state.jobId;
  const reSending = state.stopping;
  try {
    const res = await fetch('/api/stop/' + jid, { method: 'POST' });
    if (!res.ok) {
      let message = 'Could not send the stop request';
      try { const body = await res.json(); if (body.detail) message = body.detail; } catch (e) { /* ignore */ }
      toast(message);
      return;
    }
  } catch (e) {
    toast('Connection lost while sending the stop request');
    return;
  }

  // Xác nhận server ĐÃ nhận cho ĐÚNG job này, và còn bao nhiêu task phải chạy nốt.
  if (state.jobId !== jid || !state.running) return;
  const revision = state.streamRevision;
  const connection = state.es;
  let left = null;
  try {
    const res = await fetch('/api/state/' + jid);
    if (res.ok) {
      const d = await res.json();
      if (state.jobId !== jid || !state.running) return;
      if (d.status === 'done' || d.status === 'stopped') {
        if (state.streamRevision === revision && state.es === connection) applySnapshot(d);
        return;
      }
      const running = (d.tasks || []).filter(t => t.status === 'running').length;
      left = running;
      if (d.stop_requested === false) {
        toast('Server has not recorded the stop for job ' + jid + ' — try again');
        return;
      }
    }
  } catch (e) { /* không xác nhận được thì vẫn báo đã gửi */ }

  // A final SSE event can settle the job while either request is still pending.
  if (state.jobId !== jid || !state.running) return;
  onJobStopping();
  const tail = (left === null)
    ? ''
    : ' — ' + left + ' tasks still running must finish first';
  toast((reSending ? 'Re-sent stop for job ' : 'Stop sent for job ') + jid + tail);
  setStopNote('Stopping job ' + jid + tail);
}

/* Đẩy token đang có trong ô "Access tokens" vào queue của JOB HIỆN TẠI.

   Vì sao cần: trước đây muốn chạy thêm acc phải bấm Stop rồi gửi job mới — mất
   phần đang chạy và phải chấm lại pool proxy từ đầu. Nút này nối thẳng vào job
   hiện có (POST /api/append), giữ nguyên pool proxy, và bỏ qua acc trùng.

   Khác nút "Run N checkout tasks": nút Run tạo job MỚI, nút này thêm vào job CŨ.
*/
async function pushToQueue() {
  if (!state.jobId) { toast('No job yet — press Run first'); return; }
  const tokens = getTokens();
  if (!tokens.length) { toast('The Access tokens box is empty'); return; }
  if (els.pushQueue) els.pushQueue.disabled = true;
  try {
    const res = await fetch('/api/append/' + state.jobId, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tokens: tokens.join('\n') }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) { toast(data.detail || 'Push failed'); return; }
    const skipped = data.skipped ? (' — skipped ' + data.skipped + ' duplicates') : '';
    toast('Pushed ' + data.added + ' accounts into the queue' + skipped);
  } catch (e) {
    toast('Push failed');
  } finally {
    updatePushQueue();
  }
}

/* Nút Push CHỈ tồn tại khi job đang chạy.

   Vì sao: push có nghĩa là "thêm vào hàng đợi ĐANG chạy". Chưa chạy gì thì việc
   đúng là bấm Run (tạo job mới) — để nút Push hiện ra lúc đó chỉ gây lẫn.
   Số trên nút là số token đang có trong ô Access tokens.
*/
function updatePushQueue() {
  if (!els.pushQueue) return;
  const running = !!state.running;
  els.pushQueue.hidden = !running;          // không chạy -> không tồn tại
  if (!running) return;
  const n = getTokens().length;
  if (els.pushQueueCount) els.pushQueueCount.textContent = String(n);
  // Job đang dừng thì server từ chối nhận thêm (engine.append_tasks) — chặn ở đây
  // để không bấm được rồi nhận lỗi, và nói rõ lý do.
  if (state.stopping) {
    els.pushQueue.disabled = true;
    els.pushQueue.title = 'Job is stopping — wait for it to finish, then press Run for a new job';
    return;
  }
  els.pushQueue.disabled = n === 0;
  els.pushQueue.title = n === 0
    ? 'The Access tokens box is empty — paste tokens first'
    : 'Push ' + n + ' tokens into the queue of job ' + state.jobId;
}

async function retryTask(t) {
  if (!state.jobId) return;
  const jobId = state.jobId;
  try {
    const res = await fetch('/api/retry/' + jobId + '/' + t.task_id, { method: 'POST' });
    if (!res.ok) { toast('Retry failed'); return; }
  } catch (e) {
    toast('Retry failed');
    return;
  }
  // Completed jobs close their stream. Reconnect so an accepted retry is visible.
  if (state.jobId === jobId && !state.es) connectES(jobId);
  // task_init/task_start SSE events reset the row from server state.
}

async function removeTask(t) {
  const who = t.email || t.task_id;
  const okRemove = await confirmDialog({
    title: 'Remove this task?',
    text: who + '\nThe server copy is deleted too. This cannot be undone.',
    okLabel: 'Remove task',
  });
  if (!okRemove) return;
  // Phải gọi server trước: trước đây chỉ `.remove()` phần tử DOM nên F5 là task
  // quay lại nguyên vẹn — server chưa hề biết.
  if (state.jobId) {
    try {
      const res = await fetch('/api/remove/' + state.jobId + '/' + t.task_id, { method: 'POST' });
      if (!res.ok) {
        const data = await res.json().catch(() => ({}));
        toast(data.detail || 'Could not remove task');
        return;
      }
    } catch (e) {
      toast('Connection lost removing task');
      return;
    }
  }
  if (t.el && t.el.wrap) t.el.wrap.remove();
  if (t._monitor) { clearInterval(t._monitor); t._monitor = null; }
  tasks.delete(t.task_id);
  updateCounters();
  toast('Removed 1 task');
}

/* ------------------------- persistence (localStorage) ---------------- */
// Tu dong luu form -> refresh trang khong mat token / proxy / cau hinh.
// LUU Y: luu dang plaintext trong localStorage cua browser (token la credential).

const STORE_KEY = 'upi-web-form-v1';
// Di trú 1 lần cho form đã lưu trong localStorage: mặc định `retries` đổi 1 -> 3.
// Giá trị 1 đang nằm trong localStorage của người dùng là của MẶC ĐỊNH CŨ (không phải
// họ cố ý chọn), nên nếu không bỏ qua thì mặc định mới sẽ không bao giờ hiện ra.
// Tăng số này khi cần lặp lại việc di trú.
const FORM_VERSION = 2;


function saveForm() {
  try {
    localStorage.setItem(STORE_KEY, JSON.stringify({
      tokens: els.tokens.value,
      proxies: els.proxies.value,
      useProxy: els.useProxy.checked,
      mode: state.mode,
      country: els.country.value,
      promo: els.promo.value,
      workers: els.workers.value,
      retries: els.retries.value,
      view: state.view,
      formVersion: FORM_VERSION,
    }));
  } catch (e) { /* localStorage bi chan / day -> bo qua */ }
}

function loadForm() {
  let saved;
  try {
    const raw = localStorage.getItem(STORE_KEY);
    if (!raw) return false;
    saved = JSON.parse(raw);
  } catch (e) {
    return false;
  }
  if (!saved || typeof saved !== 'object') return false;

  if (typeof saved.tokens === 'string') els.tokens.value = saved.tokens;
  if (typeof saved.proxies === 'string') els.proxies.value = saved.proxies;
  els.useProxy.checked = !!saved.useProxy;
  els.proxies.hidden = !els.useProxy.checked;
  if (saved.country) els.country.value = saved.country;
  // Chỉ nhận promo còn tồn tại trong <select>: giá trị đã lưu mà option đã bị bỏ
  // (vd 'trial' vừa xoá) sẽ làm select rỗng -> gửi PP_PROMO_MODE="" xuống
  // extract_cs, mà ở đó chuỗi rỗng lại fallback thành "campaign" — tức là âm thầm
  // bật promo trong khi user tưởng đang để off.
  if (saved.promo && [...els.promo.options].some(o => o.value === saved.promo)) {
    els.promo.value = saved.promo;
  }
  if (saved.workers) els.workers.value = saved.workers;
  // Chỉ nhận retries đã lưu khi form là bản hiện tại; bản cũ thì để mặc định mới (3).
  if (saved.retries != null && saved.formVersion === FORM_VERSION) {
    els.retries.value = saved.retries;
  }
  if (saved.view === 'grid' || saved.view === 'list') state.view = saved.view;
  if (saved.mode) setMode(saved.mode);

  updatePoolStatus();
  return true;
}

function updatePoolStatus() {
  const n = els.proxies.value.split('\n').map(s => s.trim()).filter(Boolean).length;
  els.poolStatus.hidden = !(els.useProxy.checked && n > 0);
  els.poolCount.textContent = n + ' proxy trong pool';
}

/* ----------------------------- init -------------------------------- */

function bindEvents() {
  if (els.taskSearch) els.taskSearch.addEventListener('input', () => {
    state.search = els.taskSearch.value.trim().toLowerCase();
    updateCounters();
  });
  els.tokens.addEventListener('input', () => {
    updateTokenCount();
    updatePushQueue();
    saveForm();
  });
  els.useProxy.addEventListener('change', () => {
    els.proxies.hidden = !els.useProxy.checked;
    updatePoolStatus();
    saveForm();
  });
  els.proxies.addEventListener('input', () => {
    updatePoolStatus();
    saveForm();
  });
  els.poolClear.addEventListener('click', () => {
    els.proxies.value = '';
    updatePoolStatus();
    saveForm();
    toast('Proxy pool cleared');
  });
  els.country.addEventListener('input', saveForm);
  els.promo.addEventListener('change', saveForm);
  els.workers.addEventListener('input', () => { saveForm(); updateTokenCount(); });
  els.retries.addEventListener('input', saveForm);
  document.querySelectorAll('.view-option').forEach(b => {
    b.addEventListener('click', () => setView(b.dataset.view));
  });
  document.querySelectorAll('.seg').forEach(b => {
    b.addEventListener('click', () => setMode(b.dataset.mode));
  });
  document.querySelectorAll('.tab').forEach(b => {
    b.addEventListener('click', () => setFilter(b.dataset.filter));
  });

  els.btnSuccess.addEventListener('click', () => {
    setFilter(state.filter === 'success' ? 'all' : 'success');
  });
  els.btnRefresh.addEventListener('click', () => {
    if (state.jobId) {
      loadState(state.jobId);
      toast('Refresh requested');
    }
  });
  els.pushQueue.addEventListener('click', pushToQueue);
  els.btnCopyEmails.addEventListener('click', async () => {
    const list = approvedEmails();
    if (!list.length) { toast('No accounts scanned successfully yet'); return; }
    const ok = await copyToClipboard(list.join('\n'));
    toast(ok ? ('Copied ' + list.length + ' emails (one per line)') : 'Copy failed — try again');
  });
  els.btnClearTab.addEventListener('click', () => clearTab(state.filter, els.btnClearTab));
  els.btnNotify.addEventListener('click', toggleNotify);
  // Xem lịch sử job bất kỳ: gõ/dán id rồi Enter hoặc bấm "Xem"
  els.jobIdInput.addEventListener('keydown', (ev) => {
    if (ev.key === 'Enter') { ev.preventDefault(); loadJobById(els.jobIdInput.value); }
  });
  els.jobIdInput.addEventListener('input', () => els.jobIdInput.classList.remove('bad'));
  els.jobIdLoad.addEventListener('click', () => loadJobById(els.jobIdInput.value));
  els.jobIdInput.addEventListener('focus', fillJobIdList);
  // Bấm 1 dòng lịch sử = nạp job đó (điền luôn vào ô Job ID cho khỏi bỡ ngỡ).
  els.historyList.addEventListener('click', (e) => {
    const row = e.target.closest('.history-row');
    if (!row) return;
    els.jobIdInput.value = row.dataset.job;
    loadJobById(row.dataset.job);
  });
  els.submit.addEventListener('click', () => {
    if (state.running) stopJob();
    else runJob();
  });
}

/* ------------------- thống kê global: mã theo ngày ------------------ */

/* Số liệu server gom từ file kết quả trên đĩa (web + CLI) nên sống qua restart,
   và không phụ thuộc job đang mở trên màn hình. */
const GSTATS_MAX_DAYS = 14;

async function loadGlobalStats() {
  if (!els.gstatsChart) return;
  try {
    const res = await fetch('/api/stats/global');
    if (!res.ok) return;
    renderGlobalStats(await res.json());
  } catch (e) {
    // Panel phụ: lỗi mạng ở đây không được làm hỏng phần còn lại của trang.
  }
}

function renderGlobalStats(data) {
  const all = data.days || [];
  const days = all.slice(0, GSTATS_MAX_DAYS).reverse();   // trục thời gian: cũ -> mới
  const total = data.total || 0;
  els.gstatsCodes.textContent = String(total);
  els.gstatsMeta.textContent = total
    ? (data.total_days + (data.total_days === 1 ? ' day · ' : ' days · ')
       + data.total_jobs + (data.total_jobs === 1 ? ' job' : ' jobs'))
    : 'no data yet';

  if (!days.length) {
    els.gstatsChart.innerHTML = '<div class="gstats-empty">No ₹0.00 codes yet</div>';
  } else {
    const max = Math.max.apply(null, days.map(d => d.zero).concat([1]));
    const today = new Date().toISOString().slice(0, 10);
    els.gstatsChart.innerHTML = days.map(d => {
      const cls = 'gbar'
        + (d.zero ? '' : ' is-zero')
        + (d.zero && d.zero === max ? ' is-best' : '')
        + (d.date === today ? ' is-today' : '');
      const tip = d.date + ': ' + d.zero + ' ₹0.00 codes · '
        + (d.paid || 0) + ' paid · '
        + d.jobs + (d.jobs === 1 ? ' job' : ' jobs');
      const pct = d.zero ? Math.max(4, Math.round((d.zero / max) * 100)) : 2;
      return '<div class="' + cls + '" title="' + tip + '">'
        + '<span class="gbar-value">' + d.zero + '</span>'
        + '<div class="gbar-track"><div class="gbar-fill" style="height:' + pct + '%"></div></div>'
        + '<span class="gbar-date">' + d.date.slice(5) + '</span>'
        + '</div>';
    }).join('');
  }
}

async function init() {
  const restored = loadForm();
  bindEvents();
  bindModal();
  state.tokens = getTokens();
  document.body.dataset.filter = state.filter;
  setView(state.view, false);
  if (!restored) setMode('auto');
  setJobBadge(null);
  setNotify(notifyOn());
  updateTokenCount();
  updatePoolStatus();
  updateCounters();
  updateEmptyState();
  if (restored && getTokens().length) toast('Restored saved form');

  loadGlobalStats();   // panel phụ: chạy song song, không chặn phần còn lại

  // Auto-attach to the newest running job if one exists.
  try {
    const res = await fetch('/api/jobs');
    if (res.ok) {
      const data = await res.json();
      renderJobHistory(data.jobs || []);
      const running = (data.jobs || []).filter(j => j.status === 'running');
      // /api/jobs trả MỚI NHẤT trước, nên job đang chạy mới nhất là running[0].
      // Lấy running[length-1] là gắn vào job CŨ NHẤT — sai đúng lúc có nhiều job
      // đang chạy, và làm nút Stop nhắm vào job khác với cái đang xem.
      const latest = running[0] || (data.jobs || [])[0];
      if (latest) attachJob(latest.job_id);
    }
  } catch (e) {
    // Backend not reachable — stay on the empty state.
  }
}

document.addEventListener('DOMContentLoaded', init);

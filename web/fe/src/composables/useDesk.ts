/**
 * Store của desk: danh sách job, job đang xem, task (Map theo task_id) và kết nối SSE.
 *
 * Vì sao Map chứ không phải mảng: server đẩy `state` snapshot (hàng nghìn task) + event lẻ
 * (`step`, `task_done`, `task_link`…) — patch theo task_id là O(1), không phải dò mảng.
 */
import { computed, reactive, ref, shallowRef } from 'vue'
import {
  api,
  openStream,
  type JobSnapshot,
  type JobSummary,
  type StreamEvent,
  type TaskState,
} from '@/lib/api'
import { hasLink, matchesTab, type TabKey } from '@/lib/taskView'
import { playSuccessSound } from '@/composables/useNotifySound'

const jobs = ref<JobSummary[]>([])
const job = shallowRef<JobSnapshot | null>(null)
const tasks = reactive(new Map<string, TaskState>())
const order = ref<string[]>([])
const connected = ref(false)
const stopping = ref(false)
/** Link vừa trích xuất xong — dùng để bắn toast + làm nổi card (tự xoá sau 20 s). */
const lastLink = ref<{ taskId: string; email: string; at: number } | null>(null)
const freshLinks = reactive(new Set<string>())
const loading = ref(false)
const error = ref('')
const tab = ref<TabKey>('all')
const query = ref('')
const view = ref<'grid' | 'list'>('grid')
let handle: { close: () => void } | null = null

function patchTask(taskId: string, patch: Record<string, unknown>): void {
  const current = tasks.get(taskId)
  if (!current) return
  Object.assign(current, patch)
}

function applySnapshot(snapshot: JobSnapshot): void {
  job.value = snapshot
  stopping.value = Boolean(snapshot.stop_requested) && String(snapshot.status || '') === 'running'
  const list = Array.isArray(snapshot.tasks) ? snapshot.tasks : []
  const seen = new Set<string>()
  for (const task of list) {
    const id = String(task.task_id)
    seen.add(id)
    const existing = tasks.get(id)
    if (existing) Object.assign(existing, task)
    else tasks.set(id, task)
  }
  for (const id of [...tasks.keys()]) if (!seen.has(id)) tasks.delete(id)
  order.value = list.map((t) => String(t.task_id))
}

function handleEvent(ev: StreamEvent): void {
  const taskId = typeof ev.task_id === 'string' ? ev.task_id : ''
  switch (ev.type) {
    case 'state':
      if (ev.job && typeof ev.job === 'object') applySnapshot(ev.job as JobSnapshot)
      break
    case 'job_progress':
      if (job.value) {
        job.value = {
          ...job.value,
          done: Number(ev.done ?? job.value.done),
          ok: Number(ev.ok ?? job.value.ok),
          fail: Number(ev.fail ?? job.value.fail),
          skipped: Number(ev.skipped ?? job.value.skipped ?? 0),
          // Server gửi kèm running/queued: dùng luôn, khỏi phải suy ra từ Map task (dễ lệch
          // khi event task_start bị rơi hoặc task ngoài trang chưa có trong Map).
          running: Number(ev.running ?? job.value.running ?? 0),
          queued: Number(ev.queued ?? job.value.queued ?? 0),
        }
      }
      break
    case 'job_stopping':
      // Server đã nhận lệnh Stop; task đang chạy còn phải kết thúc (hoặc dừng ở checkpoint
      // kế tiếp), task trong hàng đợi sẽ bị bỏ. UI phải nói rõ trạng thái này, nếu không
      // người dùng bấm Stop mà thấy "Running" không đổi thì tưởng nút không tác dụng.
      stopping.value = true
      break
    case 'job_done':
      stopping.value = false
      void refreshJobs()
      break
    case 'task_start':
      // BẮT BUỘC: nếu bỏ qua event này, task đã chạy nhưng UI vẫn coi là "queued" và số
      // Running đứng yên ở giá trị của snapshot đầu tiên (đo thật: server 300 running,
      // UI hiện 162). Đây là lý do "Running không bù vào".
      if (taskId) {
        patchTask(taskId, {
          status: 'running',
          run: Number(ev.run || 1),
          flow: (ev.flow as string) || undefined,
        })
      }
      break
    case 'task_init':
      if (taskId) {
        const existing = tasks.get(taskId)
        if (existing) {
          Object.assign(existing, {
            status: 'pending',
            email: String(ev.email || existing.email || ''),
            steps: (ev.steps as TaskState['steps']) || existing.steps,
          })
        } else {
          tasks.set(taskId, {
            task_id: taskId,
            index: Number(ev.index || 0),
            email: String(ev.email || ''),
            status: 'pending',
            steps: (ev.steps as TaskState['steps']) || [],
          })
          order.value = [...order.value, taskId]
        }
      }
      break
    case 'task_removed':
      if (taskId) {
        tasks.delete(taskId)
        order.value = order.value.filter((id) => id !== taskId)
      }
      break
    case 'tasks_cleared':
      tasks.clear()
      order.value = []
      break
    case 'resync':
      // Server báo đã rơi event (hàng đợi SSE đầy) -> lấy lại snapshot để không bị lệch số.
      void openJob(job.value?.job_id)
      break
    case 'task_done':
      if (taskId) {
        patchTask(taskId, {
          status: ev.skipped ? 'skipped' : ev.ok ? 'done' : 'fail',
          raw_status: ev.status,
          error: ev.error ?? null,
          duration_ms: ev.duration_ms,
          done_at: ev.done_at,
          net_out: ev.net_out,
          net_in: ev.net_in,
        })
      }
      break
    case 'step':
      if (taskId) {
        const task = tasks.get(taskId)
        if (task) {
          const steps = Array.isArray(task.steps) ? task.steps : []
          const idx = steps.findIndex((s) => s.key === ev.key)
          const next = idx >= 0 ? { ...steps[idx], status: String(ev.status), err: String(ev.err ?? '') } : null
          if (next && idx >= 0) {
            const copy = steps.slice()
            copy[idx] = next
            patchTask(taskId, { steps: copy })
          }
        }
      }
      break
    case 'task_billing':
      // Server bắt được hoá đơn flow điền vào PM billing_details -> cập nhật ngay lên card.
      if (taskId) {
        const billing = (ev.billing || {}) as Record<string, unknown>
        patchTask(taskId, {
          billing_tier: String(billing.billing_tier || ''),
          billing_check: String(billing.billing_check || ''),
          billing_name: String(billing.billing_name || ''),
          billing_city: String(billing.billing_city || ''),
          billing_pin: String(billing.billing_pin || ''),
        })
      }
      break
    case 'egress':
      if (taskId) patchTask(taskId, { egress: ev as Record<string, unknown> })
      break
    case 'task_artifact':
      // Server phát field PHẲNG: {type, task_id, upi_link, qr_png, qr_svg, amount_minor, ...}
      // (engine: `{"type": "task_artifact", "task_id": ..., **task.artifact}`). Trước đây đọc
      // `ev.artifact` (undefined) nên QR/link KHÔNG hiện cho tới khi reload lấy snapshot.
      if (taskId) {
        const { type: _type, task_id: _taskId, ...fields } = ev as Record<string, unknown>
        const current = tasks.get(taskId)
        const hadLink = Boolean(current?.artifact?.upi_link)
        patchTask(taskId, { artifact: { ...(current?.artifact || {}), ...fields } })
        if (fields.upi_link) {
          void probeExpiry(taskId, String(fields.upi_link))
          // Link MỚI (chưa từng có) -> toast + nhấp nháy card để người dùng biết ngay.
          if (!hadLink) markFreshLink(taskId)
        }
      }
      break
    case 'task_link':
      if (taskId) {
        // Link vừa sang "waiting" -> lấy hạn dùng để hiện countdown ngay, không đợi vòng quét.
        if (String(ev.link_state || '') === 'waiting') {
          const url = String(tasks.get(taskId)?.artifact?.upi_link || '')
          if (url) void probeExpiry(taskId, url)
        }
        patchTask(taskId, {
          link_state: ev.link_state,
          link_checked_at: ev.link_checked_at,
          link_flip_at: ev.link_flip_at,
          link_fam: ev.link_fam,
          link_zero: ev.link_zero,
          link_note: ev.link_note,
        })
      }
      break
    case 'task_link_checked':
      if (taskId) patchTask(taskId, { link_checked_at: ev.link_checked_at })
      break
    case 'task_plus':
      if (taskId) {
        patchTask(taskId, {
          plus_state: ev.plus_state,
          plus_note: ev.plus_note,
          plus_checked_at: ev.plus_checked_at,
        })
      }
      break
    case 'task_egress':
    case 'egress':
      if (taskId) patchTask(taskId, { egress: (ev.egress as Record<string, unknown>) ?? null })
      break
    default:
      break
  }
}

async function refreshJobs(): Promise<void> {
  try {
    const data = await api.jobs()
    const list = [...data.jobs]
    // Giữ job ĐANG XEM trong danh sách: deep link `?job=` có thể trỏ tới job cũ không nằm trong
    // 20 job mới nhất, và SSE `job_done` gọi refreshJobs() sẽ làm mất option -> select trống.
    const current = job.value
    if (current?.job_id && !list.some((j) => j.job_id === current.job_id)) {
      list.unshift({
        job_id: current.job_id,
        status: String(current.status || ''),
        total: Number(current.total || 0),
        done: Number(current.done || 0),
        ok: Number(current.ok || 0),
        fail: Number(current.fail || 0),
        skipped: Number(current.skipped || 0),
        started_at: current.started_at,
      })
    }
    jobs.value = list
  } catch (exc) {
    error.value = exc instanceof Error ? exc.message : String(exc)
  }
}

/** Chọn job để xem: giữ job đang mở nếu còn tồn tại, không thì lấy job mới nhất. */
async function openJob(jobId?: string): Promise<void> {
  handle?.close()
  handle = null
  loading.value = true
  error.value = ''
  try {
    // Job có thể không nằm trong 20 job mới nhất (deep link `?job=` tới job cũ) — vẫn mở được.
    // Mặc định chọn job MỚI NHẤT còn task (job đã bị Clear hết thì bỏ qua, đừng mở job rỗng).
    const firstWithTasks = jobs.value.find((j) => Number(j.total || 0) > 0)
    const target = jobId || firstWithTasks?.job_id || jobs.value[0]?.job_id
    if (!target) {
      job.value = null
      tasks.clear()
      order.value = []
      return
    }
    syncUrl(target)
    const snapshot = await api.state(target)
    applySnapshot(snapshot)
    if (!jobs.value.some((j) => j.job_id === target)) {
      jobs.value = [
        {
          job_id: target,
          status: String(snapshot.status || ''),
          total: Number(snapshot.total || 0),
          done: Number(snapshot.done || 0),
          ok: Number(snapshot.ok || 0),
          fail: Number(snapshot.fail || 0),
          skipped: Number(snapshot.skipped || 0),
          started_at: snapshot.started_at,
        },
        ...jobs.value,
      ]
    }
    handle = openStream(target, handleEvent, (open) => {
      connected.value = open
    })
  } catch (exc) {
    error.value = exc instanceof Error ? exc.message : String(exc)
  } finally {
    loading.value = false
  }
}

/** Job đang xem nằm trên URL (`?job=<id>`) để refresh không nhảy về job mới nhất. */
function jobFromUrl(): string {
  try {
    return new URLSearchParams(window.location.search).get('job') || ''
  } catch {
    return ''
  }
}

function syncUrl(jobId: string): void {
  try {
    const url = new URL(window.location.href)
    url.searchParams.set('job', jobId)
    window.history.replaceState(null, '', url)
  } catch {
    /* môi trường không có history — bỏ qua */
  }
}

async function bootstrap(): Promise<void> {
  await refreshJobs()
  // Luôn ưu tiên job trên URL (kể cả job cũ không nằm trong 20 job mới nhất): openJob() sẽ
  // tự fetch snapshot và thêm vào danh sách chọn.
  await openJob(jobFromUrl() || undefined)
}

const counts = computed(() => {
  const base = { all: 0, running: 0, queued: 0, success: 0, fail: 0, skipped: 0, stopped: 0 }
  for (const task of tasks.values()) {
    base.all += 1
    const status = String(task.status || 'pending')
    if (status === 'running') base.running += 1
    else if (status === 'pending') base.queued += 1
    else if (status === 'skipped') base.skipped += 1
    else if (status === 'stopped') base.stopped += 1
    else if (status === 'fail') base.fail += 1
    if (status === 'done' && hasLink(task)) base.success += 1
  }
  // running/queued lấy từ server (job.running/job.queued) khi có: số đếm từ Map chỉ đúng cho
  // những task đã nhận được event, còn server là nguồn chuẩn.
  const live = job.value
  if (live) {
    if (typeof live.running === 'number') base.running = live.running
    if (typeof live.queued === 'number') base.queued = live.queued
    if (typeof live.skipped === 'number') base.skipped = live.skipped
  }
  return base
})

const visibleTasks = computed<TaskState[]>(() => {
  const needle = query.value.trim().toLowerCase()
  const out: TaskState[] = []
  for (const id of order.value) {
    const task = tasks.get(id)
    if (!task) continue
    if (!matchesTab(task, tab.value)) continue
    if (needle) {
      const hay = `${task.email || ''} ${task.task_id || ''}`.toLowerCase()
      if (!hay.includes(needle)) continue
    }
    out.push(task)
  }
  // Thứ tự: cái trích xuất RA SỚM NHẤT nằm trên cùng (đúng thứ tự thời gian), để dễ đối
  // chiếu với lúc link được tạo. Task chưa xong (không có done_at) nằm cuối, xếp theo index.
  // Riêng tab queued/running giữ nguyên thứ tự hàng đợi (index) vì chúng chưa "ra" gì cả.
  if (tab.value !== 'queued' && tab.value !== 'running') {
    out.sort((a, b) => {
      const ta = Number(a.done_at || 0)
      const tb = Number(b.done_at || 0)
      if (ta && tb && ta !== tb) return ta - tb
      if (ta && !tb) return -1
      if (!ta && tb) return 1
      return Number(a.index || 0) - Number(b.index || 0)
    })
  }
  return out
})

/** Đánh dấu task vừa có link: toast (qua lastLink) + card nhấp nháy 20 s. */
function markFreshLink(taskId: string): void {
  const task = tasks.get(taskId)
  freshLinks.add(taskId)
  // KÊU ngay khi link được trích ra (đây mới là lúc cần biết để vào lấy QR), không đợi
  // tới khi khách quét xong.
  playSuccessSound()
  lastLink.value = {
    taskId,
    email: String(task?.email || ''),
    at: Math.floor(Date.now() / 1000),
  }
  setTimeout(() => freshLinks.delete(taskId), 20_000)
}

/** Nhảy tới card của 1 task: mở tab Success, xoá search, cuộn tới và làm nổi bật. */
async function focusTask(taskId: string): Promise<void> {
  tab.value = 'success'
  query.value = ''
  await new Promise((resolve) => setTimeout(resolve, 60))
  const el = document.querySelector(`[data-task-id="${taskId}"]`)
  if (el) {
    el.scrollIntoView({ block: 'center', behavior: 'smooth' })
    el.classList.add('ring-2', 'ring-chart-2')
    setTimeout(() => el.classList.remove('ring-2', 'ring-chart-2'), 2500)
  }
}

/** ControlPanel gọi ngay sau khi bấm Stop (optimistic) / khi API lỗi để trả lại trạng thái. */
export function setStopping(next: boolean): void {
  stopping.value = next
}

/** Thời điểm dò `expires_at` gần nhất cho từng task (tránh gọi lặp). */
const expiryChecked = new Map<string, number>()
const EXPIRY_REFRESH_SECS = 20

/** Hỏi server hạn dùng trang instructions của 1 link (server cache 120 s). */
async function probeExpiry(taskId: string, url: string): Promise<void> {
  const task = tasks.get(taskId)
  if (!task || !url) return
  if (String(task.link_state || '') !== 'waiting') return
  const last = expiryChecked.get(taskId) || 0
  if (Date.now() / 1000 - last < EXPIRY_REFRESH_SECS) return
  expiryChecked.set(taskId, Date.now() / 1000)
  try {
    const probe = await api.artifact(url)
    if (probe?.expires_at) patchTask(taskId, { expires_at: probe.expires_at })
  } catch {
    /* link chết/proxy lỗi -> lần sau thử lại, không làm hỏng UI */
  }
}

/** Quét các link đang "waiting" để lấy hạn dùng (chạy mỗi 10 s, tối đa 12 link/lần). */
async function refreshExpiries(): Promise<void> {
  let budget = 12
  for (const [id, task] of tasks) {
    if (budget <= 0) break
    if (String(task.link_state || '') !== 'waiting') continue
    const url = String(task.artifact?.upi_link || '')
    if (!url) continue
    const last = expiryChecked.get(id) || 0
    if (Date.now() / 1000 - last < EXPIRY_REFRESH_SECS) continue
    budget -= 1
    void probeExpiry(id, url)
  }
}
let expiryTimer: ReturnType<typeof setInterval> | null = null

export function useDesk() {
  if (!expiryTimer) {
    expiryTimer = setInterval(() => {
      void refreshExpiries()
    }, 10_000)
  }
  return {
    jobs,
    job,
    tasks,
    order,
    connected,
    stopping,
    lastLink,
    freshLinks,
    focusTask,
    loading,
    error,
    tab,
    query,
    view,
    counts,
    visibleTasks,
    bootstrap,
    openJob,
    refreshJobs,
  }
}

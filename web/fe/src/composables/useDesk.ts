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
        }
      }
      break
    case 'job_done':
      void refreshJobs()
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
    case 'task_artifact':
      if (taskId) patchTask(taskId, { artifact: ev.artifact })
      break
    case 'task_link':
      // Có link vừa chuyển sang trạng thái thành công -> kêu 1 tiếng (như bản vanilla).
      if (String(ev.link_state || '') === 'succeeded') playSuccessSound()
      if (taskId) {
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
  // Success/Failed xếp task mới xong lên trước cho dễ nhìn.
  if (tab.value !== 'all' && tab.value !== 'queued') {
    out.sort((a, b) => Number(b.done_at || 0) - Number(a.done_at || 0))
  }
  return out
})

export function useDesk() {
  return {
    jobs,
    job,
    tasks,
    order,
    connected,
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

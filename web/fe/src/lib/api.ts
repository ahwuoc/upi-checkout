/** Kiểu dữ liệu + gọi API của desk (FastAPI ở cùng origin, SSE qua /api/stream). */

export type TaskStatus = 'pending' | 'running' | 'done' | 'fail' | 'stopped'

export interface Step {
  key: string
  label: string
  status: string
  detail?: string
  err?: string
}

export interface Artifact {
  upi_link?: string
  qr?: string
  hosted_url?: string
  amount_display?: string
  [k: string]: unknown
}

export interface TaskState {
  task_id: string
  index?: number
  email?: string
  flow?: string
  session?: string
  provider?: string
  status?: TaskStatus
  raw_status?: string
  error?: string | null
  duration_ms?: number
  run?: number
  steps?: Step[]
  artifact?: Artifact | null
  egress?: Record<string, unknown> | null
  skipped_reason?: string
  link_state?: string
  link_checked_at?: number
  link_flip_at?: number
  link_fam?: string
  link_zero?: boolean
  link_note?: string
  plus_state?: string
  plus_checked_at?: number
  plus_note?: string
  geo_state?: string
  geo_city?: string
  billing_tier?: string
  billing_check?: string
  billing_city?: string
  billing_pin?: string
  net_out?: number
  net_in?: number
  net_requests?: number
  done_at?: number
  [k: string]: unknown
}

export interface JobSnapshot {
  job_id: string
  status?: string
  mode?: string
  country?: string
  promo?: string
  workers?: number
  retries?: number
  total?: number
  done?: number
  ok?: number
  fail?: number
  skipped?: number
  running?: number
  queued?: number
  elapsed_ms?: number
  started_at?: string
  stop_reason?: string
  write_error?: string
  stop_requested?: boolean
  tasks?: TaskState[]
}

export interface JobSummary {
  job_id: string
  status: string
  total: number
  done: number
  ok: number
  fail: number
  skipped?: number
  started_at?: string
}

export interface StreamEvent {
  type: string
  [k: string]: unknown
}

/** Lỗi HTTP kèm body để hiển thị đúng lý do (vd 400 "đĩa còn < 1024 MB"). */
export class ApiError extends Error {
  status: number

  constructor(message: string, status: number) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

async function getJson<T>(url: string): Promise<T> {
  const resp = await fetch(url, { headers: { Accept: 'application/json' } })
  if (!resp.ok) {
    let detail = resp.statusText
    try {
      const body = (await resp.json()) as { detail?: string }
      if (body?.detail) detail = String(body.detail)
    } catch {
      /* body không phải JSON — giữ statusText */
    }
    throw new ApiError(detail, resp.status)
  }
  return (await resp.json()) as T
}

export interface RunPayload {
  tokens: string
  mode: string
  country: string
  promo: string
  workers: number
  retries: number
  proxies?: string | null
  mix: boolean
  filter_proxies: boolean
  stripe_telemetry: boolean
  dump_traffic: boolean
  prefilter: boolean
}

export interface RunResult {
  job_id: string
  total: number
}

async function postJson<T>(url: string, body: unknown): Promise<T> {
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body),
  })
  if (!resp.ok) {
    let detail = resp.statusText
    try {
      const data = (await resp.json()) as { detail?: string | Array<{ msg?: string }> }
      if (typeof data?.detail === 'string') detail = data.detail
      else if (Array.isArray(data?.detail)) detail = data.detail.map((d) => d.msg || '').join('; ')
    } catch {
      /* body không phải JSON */
    }
    throw new ApiError(detail, resp.status)
  }
  return (await resp.json()) as T
}

export const api = {
  jobs: () => getJson<{ jobs: JobSummary[] }>('/api/jobs'),
  state: (jobId: string) => getJson<JobSnapshot>(`/api/state/${jobId}`),
  retry: (jobId: string, taskId: string) =>
    getJson<unknown>(`/api/retry/${jobId}/${taskId}`),
  stats: () => getJson<Record<string, unknown>>('/api/stats/global'),
  run: (payload: RunPayload) => postJson<RunResult>('/api/run', payload),
  prefilter: (body: { tokens: string; workers: number; proxies?: string | null }) =>
    postJson<{ id: string; total: number; proxies: number }>('/api/prefilter', body),
  prefilterStatus: (id: string) => getJson<PrefilterStatus>(`/api/prefilter/${id}`),
  prefilterStop: (id: string) => postJson<unknown>(`/api/prefilter/${id}/stop`, {}),
  stop: (jobId: string) => postJson<unknown>(`/api/stop/${jobId}`, {}),
}

/** Ảnh QR / artifact đi qua server để không lộ URL Stripe ra ngoài + có cache. */
export function artifactUrl(url: string): string {
  return `/api/artifact?url=${encodeURIComponent(url)}`
}

export interface PrefilterRow {
  index: number
  fingerprint: string
  email: string
  verdict: string
  label: string
  detail: string
  ms: number
  dropped: boolean
}

export interface PrefilterStatus {
  id: string
  state: 'running' | 'done' | 'stopped'
  total: number
  checked: number
  kept: number
  dropped: number
  counts: Record<string, number>
  elapsed_ms: number
  results: PrefilterRow[]
}

export interface StreamHandle {
  close: () => void
}

/** Mở SSE cho 1 job. `onEvent` nhận mọi event; `onError` khi mất kết nối. */
export function openStream(
  jobId: string,
  onEvent: (ev: StreamEvent) => void,
  onState: (open: boolean) => void,
): StreamHandle {
  const source = new EventSource(`/api/stream/${jobId}`)
  source.onopen = () => onState(true)
  source.onerror = () => onState(false)
  source.onmessage = (msg: MessageEvent<string>) => {
    try {
      onEvent(JSON.parse(msg.data) as StreamEvent)
    } catch {
      /* frame hỏng thì bỏ qua, lần sau có snapshot mới */
    }
  }
  return { close: () => source.close() }
}

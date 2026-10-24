/**
 * Trạng thái hiển thị cho UI — suy ra từ dữ liệu task của desk.
 *
 * Giữ đúng ngữ nghĩa bản vanilla (web/static/app.js) để hai bản hiện cùng một kết luận:
 *  • `link_state` = trạng thái uỷ nhiệm do server dò trang instructions
 *  • thiếu `link_state` mà lỗi là "unverified/SSLError/..." -> "chưa kiểm tra được"
 */
import type { TaskState } from '@/lib/api'

export type ChipTone = 'ok' | 'warn' | 'fail' | 'idle' | 'info'

export interface Chip {
  label: string
  tone: ChipTone
}

/** Nhãn + màu cho chip trạng thái link (góc dưới card). */
export function linkChip(task: TaskState): Chip {
  const state = String(task.link_state || '')
  const status = String(task.status || 'pending')
  if (!state) {
    const err = String(task.error || '')
    if (task.artifact?.upi_link && /unverified|SSLError|Timeout|ProxyError|unreachable/i.test(err)) {
      return { label: 'Not verified yet', tone: 'idle' }
    }
    // Chưa có link: chip nói trạng thái TASK (running/fail/queued/stopped), không nói "No link"
    // — vì "No link" mơ hồ, còn "Failed"/"Running" là đúng thứ người xem cần.
    if (status === 'skipped') return { label: 'Skipped', tone: 'idle' }
    if (status === 'running') return { label: 'Running', tone: 'info' }
    if (status === 'fail') return { label: 'Failed', tone: 'fail' }
    if (status === 'stopped') return { label: 'Stopped', tone: 'idle' }
    if (status === 'pending') return { label: 'Queued', tone: 'idle' }
    return { label: task.artifact?.upi_link ? 'Link created' : 'Done', tone: 'idle' }
  }
  switch (state) {
    case 'succeeded':
      return { label: 'Payment successful', tone: 'ok' }
    case 'waiting':
      return { label: 'Awaiting payment', tone: 'warn' }
    case 'failed':
      return { label: 'Failed', tone: 'fail' }
    case 'canceled':
      return { label: 'Cancelled', tone: 'fail' }
    case 'expired':
      return { label: 'Expired', tone: 'warn' }
    default:
      return { label: state, tone: 'idle' }
  }
}

/** Badge "Plus Success" chỉ hiện khi acc đã lên Plus. */
export function plusChip(task: TaskState, jobRunning = false): Chip | null {
  const state = String(task.plus_state || '')
  if (state === 'plus') return { label: 'Plus Success', tone: 'ok' }
  // "checking" chỉ có nghĩa khi job đang chạy; job đã xong thì đó là trạng thái bỏ dở.
  if (state === 'checking') return jobRunning ? { label: 'Checking Plus…', tone: 'info' } : null
  if (state === 'not_plus') return { label: 'Not Plus', tone: 'idle' }
  return null
}

/** Tuổi lần dò link gần nhất: "vừa xong" / "4m" / "2h" / "3d". */
export function checkedAge(ts?: number): string {
  if (!ts) return ''
  const secs = Math.max(0, Math.floor(Date.now() / 1000) - ts)
  if (secs < 60) return 'vừa xong'
  if (secs < 3600) return `${Math.floor(secs / 60)}m`
  if (secs < 86400) return `${Math.floor(secs / 3600)}h`
  return `${Math.floor(secs / 86400)}d`
}

/** Lý do fail ngắn gọn để hiện trên card (log gốc vẫn xem được qua tooltip). */
export function failReason(task: TaskState): string {
  const err = String(task.error || '')
  if (!err) return ''
  if (/promo_not_eligible/i.test(err)) return 'Not eligible for ₹0 promo'
  if (/Cloudflare|cf-mitigated/i.test(err)) return 'Cloudflare blocked checkout (403)'
  if (/risk decline/i.test(err)) return 'Stripe risk decline'
  if (/Billing country must match/i.test(err)) return 'Billing country mismatch'
  if (/timeout/i.test(err)) return 'Timed out'
  if (/TLS|transport|ProxyError/i.test(err)) return 'Proxy transport error'
  if (/\b401\b/.test(err)) return 'Access token invalid (401)'
  if (/\b403\b/.test(err)) return 'Blocked (403)'
  if (/unverified/i.test(err)) return 'Link not verified yet'
  return err.length > 90 ? `${err.slice(0, 90)}…` : err
}

/** Task có link (success tab) hay không. */
export function hasLink(task: TaskState): boolean {
  return Boolean(task.artifact?.upi_link)
}

/** Link đã "chết" (khách không trả / hết hạn / huỷ) — task đó phải nằm ở tab Failed. */
export function linkDied(task: TaskState): boolean {
  return ['failed', 'expired', 'canceled'].includes(String(task.link_state || ''))
}

/** Task "thành công" = có link và link CHƯA chết (succeeded, waiting — waiting vẫn cần QR). */
export function isSuccessTask(task: TaskState): boolean {
  return hasLink(task) && !linkDied(task)
}

export type TabKey = 'all' | 'running' | 'queued' | 'success' | 'fail' | 'skipped' | 'stopped'

export function matchesTab(task: TaskState, tab: TabKey): boolean {
  const status = String(task.status || 'pending')
  switch (tab) {
    case 'all':
      return true
    case 'running':
      return status === 'running'
    case 'queued':
      return status === 'pending'
    case 'success':
      // Task tạo được link nhưng uỷ nhiệm THẤT BẠI thì không tính là success — nếu không,
      // tab Success (và nút Copy emails) sẽ lẫn acc fail (đo thật: 5 card nhưng chỉ 4 acc
      // Payment successful).
      return isSuccessTask(task)
    case 'fail':
      return status === 'fail' || linkDied(task)
    case 'skipped':
      return status === 'skipped'
    case 'stopped':
      return status === 'stopped'
    default:
      return true
  }
}

export function fmtDuration(ms?: number): string {
  if (!ms || ms < 0) return ''
  const secs = Math.round(ms / 1000)
  if (secs < 60) return `${secs}s`
  const mins = Math.floor(secs / 60)
  const rest = secs % 60
  return rest ? `${mins}m ${rest}s` : `${mins}m`
}

export function fmtAmountMinor(minor?: number): string {
  if (typeof minor !== 'number') return ''
  return `₹${(minor / 100).toFixed(2)}`
}

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useNow } from '@/composables/useNow'
import { ChevronDown, Copy, ExternalLink, Hourglass, QrCode } from '@lucide/vue'
import { Card, CardContent, CardFooter, CardHeader } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import StatusChip from '@/components/StatusChip.vue'
import { useDesk } from '@/composables/useDesk'
import type { TaskState } from '@/lib/api'
import { checkedAge, failReason, linkChip, plusChip } from '@/lib/taskView'

const desk = useDesk()

const props = defineProps<{
  task: TaskState
  view: 'grid' | 'list'
  jobRunning?: boolean
}>()

const link = computed(() => String(props.task.artifact?.upi_link || ''))
// QR là URL ảnh THẬT trên qr.stripe.com (không đi qua /api/artifact — endpoint đó trả JSON
// trạng thái). Trạng thái cuối (khách đã duyệt / link chết) thì Stripe xoá ảnh (410 Gone) nên
// KHÔNG hiện nữa — giống bản vanilla.
const TERMINAL = ['succeeded', 'failed', 'canceled', 'expired']
const qr = computed(() => {
  const art = props.task.artifact as Record<string, unknown> | null | undefined
  if (TERMINAL.includes(String(props.task.link_state || ''))) return ''
  return String(art?.qr_png || art?.qr_svg || art?.qr || '')
})
const qrSrc = computed(() => qr.value)
// Stripe trả 410 Gone cho QR của uỷ nhiệm đã hết hạn/xoá -> đừng để khung trắng vỡ ảnh.
const qrBroken = ref(false)
watch(qrSrc, () => {
  qrBroken.value = false
})
const chip = computed(() => linkChip(props.task))
const plus = computed(() => plusChip(props.task, props.jobRunning === true))
const age = computed(() => checkedAge(props.task.link_checked_at))
const reason = computed(() => failReason(props.task))

const STEP_ICON: Record<string, string> = {
  pending: '○',
  active: '◐',
  done: '✓',
  fail: '✕',
  skip: '⊘',
}
const STEP_TONE: Record<string, string> = {
  pending: 'text-muted-foreground/60',
  active: 'text-chart-3',
  done: 'text-chart-2',
  fail: 'text-destructive',
  skip: 'text-muted-foreground/60',
}

const steps = computed(() => (Array.isArray(props.task.steps) ? props.task.steps : []))
const flowLabel = computed(() => {
  const flow = String(props.task.flow || '')
  if (flow === 'cs') return 'FLOW CS'
  if (flow === 'oaics') return 'FLOW OAICS'
  return String(props.task.status) === 'running' ? 'DETECTING FLOW' : '—'
})

/** Bước đang chạy / bước lỗi gần nhất — hiện ngay trên card khi chưa mở chi tiết. */
const currentStep = computed(() => {
  const list = steps.value
  const active = list.find((step) => step.status === 'active')
  if (active) return active
  const failed = list.find((step) => step.status === 'fail')
  if (failed) return failed
  const done = [...list].reverse().find((step) => step.status === 'done')
  return done || null
})
const stepsOpen = ref(false)
const stepSummary = computed(() => {
  const list = steps.value
  if (!list.length) return ''
  const done = list.filter((step) => step.status === 'done' || step.status === 'skip').length
  return `${done}/${list.length}`
})

const TIER_LABEL: Record<string, string> = {
  tier2: 'IP + verified PIN',
  tier1: 'IP + reverse geocode',
  'hardcoded-list': 'hardcoded list — does NOT match the exit IP',
}
const CHECK_LABEL: Record<string, string> = {
  ok: 'PIN matches city/state and the exit region',
  fixed: 'city/state corrected from the PIN',
  'fixed-pin': 'invalid PIN replaced with a verified one',
  'bad-pin': 'PIN not found in India Post',
  'mismatch-exit': 'PIN state ≠ proxy exit state',
  unknown: 'not checked',
}

/** Hoá đơn mà flow điền vào billing_details + vùng IP exit (bản vanilla gọi là "Billing details"). */
const billingRows = computed<Array<[string, string]>>(() => {
  const t = props.task
  const rows: Array<[string, string]> = []
  const tier = String(t.billing_tier || '')
  if (tier) rows.push(['Address source', TIER_LABEL[tier] || tier])
  const check = String(t.billing_check || '')
  if (check) rows.push(['Address check', CHECK_LABEL[check] || check])
  if (t.billing_name) rows.push(['Billing name', String(t.billing_name)])
  const city = [t.billing_city, t.billing_pin].filter(Boolean).join(' ')
  if (city) rows.push(['Billing city', city])
  const geo = [[t.geo_state, t.geo_city].filter(Boolean).join(' · '), t.geo_pin]
    .filter(Boolean)
    .join(' ')
  if (geo) rows.push(['Exit geo', geo])
  return rows
})

/** Proxy egress: IP ra + IP billing + vị trí ước lượng (dữ liệu server bắt từ log). */
const egressRows = computed<Array<[string, string]>>(() => {
  const e = (props.task.egress || {}) as Record<string, unknown>
  const exit = (e.exit || {}) as Record<string, unknown>
  const billing = (e.billing || {}) as Record<string, unknown>
  const geo = (e.billing_geo || {}) as Record<string, unknown>
  const rows: Array<[string, string]> = []
  const exitIp = String(exit.ip || e.ip || '')
  if (exitIp) {
    const country = String(exit.country || e.billing_country || '')
    rows.push(['Exit IP', country ? `${exitIp} (${country})` : exitIp])
  }
  if (billing.ip) rows.push(['Billing IP', String(billing.ip)])
  if (geo.location) rows.push(['Billing geo', `${geo.location}${geo.approx ? ' (approx)' : ''}`])
  const isp = String(exit.isp || '')
  if (isp) rows.push(['ISP', isp.length > 42 ? `${isp.slice(0, 42)}…` : isp])
  const verdict = String(exit.verdict || '')
  if (verdict) {
    rows.push(['Proxy verdict', `${verdict}${exit.risk !== undefined ? ` · risk ${exit.risk}` : ''}`])
  }
  return rows
})

const hasDetails = computed(() => billingRows.value.length > 0 || egressRows.value.length > 0)

// Countdown hạn dùng trang instructions (chỉ có nghĩa khi còn "waiting"):
//   chip: "12:34 remaining" · quá hạn: "expired"
const now = useNow()
const expiresLeft = computed(() => {
  const exp = Number(props.task.expires_at || 0)
  return exp ? exp - now.value : 0
})
const showCountdown = computed(
  () => String(props.task.link_state || '') === 'waiting' && Number(props.task.expires_at || 0) > 0,
)
const countdownLabel = computed(() => {
  const left = expiresLeft.value
  if (left <= 0) return 'expired'
  const mm = String(Math.floor(left / 60)).padStart(2, '0')
  const ss = String(left % 60).padStart(2, '0')
  return `${mm}:${ss} remaining`
})
const detailsOpen = ref(false)

// Card "vừa có link" (trong 20 s) -> viền sáng + nhãn NEW để dễ nhận ra khi đang chạy.
const freshLinks = desk.freshLinks
const isFresh = computed(() => freshLinks.has(String(props.task.task_id)))

const SKIPPED_LABELS: Record<string, string> = {
  plus_active: 'account already Plus',
  active_sub: 'active subscription',
  banned: 'account banned',
  token_invalid: 'token invalid (401)',
  token_expired: 'token expired / revoked',
  bad_token: 'malformed token',
  expired: 'token expired',
}
const skippedLabel = computed(
  () => SKIPPED_LABELS[String(props.task.skipped_reason || '')] || 'token filter',
)

/** Tooltip: gộp note + error, bỏ dòng trùng để không hiện 2 lần cùng một câu. */
const chipTooltip = computed(() => {
  const parts = [String(props.task.link_note || ''), String(props.task.error || '')]
    .map((t) => t.trim())
    .filter(Boolean)
  const unique = [...new Set(parts.flatMap((t) => t.split('\n').map((x) => x.trim())).filter(Boolean))]
  return unique.join('\n')
})
const status = computed(() => String(props.task.status || 'pending'))

/** Giờ task xong (lúc link được trích ra) — để danh sách đọc theo thứ tự thời gian. */
const doneClock = computed(() => {
  const ts = Number(props.task.done_at || 0)
  if (!ts) return ''
  const date = new Date(ts * 1000)
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`
})
const doneTitle = computed(() => {
  const ts = Number(props.task.done_at || 0)
  return ts ? `finished ${new Date(ts * 1000).toLocaleString()}` : ''
})
const running = computed(() => status.value === 'running')

/** Viền trái theo trạng thái — nhìn nhanh được card nào đang chạy / đã xong. */
const accent = computed(() => {
  const s = status.value
  if (s === 'running') return 'border-l-ring'
  if (s === 'done') return link.value ? 'border-l-chart-2/70' : 'border-l-border'
  if (s === 'fail') return 'border-l-destructive/70'
  if (s === 'stopped') return 'border-l-muted-foreground/50'
  return 'border-l-border'
})

async function copyLink(): Promise<void> {
  if (!link.value) return
  try {
    await navigator.clipboard.writeText(link.value)
  } catch {
    /* clipboard bị chặn (không phải https) — bỏ qua, link vẫn mở được */
  }
}
</script>

<template>
  <Card
    :data-task-id="task.task_id"
    :class="[
      view === 'grid'
        ? `relative gap-1.5 border-l-2 py-2 ${accent}`
        : `relative flex-row items-center gap-3 border-l-2 py-1.5 ${accent}`,
      isFresh ? 'ring-2 ring-chart-2/70' : '',
    ]"
  >
    <span
      v-if="doneClock"
      class="mono absolute top-1.5 right-2 text-[10px] tabular-nums text-muted-foreground"
      :title="doneTitle"
    >
      {{ doneClock }}
    </span>
    <CardHeader :class="view === 'grid' ? 'px-2.5' : 'min-w-0 flex-1 px-2.5'">
      <div class="flex min-w-0 flex-wrap items-center gap-1.5 pr-12">
        <span class="mono shrink-0 text-xs text-muted-foreground">#{{ task.index ?? task.task_id }}</span>
        <span
          v-if="isFresh"
          class="shrink-0 rounded bg-chart-2 px-1 text-[9px] font-semibold tracking-wide text-background uppercase"
        >new</span>
        <span class="truncate text-xs font-medium" :title="String(task.email || '')">
          {{ task.email || '—' }}
        </span>
        <span class="shrink-0 rounded border border-border px-1 text-[10px] text-muted-foreground">
          {{ String(task.provider || 'UPI').toUpperCase() }}
        </span>
        
      </div>
      <p v-if="reason && status === 'fail'" class="mt-1 truncate text-xs text-destructive" :title="String(task.error || '')">
        {{ reason }}
      </p>
      <p
        v-else-if="status === 'skipped'"
        class="mt-1 truncate text-xs text-muted-foreground"
        :title="String(task.error || '')"
      >
        Skipped before run — {{ skippedLabel }}
      </p>
    </CardHeader>

    <CardContent :class="view === 'grid' ? 'px-2.5' : 'flex-1 px-2.5'">
      <div v-if="view === 'grid' && qrSrc && !qrBroken" class="mb-1 flex justify-center">
        <div class="rounded-lg bg-white p-2">
          <img
            :src="qrSrc"
            alt="UPI QR"
            class="size-24 object-contain"
            loading="lazy"
            @error="qrBroken = true"
          />
        </div>
      </div>
      <p v-else-if="view === 'grid' && qrSrc && qrBroken" class="mb-2 text-center text-xs text-muted-foreground">
        QR no longer available (deleted by Stripe)
      </p>
      <div v-if="link" class="mb-1 min-w-0">
        <div class="text-[9px] font-medium tracking-wide text-muted-foreground uppercase">Payment link</div>
        <a :href="link" target="_blank" rel="noreferrer" class="truncate-link text-xs" :title="link">
          {{ link }}
        </a>
      </div>
      <div
        v-else-if="running || status === 'pending'"
        class="mb-1.5 flex items-center gap-1.5 text-xs text-muted-foreground"
      >
        <QrCode class="size-3.5" />
        {{ running ? 'Working…' : 'Queued' }}
      </div>

      <div class="flex flex-wrap items-center gap-1">
        <StatusChip
          :chip="chip"
          :suffix="age || undefined"
          :title="chipTooltip || undefined"
        />
        <StatusChip v-if="plus" :chip="plus" :title="String(task.plus_note || '') || undefined" />
        <span
          v-if="showCountdown"
          class="mono inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[11px] tabular-nums"
          :class="expiresLeft > 0 ? 'border-border text-muted-foreground' : 'border-destructive/40 text-destructive'"
          :title="`expires ${new Date(Number(task.expires_at) * 1000).toLocaleString()}`"
        >
          <Hourglass class="size-3" />
          {{ countdownLabel }}
        </span>
      </div>

      <div v-if="steps.length || flowLabel !== '—'" class="mt-1.5">
        <button
          class="flex w-full items-center gap-1 text-[11px] text-muted-foreground hover:text-foreground"
          :aria-expanded="stepsOpen"
          @click="stepsOpen = !stepsOpen"
        >
          <ChevronDown :class="['size-3 shrink-0 transition-transform', stepsOpen ? 'rotate-180' : '']" />
          <span class="shrink-0">{{ flowLabel }}</span>
          <span v-if="stepSummary" class="shrink-0 opacity-70">· {{ stepSummary }}</span>
          <span
            v-if="!stepsOpen && currentStep"
            :class="['min-w-0 truncate', STEP_TONE[String(currentStep.status)] || '']"
          >
            · {{ currentStep.label }}
            <template v-if="currentStep.status === 'skip'">(skipped)</template>
          </span>
        </button>
        <div v-if="stepsOpen" class="mt-1 space-y-0.5 rounded-md border bg-muted/30 p-2">
          <div
            v-for="step in steps"
            :key="step.key"
            class="flex gap-1.5 text-[11px] leading-snug"
          >
            <span :class="['w-3 shrink-0 text-center', STEP_TONE[String(step.status)] || '']">
              {{ STEP_ICON[String(step.status)] || STEP_ICON.pending }}
            </span>
            <div class="min-w-0">
              <span :class="step.status === 'skip' ? 'text-muted-foreground line-through' : ''">
                {{ step.label }}<template v-if="step.status === 'skip'"> (Skipped)</template>
              </span>
              <div
                v-if="step.err || step.detail"
                class="break-words text-muted-foreground"
                :class="step.err ? 'text-destructive' : ''"
              >
                {{ step.err || step.detail }}
              </div>
            </div>
          </div>
        </div>
      </div>

      <div v-if="hasDetails" class="mt-1.5">
        <button
          class="flex items-center gap-1 text-[11px] text-muted-foreground hover:text-foreground"
          :aria-expanded="detailsOpen"
          @click="detailsOpen = !detailsOpen"
        >
          <ChevronDown
            :class="['size-3 transition-transform', detailsOpen ? 'rotate-180' : '']"
          />
          Billing details
          <span v-if="billingRows.length" class="opacity-70">
            · {{ task.billing_check || 'pending' }}
          </span>
        </button>
        <div v-if="detailsOpen" class="mt-1 space-y-2 rounded-md border bg-muted/30 p-2">
          <div v-if="billingRows.length">
            <p class="mb-1 text-[10px] font-medium tracking-wide text-muted-foreground uppercase">
              Billing
            </p>
            <div
              v-for="[label, value] in billingRows"
              :key="label"
              class="flex gap-2 text-[10px] leading-snug"
            >
              <span class="w-[92px] shrink-0 text-muted-foreground">{{ label }}</span>
              <span class="min-w-0 break-words">{{ value }}</span>
            </div>
          </div>
          <div v-if="egressRows.length">
            <p class="mb-1 text-[10px] font-medium tracking-wide text-muted-foreground uppercase">
              Proxy egress
            </p>
            <div
              v-for="[label, value] in egressRows"
              :key="label"
              class="flex gap-2 text-[10px] leading-snug"
            >
              <span class="w-[92px] shrink-0 text-muted-foreground">{{ label }}</span>
              <span class="mono min-w-0 break-words">{{ value }}</span>
            </div>
          </div>
        </div>
      </div>
    </CardContent>

    <CardFooter v-if="view === 'grid' && link" class="justify-end gap-0.5 px-2 pt-0 pb-0">
      <TooltipProvider :delay-duration="150">
        <Tooltip>
          <TooltipTrigger as-child>
            <Button variant="ghost" size="icon-sm" @click="copyLink">
              <Copy />
            </Button>
          </TooltipTrigger>
          <TooltipContent>Copy payment link</TooltipContent>
        </Tooltip>
        <Tooltip>
          <TooltipTrigger as-child>
            <Button variant="ghost" size="icon-sm" as="a" :href="link" target="_blank" rel="noreferrer">
              <ExternalLink />
            </Button>
          </TooltipTrigger>
          <TooltipContent>Open payment link</TooltipContent>
        </Tooltip>
      </TooltipProvider>
    </CardFooter>
  </Card>
</template>

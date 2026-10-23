<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { Copy, ExternalLink, QrCode } from '@lucide/vue'
import { Card, CardContent, CardFooter, CardHeader } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import StatusChip from '@/components/StatusChip.vue'
import type { TaskState } from '@/lib/api'
import { checkedAge, failReason, linkChip, plusChip } from '@/lib/taskView'

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

/** Tooltip: gộp note + error, bỏ dòng trùng để không hiện 2 lần cùng một câu. */
const chipTooltip = computed(() => {
  const parts = [String(props.task.link_note || ''), String(props.task.error || '')]
    .map((t) => t.trim())
    .filter(Boolean)
  const unique = [...new Set(parts.flatMap((t) => t.split('\n').map((x) => x.trim())).filter(Boolean))]
  return unique.join('\n')
})
const status = computed(() => String(props.task.status || 'pending'))
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
    :class="
      view === 'grid'
        ? `gap-2 border-l-2 py-2.5 ${accent}`
        : `flex-row items-center gap-4 border-l-2 py-2 ${accent}`
    "
  >
    <CardHeader :class="view === 'grid' ? 'px-3' : 'min-w-0 flex-1 px-3'">
      <div class="flex min-w-0 flex-wrap items-center gap-1.5">
        <span class="mono shrink-0 text-xs text-muted-foreground">#{{ task.index ?? task.task_id }}</span>
        <span class="truncate text-sm font-medium" :title="String(task.email || '')">
          {{ task.email || '—' }}
        </span>
        <span class="shrink-0 rounded border border-border px-1 text-[10px] text-muted-foreground">
          {{ String(task.provider || 'UPI').toUpperCase() }}
        </span>
        <span class="shrink-0 text-[11px] text-muted-foreground">₹0.00</span>
      </div>
      <p v-if="reason && status === 'fail'" class="mt-1 truncate text-xs text-fail/90" :title="String(task.error || '')">
        {{ reason }}
      </p>
    </CardHeader>

    <CardContent :class="view === 'grid' ? 'px-3' : 'flex-1 px-3'">
      <div v-if="view === 'grid' && qrSrc && !qrBroken" class="mb-1.5 flex justify-center">
        <div class="rounded-lg bg-white p-2">
          <img
            :src="qrSrc"
            alt="UPI QR"
            class="size-28 object-contain"
            loading="lazy"
            @error="qrBroken = true"
          />
        </div>
      </div>
      <p v-else-if="view === 'grid' && qrSrc && qrBroken" class="mb-2 text-center text-xs text-muted-foreground">
        QR no longer available (deleted by Stripe)
      </p>
      <div v-if="link" class="mb-1.5 min-w-0">
        <div class="text-[10px] font-medium tracking-wide text-muted-foreground uppercase">Payment link</div>
        <a :href="link" target="_blank" rel="noreferrer" class="truncate-link" :title="link">
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

      <div class="flex flex-wrap items-center gap-1.5">
        <StatusChip
          :chip="chip"
          :suffix="age || undefined"
          :title="chipTooltip || undefined"
        />
        <StatusChip v-if="plus" :chip="plus" :title="String(task.plus_note || '') || undefined" />
      </div>
    </CardContent>

    <CardFooter v-if="view === 'grid' && link" class="justify-end gap-0.5 px-2 pt-0">
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

<script setup lang="ts">
import { computed } from 'vue'
import { Progress } from '@/components/ui/progress'
import { Separator } from '@/components/ui/separator'
import { useDesk } from '@/composables/useDesk'
import { fmtDuration } from '@/lib/taskView'

const desk = useDesk()

const job = computed(() => desk.job.value)
const total = computed(() => Number(job.value?.total || desk.counts.value.all || 0))
const done = computed(() => Number(job.value?.done ?? 0))
const percent = computed(() => (total.value ? Math.round((done.value / total.value) * 100) : 0))
const elapsed = computed(() => fmtDuration(Number(job.value?.elapsed_ms || 0)))
const throughput = computed(() => {
  const ms = Number(job.value?.elapsed_ms || 0)
  // Job xong gần như tức thì (toàn bộ token bị bộ lọc loại) -> đừng chia cho ~0 để ra
  // "60000.0 / min": dưới 1 giây thì không có ý nghĩa thống kê.
  if (!ms || ms < 1000 || !done.value) return '—'
  return `${((done.value / ms) * 60000).toFixed(1)} / min`
})

// Job đã xong thì SSE đóng là bình thường -> gọi là "Finished", đừng báo "Disconnected".
const connection = computed(() => {
  if (desk.connected.value) return { label: 'Live (SSE)', tone: 'bg-chart-2' }
  if (job.value && job.value.status !== 'running') return { label: 'Finished', tone: 'bg-idle' }
  return { label: 'Disconnected', tone: 'bg-destructive' }
})

const cells = computed(() => [
  { label: 'Running', value: desk.counts.value.running, tone: 'text-foreground' },
  { label: 'Queued', value: desk.counts.value.queued, tone: 'text-chart-3' },
  { label: 'Success', value: desk.counts.value.success, tone: 'text-chart-2' },
  { label: 'Failed', value: desk.counts.value.fail, tone: 'text-destructive' },
  { label: 'Skipped', value: desk.counts.value.skipped, tone: 'text-muted-foreground' },
])
</script>

<template>
  <section class="rounded-xl border bg-card text-card-foreground">
    <div class="flex flex-wrap items-center justify-between gap-2 px-4 pt-4">
      <h1 class="text-base font-semibold tracking-wide uppercase">Runtime overview</h1>
      <span class="flex items-center gap-1.5 text-xs text-muted-foreground">
        <span :class="connection.tone" class="size-2 rounded-full" />
        {{ connection.label }}
      </span>
    </div>

    <div class="grid grid-cols-2 gap-px bg-border sm:grid-cols-3 xl:grid-cols-7">
      <div class="bg-card px-4 py-3">
        <div class="text-xs text-muted-foreground">Job</div>
        <div class="mono mt-1 truncate text-sm" :title="job?.job_id">{{ job?.job_id || '—' }}</div>
        <div class="mt-1 text-[11px] text-muted-foreground">{{ job?.status || 'idle' }}</div>
      </div>
      <div v-for="cell in cells" :key="cell.label" class="bg-card px-4 py-3">
        <div class="text-xs text-muted-foreground">{{ cell.label }}</div>
        <div :class="['mt-1 text-2xl font-semibold tabular-nums', cell.tone]">{{ cell.value }}</div>
      </div>
      <div class="bg-card px-4 py-3">
        <div class="text-xs text-muted-foreground">Elapsed · throughput</div>
        <div class="mt-1 text-sm tabular-nums">{{ elapsed || '—' }}</div>
        <div class="text-[11px] text-muted-foreground">{{ throughput }}</div>
      </div>
    </div>

    <div class="space-y-2 px-4 py-3">
      <div class="flex items-center justify-between text-xs text-muted-foreground">
        <span>Batch progress</span>
        <span class="tabular-nums">{{ done }}/{{ total }} · {{ percent }}%</span>
      </div>
      <Progress :model-value="percent" />
      <p v-if="job?.write_error" class="text-xs text-destructive">
        Could not write results.json: {{ job.write_error }} — free disk space (./clean_logs.sh --apply)
      </p>
      <Separator v-else />
    </div>
  </section>
</template>

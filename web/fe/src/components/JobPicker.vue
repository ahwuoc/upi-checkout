<script setup lang="ts">
/**
 * Chọn batch — thay cho `<select>` trần.
 *
 * `<select>` của trình duyệt chỉ hiện được 1 dòng chữ nên rất khó đọc: không biết batch nào
 * success bao nhiêu, fail bao nhiêu, chạy lúc nào. Popover này hiện rõ từng batch:
 *   #id (mono) · 12/100 ✓ 88 ✗ · 22:35 21/10
 */
import { computed } from 'vue'
import { Check, ChevronDown, X } from '@lucide/vue'
import { Button } from '@/components/ui/button'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { ScrollArea } from '@/components/ui/scroll-area'
import { useDesk } from '@/composables/useDesk'

const desk = useDesk()

const current = computed(() => desk.job.value)

function okCount(job: { ok?: number }): number {
  return Number(job.ok || 0)
}
function failCount(job: { fail?: number }): number {
  return Number(job.fail || 0)
}
function skippedCount(job: { skipped?: number }): number {
  return Number(job.skipped || 0)
}
function totalCount(job: { total?: number }): number {
  return Number(job.total || 0)
}

/** "22:35 21/10" — giờ chạy, để phân biệt các batch cùng ngày. */
function startedLabel(iso?: string): string {
  if (!iso) return ''
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return ''
  const hh = String(date.getHours()).padStart(2, '0')
  const mm = String(date.getMinutes()).padStart(2, '0')
  const dd = String(date.getDate()).padStart(2, '0')
  const mo = String(date.getMonth() + 1).padStart(2, '0')
  return `${hh}:${mm} ${dd}/${mo}`
}
</script>

<template>
  <Popover>
    <PopoverTrigger as-child>
      <Button variant="outline" class="h-auto w-[330px] justify-between gap-2 py-1.5">
        <span v-if="current" class="flex min-w-0 flex-1 flex-col items-start gap-0.5">
          <span class="mono text-xs text-muted-foreground">#{{ current.job_id }} · {{ current.status }}</span>
          <span class="flex items-center gap-2 whitespace-nowrap text-sm">
            <span class="flex items-center gap-1 font-medium text-chart-2">
              <Check class="size-3.5" />{{ okCount(current) }}
            </span>
            <span class="text-muted-foreground">/ {{ totalCount(current) }}</span>
            <span v-if="failCount(current)" class="flex items-center gap-1 text-destructive">
              <X class="size-3.5" />{{ failCount(current) }}
            </span>
            <span
              v-if="skippedCount(current)"
              class="text-[11px] text-muted-foreground"
              title="skipped by token filter"
            >⊘ {{ skippedCount(current) }}</span>
          </span>
        </span>
        <span v-else class="text-sm text-muted-foreground">No batch yet</span>
        <ChevronDown class="size-4 shrink-0 text-muted-foreground" />
      </Button>
    </PopoverTrigger>
    <PopoverContent class="w-[470px] p-0" align="end">
      <div class="border-b px-3 py-2">
        <p class="text-xs font-medium">Batches</p>
        <p class="text-[11px] text-muted-foreground">{{ desk.jobs.value.length }} batch gần nhất</p>
      </div>
      <ScrollArea class="h-[380px]">
        <div class="p-1">
          <button
            v-for="j in desk.jobs.value"
            :key="j.job_id"
            class="grid w-full grid-cols-[minmax(0,1fr)_auto] items-center gap-2 rounded-md px-2 py-1.5 text-left hover:bg-accent"
            :class="j.job_id === current?.job_id ? 'bg-accent/60' : ''"
            @click="desk.openJob(j.job_id)"
          >
            <!-- Left cluster: id · status · ✓ok/total · ✗fail. Every cell is nowrap so the
                 row never wraps and shifts the columns (previously "19 / 5939" broke in two). -->
            <span class="flex min-w-0 items-center gap-2.5 whitespace-nowrap">
              <span class="mono w-[92px] shrink-0 truncate text-xs" :title="j.job_id">
                {{ j.job_id }}
              </span>
              <span class="w-[52px] shrink-0 text-[11px] text-muted-foreground">{{ j.status }}</span>
              <span class="flex shrink-0 items-center gap-1 text-xs font-medium tabular-nums text-chart-2">
                <Check class="size-3 shrink-0" />{{ okCount(j) }}
              </span>
              <span class="shrink-0 text-xs tabular-nums text-muted-foreground">
                / {{ totalCount(j) }}
              </span>
              <span
                v-if="failCount(j)"
                class="flex shrink-0 items-center gap-1 text-xs tabular-nums text-destructive"
              >
                <X class="size-3 shrink-0" />{{ failCount(j) }}
              </span>
              <span
                v-if="skippedCount(j)"
                class="shrink-0 text-[11px] tabular-nums text-muted-foreground"
                title="skipped by token filter"
              >
                ⊘ {{ skippedCount(j) }}
              </span>
            </span>
            <span class="text-right text-[11px] tabular-nums whitespace-nowrap text-muted-foreground">
              {{ startedLabel(j.started_at) }}
            </span>
          </button>
        </div>
      </ScrollArea>
    </PopoverContent>
  </Popover>
</template>

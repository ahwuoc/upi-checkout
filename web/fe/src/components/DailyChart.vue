<script setup lang="ts">
/**
 * Biểu đồ "₹0.00 codes extracted per day" (giữ lại từ bản vanilla).
 *
 * Dữ liệu: `/api/stats/global.days[]` (mỗi phần tử: date, zero, paid, unknown, jobs) — cùng
 * nguồn mà panel cũ dùng, quét từ các file results trên đĩa nên sống qua restart.
 *
 * Vẽ bằng thanh CSS (không thêm thư viện chart) vì chỉ cần 1 chuỗi cột; màu lấy token shadcn
 * (`chart-2` cho ngày cao nhất, `chart-2/45` cho ngày thường).
 */
import { computed, onMounted, ref } from 'vue'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { api } from '@/lib/api'

interface DayStat {
  date: string
  zero: number
  paid: number
  unknown: number
  jobs: number
}

interface GlobalStats {
  total?: number
  total_paid?: number
  total_days?: number
  total_jobs?: number
  days?: DayStat[]
  best?: DayStat | null
}

const stats = ref<GlobalStats | null>(null)
const error = ref('')

const days = computed<DayStat[]>(() => {
  const list = [...(stats.value?.days || [])]
  list.sort((a, b) => a.date.localeCompare(b.date))
  return list.slice(-21)
})
const maxZero = computed(() => Math.max(1, ...days.value.map((d) => d.zero)))
const total = computed(() => Number(stats.value?.total || 0))
const totalJobs = computed(() => Number(stats.value?.total_jobs || 0))
const totalDays = computed(() => Number(stats.value?.total_days || days.value.length))
const bestDate = computed(() => String(stats.value?.best?.date || ''))

function barHeight(day: DayStat): string {
  const pct = Math.max(day.zero > 0 ? 6 : 2, Math.round((day.zero / maxZero.value) * 100))
  return `${pct}%`
}

function shortDate(date: string): string {
  // Chỉ "DD": 21 cột trong một hàng thì "MM-DD" bị chồng chữ; tháng/năm nằm trong tooltip.
  return date.slice(8)
}

onMounted(async () => {
  try {
    stats.value = (await api.stats()) as GlobalStats
  } catch (exc) {
    error.value = exc instanceof Error ? exc.message : String(exc)
  }
})
</script>

<template>
  <Card class="gap-3 py-4">
    <CardHeader class="px-4">
      <div class="flex flex-wrap items-end justify-between gap-2">
        <div>
          <CardTitle class="text-sm font-semibold tracking-wide uppercase">
            ₹0.00 codes extracted per day
          </CardTitle>
          <p class="mt-0.5 text-[11px] tracking-wide text-muted-foreground uppercase">All batches</p>
        </div>
        <div class="flex items-baseline gap-2">
          <span class="text-2xl font-semibold tabular-nums text-chart-2">{{ total }}</span>
          <span class="text-xs text-muted-foreground">
            ₹0.00 codes · {{ totalDays }} days · {{ totalJobs }} jobs
          </span>
        </div>
      </div>
    </CardHeader>

    <CardContent class="px-4">
      <p v-if="error" class="text-xs text-destructive">{{ error }}</p>
      <div v-else-if="!days.length" class="py-6 text-center text-xs text-muted-foreground">
        Chưa có dữ liệu ngày nào.
      </div>
      <TooltipProvider v-else :delay-duration="100">
        <div class="flex h-28 items-end gap-1.5">
          <Tooltip v-for="day in days" :key="day.date">
            <TooltipTrigger as-child>
              <div class="flex h-full min-w-0 flex-1 flex-col justify-end gap-1">
                <span class="text-center text-[10px] tabular-nums text-muted-foreground">
                  {{ day.zero || '' }}
                </span>
                <div
                  :class="[
                    'w-full rounded-sm transition-[filter] hover:brightness-110',
                    day.date === bestDate ? 'bg-chart-1' : day.zero > 0 ? 'bg-chart-2' : 'bg-muted',
                  ]"
                  :style="{ height: barHeight(day) }"
                />
              </div>
            </TooltipTrigger>
            <TooltipContent>
              <div class="space-y-0.5 text-xs">
                <div class="font-medium">{{ day.date }}</div>
                <div>₹0.00 codes: <b>{{ day.zero }}</b></div>
                <div>paid: {{ day.paid }} · unknown: {{ day.unknown }}</div>
                <div>jobs: {{ day.jobs }}</div>
              </div>
            </TooltipContent>
          </Tooltip>
        </div>
        <div class="mt-1.5 flex gap-1.5">
          <span
            v-for="day in days"
            :key="day.date"
            class="min-w-0 flex-1 overflow-hidden text-center text-[10px] tabular-nums text-muted-foreground"
            :class="day.date === bestDate ? 'font-semibold text-foreground' : ''"
          >
            {{ shortDate(day.date) }}
          </span>
        </div>
      </TooltipProvider>
    </CardContent>
  </Card>
</template>

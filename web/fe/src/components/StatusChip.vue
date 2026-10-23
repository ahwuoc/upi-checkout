<script setup lang="ts">
import { computed } from 'vue'
import { cn } from 'cn'
import type { Chip } from '@/lib/taskView'

const props = defineProps<{
  chip: Chip
  suffix?: string
  title?: string
}>()

// Màu trạng thái lấy từ token trong style.css (--ok/--warn/--fail/--idle).
const tone = computed(
  () =>
    ({
      ok: 'border-ok/35 bg-ok/12 text-ok',
      warn: 'border-warn/35 bg-warn/12 text-warn',
      fail: 'border-fail/35 bg-fail/12 text-fail',
      idle: 'border-border bg-muted/40 text-muted-foreground',
      info: 'border-ring/35 bg-ring/12 text-foreground',
    })[props.chip.tone],
)
</script>

<template>
  <span
    :class="
      cn(
        'inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs font-medium whitespace-nowrap',
        tone,
      )
    "
    :title="title"
  >
    {{ chip.label }}
    <span v-if="suffix" class="opacity-70">· {{ suffix }}</span>
  </span>
</template>

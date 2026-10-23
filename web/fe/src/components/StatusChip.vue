<script setup lang="ts">
import { computed } from 'vue'
import { cn } from 'cn'
import { Badge } from '@/components/ui/badge'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import type { Chip } from '@/lib/taskView'

const props = defineProps<{
  chip: Chip
  suffix?: string
  /** Nội dung tooltip (lý do fail, giờ dò link, log gốc…). */
  title?: string
}>()

// Màu lấy từ TOKEN CÓ SẴN của shadcn (không tự pha):
//   ok   -> chart-2 (xanh ngọc)   warn -> chart-3 (hổ phách)
//   fail -> destructive           idle -> muted-foreground
//   info -> foreground
const tone = computed(
  () =>
    ({
      ok: 'border-chart-2/40 bg-chart-2/15 text-chart-2',
      warn: 'border-chart-3/40 bg-chart-3/15 text-chart-3',
      fail: 'border-destructive/40 bg-destructive/15 text-destructive',
      idle: 'border-border bg-muted text-muted-foreground',
      info: 'border-border bg-secondary text-secondary-foreground',
    })[props.chip.tone],
)
</script>

<template>
  <TooltipProvider v-if="title" :delay-duration="150">
    <Tooltip>
      <TooltipTrigger as-child>
        <Badge variant="outline" :class="cn('gap-1 border font-medium', tone)">
          {{ chip.label }}
          <span v-if="suffix" class="opacity-70">· {{ suffix }}</span>
        </Badge>
      </TooltipTrigger>
      <TooltipContent class="max-w-96 text-pretty">{{ title }}</TooltipContent>
    </Tooltip>
  </TooltipProvider>

  <Badge v-else variant="outline" :class="cn('gap-1 border font-medium', tone)">
    {{ chip.label }}
    <span v-if="suffix" class="opacity-70">· {{ suffix }}</span>
  </Badge>
</template>

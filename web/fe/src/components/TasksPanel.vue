<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { LayoutGrid, List, Search } from '@lucide/vue'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs'
import TaskCard from '@/components/TaskCard.vue'
import { useDesk } from '@/composables/useDesk'
import type { TabKey } from '@/lib/taskView'

const desk = useDesk()

// Chỉ vẽ tối đa PAGE card một lần. Job thật 6.000 task: vẽ hết sẽ tạo 6.000 DOM card và mỗi
// lần SSE cập nhật đều phải diff hết -> treo. Bấm "Load more" để xem tiếp.
const PAGE = 120
const limit = ref(PAGE)
watch([desk.tab, desk.query], () => {
  limit.value = PAGE
})
const shown = computed(() => desk.visibleTasks.value.slice(0, limit.value))
const remaining = computed(() => Math.max(0, desk.visibleTasks.value.length - shown.value.length))

const tabs = computed<Array<{ key: TabKey; label: string; count: number }>>(() => [
  { key: 'all', label: 'All', count: desk.counts.value.all },
  { key: 'running', label: 'Running', count: desk.counts.value.running },
  { key: 'queued', label: 'Queued', count: desk.counts.value.queued },
  { key: 'success', label: 'Success', count: desk.counts.value.success },
  { key: 'fail', label: 'Failed', count: desk.counts.value.fail },
  { key: 'stopped', label: 'Stopped', count: desk.counts.value.stopped },
])
</script>

<template>
  <section class="space-y-3">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <div class="flex items-baseline gap-2">
        <h2 class="text-lg font-semibold">Tasks</h2>
        <span class="text-xs text-muted-foreground">Activity &amp; results</span>
      </div>
      <div class="flex items-center gap-2">
        <div class="relative">
          <Search class="pointer-events-none absolute top-2 left-2 size-4 text-muted-foreground" />
          <Input v-model="desk.query.value" placeholder="Search email or task ID…" class="w-64 pl-8" />
        </div>
        <div class="flex overflow-hidden rounded-md border">
          <Button
            variant="ghost"
            size="icon-sm"
            :class="desk.view.value === 'list' ? 'bg-accent text-accent-foreground' : ''"
            title="List view"
            @click="desk.view.value = 'list'"
          >
            <List />
          </Button>
          <Button
            variant="ghost"
            size="icon-sm"
            :class="desk.view.value === 'grid' ? 'bg-accent text-accent-foreground' : ''"
            title="Grid view"
            @click="desk.view.value = 'grid'"
          >
            <LayoutGrid />
          </Button>
        </div>
      </div>
    </div>

    <Tabs v-model="desk.tab.value">
      <TabsList class="flex flex-wrap">
        <TabsTrigger v-for="t in tabs" :key="t.key" :value="t.key" class="gap-1.5">
          {{ t.label }}
          <span class="rounded-full bg-muted px-1.5 text-[11px] text-muted-foreground">{{ t.count }}</span>
        </TabsTrigger>
      </TabsList>
    </Tabs>

    <div v-if="desk.error.value" class="rounded-md border border-fail/40 bg-fail/10 px-3 py-2 text-sm text-fail">
      {{ desk.error.value }}
    </div>

    <div
      v-if="desk.visibleTasks.value.length"
      :class="desk.view.value === 'grid' ? 'grid gap-3 md:grid-cols-2 xl:grid-cols-3' : 'flex flex-col gap-2'"
    >
      <TaskCard
        v-for="task in shown"
        :key="task.task_id"
        :task="task"
        :view="desk.view.value"
        :job-running="desk.job.value?.status === 'running'"
      />
    </div>
    <div v-if="remaining" class="flex justify-center">
      <Button variant="outline" size="sm" @click="limit += PAGE">
        Load {{ Math.min(PAGE, remaining) }} more · còn {{ remaining }} task
      </Button>
    </div>
    <p v-else class="rounded-lg border border-dashed border-border px-4 py-10 text-center text-sm text-muted-foreground">
      {{ desk.loading.value ? 'Loading…' : 'No tasks in this tab yet.' }}
    </p>
  </section>
</template>

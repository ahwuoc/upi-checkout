<script setup lang="ts">
import { onMounted } from 'vue'
import StatsBar from '@/components/StatsBar.vue'
import TasksPanel from '@/components/TasksPanel.vue'
import { useDesk } from '@/composables/useDesk'

const desk = useDesk()

onMounted(() => {
  void desk.bootstrap()
})
</script>

<template>
  <div class="min-h-dvh bg-background">
    <header class="mx-auto flex max-w-[1600px] flex-wrap items-center justify-between gap-3 px-5 py-4">
      <div>
        <h1 class="text-lg font-semibold">UPI Checkout · desk</h1>
        <p class="text-xs text-muted-foreground">Your tasks, payment links and QR codes in one place.</p>
      </div>
      <select
        class="h-9 max-w-72 rounded-md border border-input bg-transparent px-2 text-sm"
        :value="desk.job.value?.job_id || ''"
        @change="desk.openJob(($event.target as HTMLSelectElement).value)"
      >
        <option v-for="j in desk.jobs.value" :key="j.job_id" :value="j.job_id">
          {{ j.job_id }} · {{ j.status }} · {{ j.done }}/{{ j.total }}
        </option>
      </select>
    </header>

    <main class="mx-auto max-w-[1600px] space-y-4 px-5 pb-10">
      <StatsBar />
      <TasksPanel />
    </main>
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from 'vue' 
import ControlPanel from '@/components/ControlPanel.vue'
import DailyChart from '@/components/DailyChart.vue'
import JobPicker from '@/components/JobPicker.vue'
import { Toaster } from '@/components/ui/sonner'
import { toast } from 'vue-sonner'
import StatsBar from '@/components/StatsBar.vue'
import TasksPanel from '@/components/TasksPanel.vue'
import { Moon, Sun } from '@lucide/vue'
import { Button } from '@/components/ui/button'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { useDesk } from '@/composables/useDesk'

const desk = useDesk()

// Theme theo đúng cơ chế của shadcn: class `dark` trên <html>, lưu vào localStorage.
const THEME_KEY = 'upi-theme'
const dark = ref(true)

function applyTheme(next: boolean): void {
  dark.value = next
  document.documentElement.classList.toggle('dark', next)
  try {
    localStorage.setItem(THEME_KEY, next ? 'dark' : 'light')
  } catch {
    /* chế độ riêng tư: không lưu được thì thôi */
  }
}

// Link mới -> toast (bấm "Show" để nhảy tới card) và thông báo hệ thống nếu tab đang ẩn.
watch(
  () => desk.lastLink.value,
  (link) => {
    if (!link) return
    const clock = new Date(link.at * 1000).toLocaleTimeString()
    toast.success('New link extracted', {
      description: `${link.email || link.taskId} · ${clock}`,
      action: { label: 'Show', onClick: () => void desk.focusTask(link.taskId) },
      duration: 8000,
    })
    if (typeof Notification !== 'undefined' && Notification.permission === 'granted' && document.hidden) {
      try {
        new Notification('New ₹0 link extracted', {
          body: `${link.email || link.taskId} · ${clock}`,
          tag: link.taskId,
        })
      } catch {
        /* trình duyệt chặn notification -> bỏ qua, toast vẫn hiện */
      }
    }
  },
)

onMounted(() => {
  let saved: string | null = null
  try {
    saved = localStorage.getItem(THEME_KEY)
  } catch {
    saved = null
  }
  const prefersDark = window.matchMedia?.('(prefers-color-scheme: dark)').matches ?? true
  applyTheme(saved ? saved === 'dark' : prefersDark)
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
      <div class="flex items-center gap-2">
        <TooltipProvider :delay-duration="150">
          <Tooltip>
            <TooltipTrigger as-child>
              <Button variant="outline" size="icon-sm" @click="applyTheme(!dark)">
                <Sun v-if="dark" />
                <Moon v-else />
              </Button>
            </TooltipTrigger>
            <TooltipContent>Switch theme</TooltipContent>
          </Tooltip>
        </TooltipProvider>
      </div>
      <JobPicker />
    </header>

    <main class="mx-auto grid max-w-[1900px] gap-4 px-5 pb-10 lg:grid-cols-[340px_minmax(0,1fr)]">
      <div class="lg:sticky lg:top-4 lg:self-start">
        <ControlPanel />
      </div>
      <div class="min-w-0 space-y-4">
        <div class="grid gap-4">
          <StatsBar />
          <DailyChart />
        </div>
        <TasksPanel />
      </div>
    </main>

  </div>
  <!-- Đặt NGOÀI khối app: Toaster dùng position:fixed, nếu nằm trong phần tử có transform/overflow
       thì toast bị cắt ở đáy màn hình (đã gặp: toast dài bị kẹt dưới, chỉ thấy 1 dòng). -->
  <Toaster
    position="top-right"
    :offset="16"
    :mobile-offset="10"
    :gap="8"
    :visible-toasts="2"
    :duration="3000"
    :close-button="true"
    rich-colors
    :expand="true"
  />
</template>

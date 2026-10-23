<script setup lang="ts">
/**
 * Sidebar điều khiển job — bản port của panel bên trái giao diện vanilla.
 *
 * Gồm: dán access token, chọn luồng/quốc gia/promo, workers, số lần chạy, proxy (tuỳ chọn),
 * các cờ thử nghiệm, nút Run/Stop. Toàn bộ giá trị lưu vào localStorage để mở lại không mất.
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { Loader2, Play, Square } from '@lucide/vue'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from '@/components/ui/collapsible'
import { Textarea } from '@/components/ui/textarea'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { api, ApiError, type RunPayload } from '@/lib/api'
import { useDesk } from '@/composables/useDesk'

const desk = useDesk()

const FORM_KEY = 'upi-run-form'
const MAX_WORKERS = 300

const form = reactive({
  tokens: '',
  mode: 'cs',
  country: 'IN',
  promo: 'off',
  workers: 300,
  retries: 1,
  useProxy: true,
  proxies: '',
  mix: false,
  filterProxies: true,
  stripeTelemetry: false,
  dumpTraffic: false,
})

const starting = ref(false)
const stopping = ref(false)
const error = ref('')
const advanced = ref(false)

const tokenCount = computed(
  () => form.tokens.split('\n').map((t) => t.trim()).filter(Boolean).length,
)
const jobRunning = computed(() => desk.job.value?.status === 'running')

onMounted(() => {
  try {
    const raw = localStorage.getItem(FORM_KEY)
    if (!raw) return
    const saved = JSON.parse(raw) as Partial<typeof form>
    Object.assign(form, saved)
    // Không khôi phục token đã chạy: hạn chế dán lại nhầm cả batch cũ.
    form.tokens = ''
  } catch {
    /* localStorage hỏng/không có -> dùng mặc định */
  }
})

function saveForm(): void {
  try {
    const { tokens: _tokens, ...rest } = form
    localStorage.setItem(FORM_KEY, JSON.stringify(rest))
  } catch {
    /* chế độ riêng tư */
  }
}

async function startJob(): Promise<void> {
  error.value = ''
  if (!tokenCount.value) {
    error.value = 'Dán ít nhất 1 access token (mỗi dòng 1 token).'
    return
  }
  starting.value = true
  try {
    const payload: RunPayload = {
      tokens: form.tokens,
      mode: form.mode,
      country: form.country.toUpperCase() || 'IN',
      promo: form.promo,
      workers: Math.max(1, Math.min(MAX_WORKERS, Number(form.workers) || 1)),
      retries: Math.max(1, Math.min(5, Number(form.retries) || 1)),
      proxies: form.useProxy ? form.proxies || null : null,
      mix: form.mix,
      filter_proxies: form.filterProxies,
      stripe_telemetry: form.stripeTelemetry,
      dump_traffic: form.dumpTraffic,
    }
    const job = await api.run(payload)
    await desk.refreshJobs()
    await desk.openJob(job.job_id)
  } catch (exc) {
    error.value =
      exc instanceof ApiError
        ? exc.message
        : exc instanceof Error
          ? exc.message
          : String(exc)
  } finally {
    starting.value = false
  }
}

async function stopJob(): Promise<void> {
  const jobId = desk.job.value?.job_id
  if (!jobId) return
  stopping.value = true
  try {
    await api.stop(jobId)
  } catch (exc) {
    error.value = exc instanceof Error ? exc.message : String(exc)
  } finally {
    stopping.value = false
  }
}
</script>

<template>
  <aside class="space-y-4 rounded-xl border bg-card p-4 text-card-foreground">
    <div class="space-y-1.5">
      <div class="flex items-center justify-between">
        <Label for="tokens">Access tokens</Label>
        <span class="text-xs text-muted-foreground">{{ tokenCount }} token</span>
      </div>
      <Textarea
        id="tokens"
        v-model="form.tokens"
        rows="6"
        spellcheck="false"
        class="mono max-h-52 min-h-24 text-xs"
        placeholder="Dán token, mỗi dòng 1 cái (JWT)…"
        @input="saveForm"
      />
    </div>

    <div class="grid grid-cols-2 gap-3">
      <div class="space-y-1.5">
        <Label>Flow</Label>
        <Select v-model="form.mode" @update:model-value="saveForm">
          <SelectTrigger class="w-full"><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="cs">CS / Stripe</SelectItem>
            <SelectItem value="oaics">OAICS</SelectItem>
            <SelectItem value="auto">Auto detect</SelectItem>
          </SelectContent>
        </Select>
      </div>
      <div class="space-y-1.5">
        <Label>Country</Label>
        <Input v-model="form.country" maxlength="2" class="uppercase" @change="saveForm" />
      </div>
      <div class="space-y-1.5">
        <Label>Workers</Label>
        <Input v-model.number="form.workers" type="number" min="1" :max="MAX_WORKERS" @change="saveForm" />
      </div>
      <div class="space-y-1.5">
        <Label>Runs / account</Label>
        <Input v-model.number="form.retries" type="number" min="1" max="5" @change="saveForm" />
      </div>
    </div>

    <Collapsible v-model:open="advanced" class="rounded-lg border">
      <CollapsibleTrigger as-child>
        <Button variant="ghost" class="w-full justify-between px-3">
          <span>Advanced settings</span>
          <span class="text-xs text-muted-foreground">{{ advanced ? '▾' : '▸' }}</span>
        </Button>
      </CollapsibleTrigger>
      <CollapsibleContent class="space-y-3 border-t px-3 py-3">
        <div class="space-y-1.5">
          <Label>Promotion</Label>
          <Select v-model="form.promo" @update:model-value="saveForm">
            <SelectTrigger class="w-full"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="off">Off — full price</SelectItem>
              <SelectItem value="campaign">plus-1-month-free</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <div class="flex items-center justify-between gap-2">
          <Label for="use-proxy">Use my own proxy list</Label>
          <Switch id="use-proxy" v-model="form.useProxy" @update:model-value="saveForm" />
        </div>
        <Textarea
          v-if="form.useProxy"
          v-model="form.proxies"
          rows="3"
          spellcheck="false"
          class="mono text-xs"
          placeholder="host:port:user:pass — mỗi dòng 1 proxy (trống = proxy.txt)"
          @input="saveForm"
        />

        <div class="space-y-2 border-t pt-3">
          <div class="flex items-center justify-between gap-2">
            <div>
              <Label for="mix">Mix pool order</Label>
              <p class="text-[11px] text-muted-foreground">Shuffles the pool each run</p>
            </div>
            <Switch id="mix" v-model="form.mix" @update:model-value="saveForm" />
          </div>
          <div class="flex items-center justify-between gap-2">
            <div>
              <Label for="filter">Proxy quality filter</Label>
              <p class="text-[11px] text-muted-foreground">Drops dead / wrong-country / CF-blocked</p>
            </div>
            <Switch id="filter" v-model="form.filterProxies" @update:model-value="saveForm" />
          </div>
          <div class="flex items-center justify-between gap-2">
            <div>
              <Label for="telemetry">Stripe session (m.stripe.com)</Label>
              <p class="text-[11px] text-muted-foreground">Real muid/guid/sid instead of made-up</p>
            </div>
            <Switch id="telemetry" v-model="form.stripeTelemetry" @update:model-value="saveForm" />
          </div>
          <div class="flex items-center justify-between gap-2">
            <div>
              <Label for="dump">Dump all API traffic</Label>
              <p class="text-[11px] text-muted-foreground">HAR entries in dumps/&lt;task&gt;/</p>
            </div>
            <Switch id="dump" v-model="form.dumpTraffic" @update:model-value="saveForm" />
          </div>
        </div>
      </CollapsibleContent>
    </Collapsible>

    <p v-if="error" class="rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-xs text-destructive">
      {{ error }}
    </p>

    <div class="space-y-2">
      <p class="text-xs text-muted-foreground">
        {{ tokenCount }} task{{ tokenCount === 1 ? '' : 's' }} × {{ form.retries }} run{{ Number(form.retries) === 1 ? '' : 's' }}
        · {{ Math.max(1, Math.min(MAX_WORKERS, Number(form.workers) || 1)) }} workers
      </p>
      <Button class="w-full" :disabled="starting || !tokenCount" @click="startJob">
        <Loader2 v-if="starting" class="animate-spin" />
        <Play v-else />
        {{ starting ? 'Starting…' : `Run ${tokenCount || ''} task${tokenCount === 1 ? '' : 's'}` }}
      </Button>
      <TooltipProvider :delay-duration="150">
        <Tooltip>
          <TooltipTrigger as-child>
            <Button variant="destructive" class="w-full" :disabled="!jobRunning || stopping" @click="stopJob">
              <Square />
              {{ stopping ? 'Stopping…' : 'Stop job' }}
            </Button>
          </TooltipTrigger>
          <TooltipContent>Dừng job đang chạy (task đang xử lý sẽ kết thúc)</TooltipContent>
        </Tooltip>
      </TooltipProvider>
    </div>
  </aside>
</template>

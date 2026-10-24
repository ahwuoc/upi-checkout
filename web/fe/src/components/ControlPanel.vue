<script setup lang="ts">
/**
 * Sidebar điều khiển job — bản port của panel bên trái giao diện vanilla.
 *
 * Gồm: dán access token, chọn luồng/quốc gia/promo, workers, số lần chạy, proxy (tuỳ chọn),
 * các cờ thử nghiệm, nút Run/Stop. Toàn bộ giá trị lưu vào localStorage để mở lại không mất.
 */
import { computed, onMounted, reactive, ref } from 'vue'
import { Bell, BellOff, Check, Copy, Filter, KeyRound, Loader2, Play, Square } from '@lucide/vue'
import { Badge } from '@/components/ui/badge'
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
import { api, ApiError, type PrefilterStatus, type RunPayload } from '@/lib/api'
import { useNotifySound } from '@/composables/useNotifySound'
import { setStopping, useDesk } from '@/composables/useDesk'

const desk = useDesk()
const sound = useNotifySound()

const FORM_KEY = 'upi-run-form'
const TOKENS_KEY = 'upi-tokens'
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
  prefilter: true,
  filterLimit: 200,
  filterWorkers: 16,
  rememberTokens: false,
})

const starting = ref(false)
const checking = ref(false)
const prefilter = ref<PrefilterStatus | null>(null)
const prefilterId = ref('')
const prefilterError = ref('')
const startedAt = ref(0)
const stopRequested = ref(false)
let pollTimer: ReturnType<typeof setInterval> | null = null
const copied = ref(false)
let copyTimer: ReturnType<typeof setTimeout> | null = null
const stopping = ref(false)
const error = ref('')
const advanced = ref(false)

const tokenCount = computed(
  () => form.tokens.split('\n').map((t) => t.trim()).filter(Boolean).length,
)
const jobRunning = computed(() => desk.job.value?.status === 'running')

const VERDICT_LABELS: Record<string, string> = {
  eligible: 'Eligible',
  plus_active: 'Plus',
  active_sub: 'Subscribed',
  banned: 'Banned',
  token_invalid: 'Invalid',
  token_expired: 'Expired',
  rate_limited: 'Rate limited',
  network_error: 'Network',
  bad_token: 'Malformed',
  expired: 'Expired',
  not_eligible: 'No promo',
}
function verdictLabel(verdict: string): string {
  return VERDICT_LABELS[verdict] || verdict
}

/** Thay ô token bằng đúng những acc vừa được giữ lại. */
function applyKept(): void {
  const keep = new Set((prefilter.value?.results || []).filter((r) => !r.dropped).map((r) => r.index))
  const tokens = tokenList(form.tokens)
  const sample = filterSample.value
  form.tokens = tokens.filter((_, index) => index >= sample || keep.has(index)).join('\n')
  saveForm()
}

onMounted(() => {
  try {
    const raw = localStorage.getItem(FORM_KEY)
    if (!raw) return
    const saved = JSON.parse(raw) as Partial<typeof form>
    Object.assign(form, saved)
    // Token chỉ khôi phục khi người dùng chủ động bật "Remember tokens"; mặc định không lưu
    // để tránh chạy nhầm batch cũ và tránh token nằm lại trong trình duyệt.
    const savedTokens = localStorage.getItem(TOKENS_KEY)
    if (form.rememberTokens && savedTokens) form.tokens = savedTokens
    else form.tokens = ''
  } catch {
    /* localStorage hỏng/không có -> dùng mặc định */
  }
})

/** Copy đúng những token đang có trong ô (đã trim, bỏ dòng trống) để dán sang nơi khác. */
async function copyTokens(): Promise<void> {
  const text = form.tokens
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .join('\n')
  if (!text) return
  try {
    await navigator.clipboard.writeText(text)
  } catch {
    // Không có quyền clipboard (http không phải localhost): chọn text để user tự Ctrl+C.
    const area = document.getElementById('tokens') as HTMLTextAreaElement | null
    area?.select()
    return
  }
  copied.value = true
  if (copyTimer) clearTimeout(copyTimer)
  copyTimer = setTimeout(() => {
    copied.value = false
  }, 1500)
}

function saveForm(): void {
  try {
    const { tokens, ...rest } = form
    localStorage.setItem(FORM_KEY, JSON.stringify(rest))
    if (form.rememberTokens) localStorage.setItem(TOKENS_KEY, tokens)
    else localStorage.removeItem(TOKENS_KEY)
  } catch {
    /* chế độ riêng tư */
  }
}

/** Bật/tắt nhớ token: bật thì lưu ngay, tắt thì xoá khỏi máy. */
function setRememberTokens(next: boolean): void {
  form.rememberTokens = next
  saveForm()
}

/** Tách token thành mảng đã trim (bỏ dòng trống) — dùng chung cho đếm và lọc. */
function tokenList(raw: string): string[] {
  return raw
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
}

/** Số token thực sự đem đi check: tối đa `form.filterLimit` (0 = check hết). */
/** Tốc độ check (token/giây) và thời gian còn lại — để biết có nên chờ hay bỏ qua. */
const speed = computed(() => {
  const snap = prefilter.value
  if (!snap || !startedAt.value || !snap.checked) return 0
  const secs = (Date.now() - startedAt.value) / 1000
  return secs > 0.5 ? snap.checked / secs : 0
})
const eta = computed(() => {
  const snap = prefilter.value
  if (!snap || !speed.value) return '—'
  const left = Math.max(0, snap.total - snap.checked) / speed.value
  if (left < 60) return `${Math.round(left)}s`
  return `${Math.round(left / 60)}m`
})

const filterSample = computed(() => {
  const all = tokenCount.value
  const limit = Number(form.filterLimit) || 0
  return limit > 0 ? Math.min(all, limit) : all
})

/**
 * Gọi prefilter (1 GET/token, chỉ `filterSample` token đầu) rồi trả về token ĐỦ điều kiện.
 * Token nằm ngoài mẫu KHÔNG bị loại — batch lớn vẫn chạy được ngay, không phải chờ check hết.
 */
async function runPrefilter(raw: string): Promise<string[]> {
  prefilterError.value = ''
  checking.value = true
  prefilter.value = null
  startedAt.value = Date.now()
  const tokens = tokenList(raw)
  const sample = filterSample.value
  const head = tokens.slice(0, sample)
  try {
    const started = await api.prefilter({
      tokens: head.join('\n'),
      workers: Math.max(2, Math.min(32, Number(form.filterWorkers) || 16)),
      proxies: form.useProxy ? form.proxies || null : null,
    })
    prefilterId.value = started.id
    for (;;) {
      const snap = await api.prefilterStatus(started.id)
      prefilter.value = snap
      if (snap.state !== 'running') break
      if (stopRequested.value) {
        await api.prefilterStop(started.id).catch(() => undefined)
        stopRequested.value = false
      }
      await new Promise((resolve) => setTimeout(resolve, 600))
    }
    const keepHead = new Set(
      (prefilter.value?.results || []).filter((r) => !r.dropped).map((r) => r.index),
    )
    return tokens.filter((_, index) => index >= sample || keepHead.has(index))
  } catch (exc) {
    prefilterError.value = exc instanceof ApiError ? exc.message : String(exc)
    return tokens
  } finally {
    checking.value = false
    if (pollTimer) {
      clearInterval(pollTimer)
      pollTimer = null
    }
  }
}

async function startJob(): Promise<void> {
  error.value = ''
  if (!tokenCount.value) {
    error.value = 'Paste at least one access token (one per line).'
    return
  }
  starting.value = true
  try {
    sound.unlockAudio()
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
      // Bộ lọc chạy TRONG worker (1 GET/token ngay trước flow của chính acc đó) nên job
      // bắt đầu ngay, không phải chờ quét cả batch như trước.
      prefilter: form.prefilter,
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
  error.value = ''
  stopping.value = true
  setStopping(true)
  try {
    await api.stop(jobId)
  } catch (exc) {
    // 400 "job is not running" = job đã dừng/xong từ trước, không phải lỗi của người dùng.
    const message = exc instanceof Error ? exc.message : String(exc)
    if (!/not running|does not exist/i.test(message)) error.value = message
    setStopping(false)
  } finally {
    stopping.value = false
  }
}
</script>

<template>
  <aside class="space-y-4 rounded-xl border bg-card p-4 text-card-foreground">
    <div class="space-y-1.5">
      <div class="flex items-center justify-between gap-2">
        <Label for="tokens" class="flex items-center gap-1.5">
          <KeyRound class="size-3.5 text-muted-foreground" />
          Access tokens
        </Label>
        <div class="flex items-center gap-1">
          <span class="text-xs text-muted-foreground">{{ tokenCount }} token{{ tokenCount === 1 ? '' : 's' }}</span>
          <TooltipProvider :delay-duration="150">
            <Tooltip>
              <TooltipTrigger as-child>
                <Button
                  id="copy-tokens"
                  variant="ghost"
                  size="icon-xs"
                  :disabled="!tokenCount"
                  :aria-label="copied ? 'Copied' : 'Copy tokens'"
                  @click="copyTokens"
                >
                  <Check v-if="copied" class="text-chart-2" />
                  <Copy v-else />
                </Button>
              </TooltipTrigger>
              <TooltipContent>{{ copied ? 'Copied' : 'Copy all tokens' }}</TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
      </div>
      <Textarea
        id="tokens"
        v-model="form.tokens"
        rows="6"
        spellcheck="false"
        class="mono max-h-52 min-h-24 text-xs"
        placeholder="Paste tokens, one per line (JWT)…"
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
          id="proxies"
          v-model="form.proxies"
          rows="3"
          spellcheck="false"
          class="mono text-xs"
          placeholder="host:port:user:pass — one proxy per line (empty = proxy.txt)"
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
              <Label for="remember-tokens" class="flex items-center gap-1.5">
                <KeyRound class="size-3.5 text-muted-foreground" />
                Remember tokens
              </Label>
              <p class="text-[11px] text-muted-foreground">
                Keep the pasted tokens in this browser (localStorage) — off by default
              </p>
            </div>
            <Switch
              id="remember-tokens"
              :model-value="form.rememberTokens"
              @update:model-value="setRememberTokens($event === true)"
            />
          </div>
          <div class="flex items-center justify-between gap-2">
            <div>
              <Label for="sound" class="flex items-center gap-1.5">
                <Bell v-if="sound.enabled.value" class="size-3.5 text-muted-foreground" />
                <BellOff v-else class="size-3.5 text-muted-foreground" />
                Sound on new link
              </Label>
              <p class="text-[11px] text-muted-foreground">
                Messenger sound the moment a ₹0 link/QR is extracted
              </p>
            </div>
            <div class="flex items-center gap-1">
              <Button
                id="sound-test"
                variant="ghost"
                size="icon-xs"
                title="Play the sound"
                :disabled="!sound.enabled.value"
                @click="sound.unlockAudio(); sound.playSuccessSound()"
              >
                <Play />
              </Button>
              <Switch
                id="sound"
                :model-value="sound.enabled.value"
                @update:model-value="sound.setSoundEnabled($event === true)"
              />
            </div>
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

    <div class="space-y-2 rounded-lg border p-3">
      <div class="flex items-center justify-between gap-2">
        <div>
          <Label for="prefilter" class="flex items-center gap-1.5">
            <Filter class="size-3.5 text-muted-foreground" />
            Filter tokens before run
          </Label>
          <p class="text-[11px] text-muted-foreground">
            Each worker checks its own token first (1 GET) — drops Plus, banned, dead tokens
            before spending a full flow. No upfront wait.
          </p>
        </div>
        <Switch id="prefilter" v-model="form.prefilter" @update:model-value="saveForm" />
      </div>

      <div class="grid grid-cols-2 gap-2">
        <div class="space-y-1">
          <Label for="filter-limit" class="text-[11px] text-muted-foreground">Sample (0 = all)</Label>
          <Input
            id="filter-limit"
            v-model.number="form.filterLimit"
            type="number"
            min="0"
            :max="tokenCount || 0"
            class="h-8"
            @change="saveForm"
          />
        </div>
        <div class="space-y-1">
          <Label for="filter-workers" class="text-[11px] text-muted-foreground">Threads</Label>
          <Input
            id="filter-workers"
            v-model.number="form.filterWorkers"
            type="number"
            min="2"
            max="32"
            class="h-8"
            @change="saveForm"
          />
        </div>
      </div>

      <div class="flex items-center gap-2">
        <Button
          id="check-tokens"
          variant="secondary"
          size="sm"
          class="flex-1"
          :disabled="checking || !tokenCount"
          @click="runPrefilter(form.tokens)"
        >
          <Loader2 v-if="checking" class="animate-spin" />
          <Filter v-else />
          {{ checking ? 'Checking…' : `Preview check (${filterSample || 0})` }}
        </Button>
        <Button v-if="checking" size="sm" variant="outline" @click="stopRequested = true">
          Stop
        </Button>
        <Button
          v-else-if="prefilter && prefilter.dropped > 0"
          size="sm"
          variant="outline"
          @click="applyKept"
        >
          Use {{ prefilter.kept }}
        </Button>
      </div>

      <div v-if="prefilter" class="space-y-1">
        <div class="flex items-center justify-between text-[11px] text-muted-foreground">
          <span>
            {{ prefilter.checked }}/{{ prefilter.total }} · kept {{ prefilter.kept }}
            <template v-if="prefilter.state === 'running' && speed > 0">
              · {{ speed.toFixed(1) }}/s · ETA {{ eta }}
            </template>
          </span>
          <span v-if="prefilter.dropped" class="text-destructive">dropped {{ prefilter.dropped }}</span>
        </div>
        <div class="h-1.5 overflow-hidden rounded-full bg-muted">
          <div
            class="h-full bg-chart-2 transition-[width]"
            :style="{ width: `${prefilter.total ? (prefilter.checked / prefilter.total) * 100 : 0}%` }"
          />
        </div>
        <div class="flex flex-wrap gap-1">
          <Badge
            v-for="(count, verdict) in prefilter.counts"
            :key="verdict"
            variant="outline"
            class="text-[10px]"
          >
            {{ verdictLabel(String(verdict)) }}: {{ count }}
          </Badge>
        </div>
      </div>

      <p v-if="prefilterError" class="text-[11px] text-destructive">{{ prefilterError }}</p>
    </div>

    <div
      v-if="desk.stopping.value || stopping"
      class="rounded-md border border-chart-3/40 bg-chart-3/10 px-3 py-2 text-xs"
    >
      <p class="font-medium text-chart-3">Stopping…</p>
      <p class="text-muted-foreground">
        {{ desk.job.value?.running || 0 }} task(s) still finishing; queued tasks will be skipped.
        The job shows “stopped” when the last one is done.
      </p>
    </div>

    <p v-if="error" class="rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-xs text-destructive">
      {{ error }}
    </p>

    <div class="space-y-2">
      <p class="text-xs text-muted-foreground">
        {{ tokenCount }} task{{ tokenCount === 1 ? '' : 's' }} × {{ form.retries }} run{{ Number(form.retries) === 1 ? '' : 's' }}
        · {{ Math.max(1, Math.min(MAX_WORKERS, Number(form.workers) || 1)) }} workers
      </p>
      <Button
        id="submit"
        class="w-full"
        :disabled="starting || !tokenCount || desk.stopping.value"
        @click="startJob"
      >
        <Loader2 v-if="starting" class="animate-spin" />
        <Play v-else />
        {{ starting ? 'Starting…' : `Run ${tokenCount || ''} task${tokenCount === 1 ? '' : 's'}` }}
      </Button>
      <TooltipProvider :delay-duration="150">
        <Tooltip>
          <TooltipTrigger as-child>
            <Button
              id="stop"
              variant="destructive"
              class="w-full"
              :disabled="!jobRunning || stopping || desk.stopping.value"
              @click="stopJob"
            >
              <Square />
              {{ stopping || desk.stopping.value ? 'Stopping…' : 'Stop job' }}
            </Button>
          </TooltipTrigger>
          <TooltipContent>Stop the running job (in-flight task will finish)</TooltipContent>
        </Tooltip>
      </TooltipProvider>
    </div>
  </aside>
</template>

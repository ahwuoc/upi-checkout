/**
 * Âm thanh thông báo khi có link thành công (bản vanilla trước đây có `notify.mp3`).
 *
 * Vì sao không dùng file mp3: asset đó đã bị xoá cùng giao diện cũ, mà một tiếng "ding" thì
 * Web Audio tạo được ngay (0 KB, không phụ thuộc mạng, không lo autoplay của file).
 *
 * Quy tắc autoplay: AudioContext chỉ phát được sau một thao tác của người dùng, nên ta tạo và
 * `resume()` context ngay trong cú click **Run** (xem `unlock()`), sau đó phát tự do.
 */
import { ref } from 'vue'

const STORAGE_KEY = 'upi-notify-sound'
const enabled = ref(true)
let ctx: AudioContext | null = null

function load(): void {
  try {
    const saved = localStorage.getItem(STORAGE_KEY)
    if (saved === 'off') enabled.value = false
  } catch {
    /* chế độ riêng tư */
  }
}
load()

function audioContext(): AudioContext | null {
  if (typeof window === 'undefined') return null
  if (!ctx) {
    const Ctor = window.AudioContext || (window as unknown as { webkitAudioContext?: typeof AudioContext }).webkitAudioContext
    if (!Ctor) return null
    ctx = new Ctor()
  }
  return ctx
}

/** Gọi trong 1 sự kiện click (nút Run) để mở khoá autoplay. */
export function unlockAudio(): void {
  const context = audioContext()
  if (context && context.state === 'suspended') void context.resume()
}

/** Hai nốt đi lên (C6 → E6) — đủ rõ để nghe mà không chói khi chạy 300 worker. */
export function playSuccessSound(volume = 0.16): void {
  if (!enabled.value) return
  const context = audioContext()
  if (!context) return
  if (context.state === 'suspended') void context.resume()
  const now = context.currentTime
  for (const [index, freq] of [1046.5, 1318.5].entries()) {
    const osc = context.createOscillator()
    const gain = context.createGain()
    osc.type = 'sine'
    osc.frequency.value = freq
    const start = now + index * 0.12
    gain.gain.setValueAtTime(0, start)
    gain.gain.linearRampToValueAtTime(volume, start + 0.02)
    gain.gain.exponentialRampToValueAtTime(0.0001, start + 0.34)
    osc.connect(gain).connect(context.destination)
    osc.start(start)
    osc.stop(start + 0.36)
  }
}

/** Bật/tắt + nhớ lựa chọn. */
export function setSoundEnabled(next: boolean): void {
  enabled.value = next
  try {
    localStorage.setItem(STORAGE_KEY, next ? 'on' : 'off')
  } catch {
    /* chế độ riêng tư */
  }
  if (next) {
    unlockAudio()
    playSuccessSound(0.1)   // nghe thử ngay khi bật
  }
}

export function useNotifySound() {
  return { enabled, setSoundEnabled, playSuccessSound, unlockAudio }
}

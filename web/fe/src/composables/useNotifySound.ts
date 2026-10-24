/**
 * Âm thanh thông báo khi có link thành công.
 *
 * Dùng file thật `public/sounds/success.mp3` (tiếng thông báo Messenger) — cùng cơ chế với
 * `notify.mp3` của bản vanilla trước đây, nhưng asset nằm trong bản build nên `/` phục vụ được.
 *
 * Vì sao vẫn có Web Audio dự phòng: nếu file lỗi/không tải được (mất mạng, cache hỏng) thì
 * phát 2 nốt C6→E6 để vẫn có tín hiệu, thay vì im lặng.
 *
 * Autoplay: Audio chỉ phát được sau một thao tác của người dùng, nên `unlockAudio()` được gọi
 * ngay trong cú click **Run** (và khi bật công tắc) để "mở khoá" trước.
 */
import { ref } from 'vue'

const STORAGE_KEY = 'upi-notify-sound'
const SOUND_URL = '/sounds/success.mp3'

const enabled = ref(true)
let audio: HTMLAudioElement | null = null
let ctx: AudioContext | null = null
let unlocked = false

function load(): void {
  try {
    const saved = localStorage.getItem(STORAGE_KEY)
    if (saved === 'off') enabled.value = false
  } catch {
    /* chế độ riêng tư */
  }
}
load()

function audioElement(): HTMLAudioElement | null {
  if (typeof window === 'undefined') return null
  if (!audio) {
    audio = new Audio(SOUND_URL)
    audio.preload = 'auto'
    audio.volume = 0.7
  }
  return audio
}

function audioContext(): AudioContext | null {
  if (typeof window === 'undefined') return null
  if (!ctx) {
    const Ctor =
      window.AudioContext ||
      (window as unknown as { webkitAudioContext?: typeof AudioContext }).webkitAudioContext
    if (!Ctor) return null
    ctx = new Ctor()
  }
  return ctx
}

/** Gọi trong 1 sự kiện click (nút Run / bật công tắc) để mở khoá autoplay. */
export function unlockAudio(): void {
  if (unlocked) return
  unlocked = true
  const element = audioElement()
  if (element) {
    // play() rồi pause ngay: một số trình duyệt chỉ cho phát sau khi audio đã "được chạm".
    const promise = element.play()
    if (promise && typeof promise.then === 'function') {
      promise
        .then(() => {
          element.pause()
          element.currentTime = 0
        })
        .catch(() => undefined)
    }
  }
  const context = audioContext()
  if (context && context.state === 'suspended') void context.resume()
}

/** Tiếng "ding" dự phòng khi file mp3 không phát được. */
function playFallbackChime(volume = 0.16): void {
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

/** Phát tiếng thông báo; nếu mp3 lỗi thì dùng chime dự phòng. */
export function playSuccessSound(): void {
  if (!enabled.value) return
  const element = audioElement()
  if (!element) return
  try {
    element.currentTime = 0
  } catch {
    /* chưa load xong metadata — play() vẫn tự phát từ đầu */
  }
  const promise = element.play()
  if (promise && typeof promise.then === 'function') {
    promise.catch(() => playFallbackChime())
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
    playSuccessSound()   // nghe thử ngay khi bật
  }
}

export function useNotifySound() {
  return { enabled, setSoundEnabled, playSuccessSound, unlockAudio, SOUND_URL }
}

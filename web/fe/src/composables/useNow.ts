/**
 * Một đồng hồ chung (1 giây/tick) cho các countdown trên card.
 *
 * Vì sao không setInterval trong từng card: job 6.000 task = 6.000 interval, vừa tốn CPU vừa
 * lệch nhịp. Một ref dùng chung, mọi card tự tính `expires_at - now`.
 */
import { onScopeDispose, ref } from 'vue'

const now = ref(Math.floor(Date.now() / 1000))
let timer: ReturnType<typeof setInterval> | null = null
let users = 0

export function useNow() {
  users += 1
  if (!timer) {
    timer = setInterval(() => {
      now.value = Math.floor(Date.now() / 1000)
    }, 1000)
  }
  onScopeDispose(() => {
    users -= 1
    if (users <= 0 && timer) {
      clearInterval(timer)
      timer = null
    }
  })
  return now
}

import type { Clock } from './types'

/**
 * The wall clock. `sleep` resolves early when the signal aborts (without
 * rejecting: the caller checks the signal), and the timer is never unref'd:
 * a caller is awaiting it, so the process must stay alive for it.
 */
export const systemClock: Clock = {
  now: () => Date.now(),
  sleep (ms, signal) {
    return new Promise((resolve) => {
      if (signal?.aborted === true) {
        resolve()
        return
      }
      const onAbort = (): void => {
        clearTimeout(timer)
        resolve()
      }
      const timer = setTimeout(() => {
        signal?.removeEventListener('abort', onAbort)
        resolve()
      }, ms)
      signal?.addEventListener('abort', onAbort)
    })
  }
}

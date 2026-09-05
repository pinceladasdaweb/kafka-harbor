import type { Clock } from '../../src/index'

/**
 * Deterministic clock: now() is frozen until advance() moves it, and every
 * sleep is recorded so tests can assert the exact waits instead of racing
 * wall time. A sleep resolves when time is advanced past its deadline or
 * when its signal aborts.
 */
export class ManualClock implements Clock {
  time: number
  readonly sleeps: number[] = []
  private readonly pending: Array<{ until: number, resolve: () => void }> = []

  constructor (start = 1_700_000_000_000) {
    this.time = start
  }

  now (): number {
    return this.time
  }

  sleep (ms: number, signal?: AbortSignal): Promise<void> {
    this.sleeps.push(ms)
    return new Promise((resolve) => {
      if (ms <= 0 || signal?.aborted === true) {
        resolve()
        return
      }
      const entry = { until: this.time + ms, resolve }
      this.pending.push(entry)
      signal?.addEventListener('abort', () => {
        const index = this.pending.indexOf(entry)
        if (index >= 0) this.pending.splice(index, 1)
        resolve()
      }, { once: true })
    })
  }

  /** Moves time forward and releases every sleep whose deadline passed. */
  advance (ms: number): void {
    this.time += ms
    for (const entry of [...this.pending]) {
      if (entry.until <= this.time) {
        this.pending.splice(this.pending.indexOf(entry), 1)
        entry.resolve()
      }
    }
  }

  /** Sleeps currently waiting on the clock. */
  get waiting (): number {
    return this.pending.length
  }
}

/** Yields to the event loop a few times so promise chains settle. */
export async function settle (turns = 5): Promise<void> {
  for (let i = 0; i < turns; i++) await new Promise((resolve) => setImmediate(resolve))
}

/** Polls `condition` until it holds or `timeoutMs` passes. */
export async function until (condition: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('condition not met in time')
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

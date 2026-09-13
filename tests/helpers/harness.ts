import { createHarbor, type Harbor, type HarborConfig, type HarborEvents, type Logger } from '../../src/index'
import { memoryAdapter, type MemoryAdapter, type MemoryAdapterOptions } from '../../src/testing/index'
import { ManualClock } from './manual-clock'

export const silentLogger: Logger = {
  info: () => {},
  warn: () => {},
  error: () => {}
}

export interface Harness {
  adapter: MemoryAdapter
  clock: ManualClock
  harbor: Harbor
  logs: Array<{ level: 'info' | 'warn' | 'error', message: string }>
}

/** A harbor on the in-memory adapter with a frozen clock and a captured log. */
export function harness (config: Partial<HarborConfig> = {}, adapterOptions: MemoryAdapterOptions = {}): Harness {
  const clock = new ManualClock()
  const adapter = memoryAdapter({ now: () => clock.now(), ...adapterOptions })
  const logs: Harness['logs'] = []
  const logger: Logger = {
    info: (message) => { logs.push({ level: 'info', message }) },
    warn: (message) => { logs.push({ level: 'warn', message }) },
    error: (message) => { logs.push({ level: 'error', message }) }
  }
  const harbor = createHarbor({
    clientId: 'test-app',
    brokers: ['memory:9092'],
    adapter,
    clock,
    logger,
    headers: { correlationId: () => 'corr-fixed' },
    produceRetry: { attempts: 1 },
    ...config
  })
  return { adapter, clock, harbor, logs }
}

export const text = (buffer: Buffer | null): string | null => buffer === null ? null : buffer.toString('utf8')
export const json = (buffer: Buffer | null): unknown => buffer === null ? null : JSON.parse(buffer.toString('utf8'))

/** Every `error` event the harbor emits from now on, in order. */
export function captureErrors (harbor: Harbor): Array<HarborEvents['error']> {
  return captureEvents(harbor, 'error')
}

/** Every event of one kind the harbor emits from now on, in order. */
export function captureEvents<K extends keyof HarborEvents> (harbor: Harbor, event: K): Array<HarborEvents[K]> {
  const events: Array<HarborEvents[K]> = []
  harbor.on(event, (payload) => { events.push(payload) })
  return events
}

/** A promise a handler can wait on until the test lets it go. */
export interface Gate {
  readonly wait: Promise<void>
  readonly release: () => void
}

export function gate (): Gate {
  let release!: () => void
  const wait = new Promise<void>((resolve) => { release = resolve })
  return { wait, release }
}

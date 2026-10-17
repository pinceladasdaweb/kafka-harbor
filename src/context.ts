import type { RetryPolicy } from 'breakwater'
import type { Transaction, TransactionOffsets, TransactionOptions } from './transaction'

import type { HeaderNames } from './headers'
import type { Clock, Logger } from './types'
import type { ClientAdapter } from './adapter'
import type { Serializer } from './serializer'
import type { Instrumentation } from './instrumentation'

/** The payload of the `error` event, the same for every pipeline that emits it. */
export interface HarborErrorEvent {
  error: unknown
  scope: 'consumer' | 'producer' | 'adapter'
  groupId?: string
  topic?: string
}

/**
 * What the harbor hands to the pipelines it runs (consumers, redrives): the
 * client, the conventions in effect and the harbor's own lifecycle. One
 * shape, built once, so a field added here reaches every pipeline.
 */
export interface CoreContext<E extends object> {
  readonly adapter: ClientAdapter
  readonly clientId: string
  readonly serializer: Serializer
  readonly headerNames: HeaderNames
  /** Generates a correlation id for a message that carries none. */
  readonly correlationId: () => string
  readonly logger: Logger
  readonly clock: Clock
  /** Retry of the produce calls the pipeline makes on its own behalf. */
  readonly producePolicy: RetryPolicy
  readonly emit: <K extends keyof E>(event: K, payload: E[K]) => void
  /** Whether the harbor refuses new work that outlives a call (a consumer starting, a redrive): true from the moment a shutdown begins. */
  readonly isClosed: () => boolean
  /**
   * Aborts when the harbor stops producing for good: its shutdown is about
   * to release the client. Until then, handlers a shutdown drains (and the
   * application) still produce and run transactions. The reason is a
   * `ClosedError`.
   */
  readonly closed: AbortSignal
  /** Connects if needed; while a shutdown drains, the open connection serves until `closed` aborts. */
  readonly ensureConnected: () => Promise<void>
  /** Lets a shutdown wait for `work` (a send, a transaction) before it releases the client. */
  readonly track: <T>(work: Promise<T>) => Promise<T>
  /** The tracing hooks configured on the harbor, if any. */
  readonly instrumentation?: Instrumentation
  /** Runs a transaction on the harbor's transactional producer; a consumer hands it the offsets to commit along. */
  readonly transaction: <T>(fn: (tx: Transaction) => Promise<T>, options?: TransactionOptions, offsets?: TransactionOffsets) => Promise<T>
}

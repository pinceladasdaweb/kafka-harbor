import type { HandlerContext } from './consumer'
import type { Duration, Message } from './types'

/**
 * What a handler run is keyed by. The key names the intent (a business id,
 * a message id header); the payload, when given, is fingerprinted by the
 * engine to catch the same key reused for different content.
 */
export interface IdempotencyInput {
  readonly key: string
  readonly payload?: unknown
  /** Replay window for this run, overriding the engine's default. */
  readonly resultTtl?: Duration
}

/**
 * The engine a consumer runs its handlers through when `idempotency` is
 * configured: one execution per key, a repeat replays the first outcome
 * instead of running the handler again. quayside's `Idempotency` has this
 * shape as is; anything with the same method fits, and the core never
 * imports either.
 */
export interface IdempotencyEngine {
  executeWithMetadata: <T>(input: string | IdempotencyInput, run: () => Promise<T>) => Promise<{ value: T, replayed: boolean }>
}

export interface IdempotencyOptions<T = unknown> {
  /**
   * The engine. Its lock must outlive the handler: set the engine's lock
   * TTL above the longest a handler may run (`maxProcessingTime`, 5 minutes
   * by default), or a run that finishes after the lease expired is refused
   * by the engine and retried like a failure.
   */
  readonly engine: IdempotencyEngine
  /**
   * Derives the key of a delivery. Default: the group, topic, partition and
   * offset, which collapses the redeliveries at-least-once allows (a
   * rebalance or a crash after the handler ran and before the commit) and
   * nothing else. Return a business key (`message.value.orderId`) to
   * collapse duplicates the producer sent as well; add the payload to have
   * the engine refuse the same key with different content.
   */
  readonly key?: (message: Message<T>, context: HandlerContext) => string | IdempotencyInput
}

/**
 * The default key: one per delivery of one group. Exported so a key of your
 * own can build on it (`${defaultIdempotencyKey(message, context)}:${step}`).
 */
export const defaultIdempotencyKey = (message: Message, context: HandlerContext): string =>
  `${context.groupId}:${message.topic}:${message.partition}:${message.offset}`

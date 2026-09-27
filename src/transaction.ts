/**
 * Transactions: records produced inside `harbor.transaction()` become
 * visible to a read-committed consumer all at once when it commits, and
 * not at all when it aborts. Inside a handler, `ctx.transaction()` adds the
 * consumed offset to the same transaction, which is Kafka's exactly-once
 * between an input topic and the output topics: the produce and the offset
 * commit land together or not at all.
 *
 * One transaction at a time: a transactional producer holds one open
 * transaction, so calls queue behind each other. What happens outside Kafka
 * inside the function (a database write, an HTTP call) is not covered; the
 * function runs once per attempt and the message walks the ladder like any
 * other when it throws.
 */
import { toRecords } from './producer'
import type { Serializer } from './serializer'
import type { OutgoingMessage } from './types'
import { AsyncLocalStorage } from 'node:async_hooks'
import { produceRecords, type ProduceEvents } from './produce'
import type { CoreContext, HarborErrorEvent } from './context'

import { ClosedError, ConfigError, describeError } from './errors'
import type { ConsumerHandle, TopicPartitionOffset } from './adapter'

/** What the function given to `harbor.transaction()` produces with. */
export interface Transaction {
  send: <T = unknown>(topic: string, message: OutgoingMessage<T>) => Promise<void>
  sendBatch: <T = unknown>(topic: string, messages: readonly OutgoingMessage<T>[]) => Promise<void>
}

export interface TransactionOptions {
  /** Overrides the harbor-wide serializer for what this transaction produces. */
  serializer?: Serializer
}

/** The offsets a handler's transaction commits along with what it produced. */
export interface TransactionOffsets {
  readonly consumption: ConsumerHandle
  readonly offsets: readonly TopicPartitionOffset[]
}

export interface TransactionEvents extends ProduceEvents {
  /**
   * A transaction ended. `records` is what it sent, acknowledged by the
   * broker as it went (each send is a `messageProduced` of kind
   * `transaction`, before the outcome is known): visible when `committed`,
   * gone when `aborted`; `error` says why it aborted.
   */
  transactionCompleted: { outcome: 'committed' | 'aborted', records: number, durationMs: number, error?: unknown }
  error: HarborErrorEvent
}

/**
 * Runs transactions one after the other on the adapter's transactional
 * producer. Owned by the harbor; the consumers reach it through the core
 * context to commit the offsets of what they consumed.
 */
export class TransactionRunner {
  private readonly context: CoreContext<TransactionEvents>
  private turn: Promise<unknown> = Promise.resolve()
  /** Set while a transaction's function runs, so a transaction begun from inside it is refused instead of waiting for itself. */
  private readonly inside = new AsyncLocalStorage<true>()

  constructor (context: CoreContext<TransactionEvents>) {
    this.context = context
  }

  run<T> (fn: (tx: Transaction) => Promise<T>, options: TransactionOptions = {}, offsets?: TransactionOffsets): Promise<T> {
    if (typeof fn !== 'function') throw new ConfigError('transaction() takes a function that receives the transaction')
    if (this.inside.getStore() === true) throw new ConfigError('transaction() called inside a transaction: one runs at a time, and the inner one would wait for the outer one forever')
    // `turn` never rejects (the failure of one transaction is its caller's, not the next one's), so the chain waits and nothing more.
    const next = this.turn.then(async () => await this.execute(fn, options, offsets))
    this.turn = next.catch(() => undefined)
    return next
  }

  private async execute<T> (fn: (tx: Transaction) => Promise<T>, options: TransactionOptions, offsets: TransactionOffsets | undefined): Promise<T> {
    const { adapter, clock } = this.context
    if (this.context.isClosed()) throw new ClosedError('harbor')
    if (adapter.transaction === undefined) {
      throw new ConfigError(`adapter "${adapter.name}" has no transactions; the Confluent, platformatic and memory adapters do, with transactionalId set on the harbor`)
    }
    await this.context.ensureConnected()
    const startedAt = clock.now()
    const handle = await adapter.transaction()
    const serializer = options.serializer ?? this.context.serializer
    let records = 0
    const tx: Transaction = {
      send: async (topic, message) => { await tx.sendBatch(topic, [message]) },
      sendBatch: async (topic, messages) => {
        const outgoing = await toRecords(this.context, serializer, topic, messages)
        if (outgoing.length === 0) return
        // No retry inside a transaction: a produce that failed aborts it, and the caller's ladder decides what comes next.
        await produceRecords(this.context, { topic, kind: 'transaction' }, outgoing, null, async (batch) => { await handle.produce(batch) })
        records += outgoing.length
      }
    }
    let result: T
    try {
      result = await this.inside.run(true, async () => await fn(tx))
      if (offsets !== undefined) await handle.sendOffsets(offsets.consumption, offsets.offsets)
      await handle.commit()
    } catch (error) {
      // The abort is best effort: a producer the broker fenced, or a
      // connection that went, has nothing left to abort, and the timeout
      // aborts the transaction on the broker's side anyway.
      await handle.abort().catch((abortError: unknown) => {
        this.context.logger.warn(`[kafka-harbor] transaction abort failed after ${describeError(error)}: ${describeError(abortError)}`)
      })
      this.context.emit('transactionCompleted', { outcome: 'aborted', records, durationMs: clock.now() - startedAt, error })
      throw error
    }
    this.context.emit('transactionCompleted', { outcome: 'committed', records, durationMs: clock.now() - startedAt })
    return result
  }
}

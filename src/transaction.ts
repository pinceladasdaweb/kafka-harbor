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

import { ClosedError, ConfigError, describeError, isFencedError } from './errors'
import type { ConsumerHandle, TopicPartitionOffset, TransactionHandle } from './adapter'

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
   * broker as it went (each send is a `batchProduced` of kind
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
  /** Settles once every transaction asked for so far has ended: the next one starts after it. Never rejects. */
  private turn: Promise<unknown> = Promise.resolve()
  /**
   * The calls waiting for their turn. When the harbor stops producing they
   * are refused with ClosedError: a function that never settles holds the
   * transaction in progress, and must not hold every later one forever.
   */
  private readonly queued = new Set<(error: unknown) => void>()
  /** The transaction whose function runs in this async context, so one begun from inside it is refused instead of waiting for itself. */
  private readonly inside = new AsyncLocalStorage<{ open: boolean }>()

  constructor (context: CoreContext<TransactionEvents>) {
    this.context = context
    context.closed.addEventListener('abort', () => {
      for (const refuse of this.queued) refuse(new ClosedError('harbor'))
      this.queued.clear()
    }, { once: true })
  }

  run<T> (fn: (tx: Transaction) => Promise<T>, options: TransactionOptions = {}, offsets?: TransactionOffsets): Promise<T> {
    if (typeof fn !== 'function') throw new ConfigError('transaction() takes a function that receives the transaction')
    if (this.inside.getStore()?.open === true) throw new ConfigError('transaction() called inside a transaction: one runs at a time, and the inner one would wait for the outer one forever')
    if (this.context.closed.aborted) return Promise.reject(new ClosedError('harbor'))
    const previous = this.turn
    let refuse!: (error: unknown) => void
    const refused = new Promise<never>((_resolve, reject) => { refuse = reject })
    this.queued.add(refuse)
    const next = Promise.race([previous, refused]).then(async () => {
      this.queued.delete(refuse)
      return await this.execute(fn, options, offsets)
    })
    // The next in line waits for this one to end and for the one before it,
    // even when this one was refused while waiting: transactions never
    // overlap. The turn keeps no result: a chain of them would hold every
    // transaction's outcome for the life of the harbor.
    this.turn = Promise.allSettled([previous, next]).then(() => undefined)
    return this.context.track(next)
  }

  private async execute<T> (fn: (tx: Transaction) => Promise<T>, options: TransactionOptions, offsets: TransactionOffsets | undefined): Promise<T> {
    const { adapter, clock } = this.context
    if (adapter.transaction === undefined) {
      throw new ConfigError(`adapter "${adapter.name}" has no transactions; the Confluent, platformatic and memory adapters do, with transactionalId set on the harbor`)
    }
    await this.context.ensureConnected()
    const startedAt = clock.now()
    let handle: TransactionHandle
    try {
      handle = await adapter.transaction()
    } catch (error) {
      // Nothing was sent and there is nothing to abort; it still counts as a transaction that did not commit.
      this.context.emit('transactionCompleted', { outcome: 'aborted', records: 0, durationMs: clock.now() - startedAt, error })
      throw error
    }
    const serializer = options.serializer ?? this.context.serializer
    let records = 0
    const scope = { open: true }
    /** Every send begun while the function ran, awaited or not: each one lands in this transaction, or fails it, before the outcome. */
    const sends: Array<Promise<void>> = []
    const tx: Transaction = {
      send: async (topic, message) => await tx.sendBatch(topic, [message]),
      sendBatch: async (topic, messages) => {
        // After the function ended, the transaction is committing or gone: a
        // late send would land in the next one, or in none.
        if (!scope.open) throw new ClosedError('transaction')
        if (this.context.closed.aborted) throw new ClosedError('harbor')
        const work = (async () => {
          const outgoing = await toRecords(this.context, serializer, topic, messages)
          if (outgoing.length === 0) return
          // No retry inside a transaction: a produce that failed aborts it, and the caller's ladder decides what comes next.
          await produceRecords(this.context, { topic, kind: 'transaction' }, outgoing, null, async (batch) => { await handle.produce(batch) })
          records += outgoing.length
        })()
        sends.push(work)
        return await work
      }
    }
    let result: T
    try {
      try {
        result = await this.inside.run(scope, async () => await fn(tx))
      } finally {
        scope.open = false
      }
      await Promise.all(sends)
      // The shutdown released the client while the function ran: nothing left to commit through.
      if (this.context.closed.aborted) throw new ClosedError('harbor')
      if (offsets !== undefined) await handle.sendOffsets(offsets.consumption, offsets.offsets)
      await handle.commit()
    } catch (thrown) {
      const settled = await Promise.allSettled(sends)
      // A send the broker refused because another producer took the id
      // says more than whatever the function threw after it: the caller
      // has to stop, not retry.
      const fenced = settled.find((entry): entry is PromiseRejectedResult => entry.status === 'rejected' && isFencedError(entry.reason))
      const error: unknown = isFencedError(thrown) || fenced === undefined ? thrown : fenced.reason
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

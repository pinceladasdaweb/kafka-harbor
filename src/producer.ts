import { isRetryable } from './errors'
import { stampProducer } from './headers'
import type { RawRecord } from './adapter'
import type { Serializer } from './serializer'
import type { OutgoingMessage } from './types'
import type { CoreContext, HarborErrorEvent } from './context'
import { produceRecords, type ProduceEvents } from './produce'
import { requireNonEmptyString, requirePositiveInteger } from './validate'
import { exponential, retry, type Backoff, type RetryPolicy } from 'breakwater'

export interface ProducerRetryOptions {
  /** Total attempts per produce call, including the first. Default: 5. */
  attempts?: number
  /** Delay strategy between attempts. Default: exponential, full jitter, 100ms to 5s. */
  backoff?: Backoff
}

export interface ProducerOptions<T = unknown> {
  /** Overrides the harbor-wide serializer for this producer. */
  serializer?: Serializer<T>
  /**
   * Retry of transient broker failures. The client already retries at the
   * protocol level; this is the application-level ladder on top of it,
   * driven by breakwater. `attempts: 1` disables it.
   */
  retry?: ProducerRetryOptions
}

export interface ProducerEvents extends ProduceEvents {
  error: HarborErrorEvent
}

/** What the core needs from the harbor to build a producer. */
export type ProducerContext = CoreContext<ProducerEvents>

export function buildProducerRetry (options: ProducerRetryOptions | undefined): RetryPolicy {
  const attempts = requirePositiveInteger(options?.attempts ?? 5, 'producer retry.attempts')
  return retry({
    attempts,
    backoff: options?.backoff ?? exponential({ initial: 100, max: 5_000, jitter: 'full' }),
    retryIf: isRetryable
  })
}

export class Producer<T = unknown> {
  private readonly context: ProducerContext
  private readonly serializer: Serializer<T>
  private readonly policy: RetryPolicy

  constructor (context: ProducerContext, options: ProducerOptions<T> = {}) {
    this.context = context
    this.serializer = options.serializer ?? (context.serializer as Serializer<T>)
    this.policy = buildProducerRetry(options.retry)
  }

  async send (topic: string, message: OutgoingMessage<T>): Promise<void> {
    await this.sendBatch(topic, [message])
  }

  /**
   * Produces every message and resolves once the broker acknowledged all of
   * them; the harbor connects first if it has not yet. Serialization happens
   * before any byte leaves the process, so a batch with one unencodable
   * value produces nothing. A `null` value is a tombstone: no value bytes at
   * all, which is what a compacted topic reads as "delete this key"; the
   * serializer never sees it.
   *
   * A batch that the broker acknowledged is reported as `messageProduced`
   * with kind `send`; one that failed after the retries is reported as
   * `error` with the producer scope and rejected to the caller. A connection
   * that fails is the harbor's to report (once, with the adapter scope) and
   * rejects the send without a producer error.
   */
  async sendBatch (topic: string, messages: readonly OutgoingMessage<T>[]): Promise<void> {
    requireNonEmptyString(topic, 'topic')
    // One instant for the whole batch: the records leave together.
    const stamp = { clientId: this.context.clientId, at: new Date(this.context.clock.now()), correlationId: this.context.correlationId }
    const records = messages.map((message): RawRecord => ({
      topic,
      key: message.key === undefined || message.key === null ? null : Buffer.from(message.key, 'utf8'),
      value: message.value === null ? null : this.serializer.serialize(message.value, topic),
      headers: stampProducer(message.headers ?? {}, this.context.headerNames, stamp),
      partition: message.partition
    }))
    if (records.length === 0) return
    await this.context.ensureConnected()
    try {
      await produceRecords(this.context, { topic, kind: 'send' }, records, this.policy)
    } catch (error) {
      this.context.emit('error', { error, scope: 'producer', topic })
      throw error
    }
  }
}

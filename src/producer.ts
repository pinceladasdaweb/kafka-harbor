import { ConfigError, isRetryable } from './errors'
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

/**
 * The records a batch of outgoing messages becomes, serialized before any
 * byte leaves: one unencodable value means no record at all, a serializer
 * that is asynchronous (a schema registry) is awaited, and every record
 * carries the same produced-at instant and the automatic headers.
 */
export async function toRecords<T> (context: Pick<CoreContext<ProduceEvents>, 'clientId' | 'clock' | 'correlationId' | 'headerNames'>, serializer: Serializer<T>, topic: string, messages: readonly OutgoingMessage<T>[]): Promise<RawRecord[]> {
  requireNonEmptyString(topic, 'topic')
  if (!Array.isArray(messages)) throw new ConfigError(`messages for topic "${topic}" must be an array`)
  // A shape the client cannot encode would fail inside it with an error
  // nobody marked as definitive, and be retried as if the broker were away.
  for (const message of messages) assertSendable(message, topic)
  const stamp = { clientId: context.clientId, at: new Date(context.clock.now()), correlationId: context.correlationId }
  return await Promise.all(messages.map(async (message): Promise<RawRecord> => ({
    topic,
    // A copy for bytes, UTF-8 for text: what the caller does with its buffer afterwards is its own.
    key: message.key === undefined || message.key === null ? null : Buffer.from(message.key),
    value: message.value === null ? null : await serializer.serialize(message.value, topic),
    headers: stampProducer(message.headers ?? {}, context.headerNames, stamp),
    partition: message.partition
  })))
}

const describeType = (value: unknown): string => value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value

const assertSendable = (message: OutgoingMessage<unknown>, topic: string): void => {
  if (typeof message !== 'object' || message === null) throw new ConfigError(`a message for topic "${topic}" must be an object; got ${describeType(message)}`)
  const { key, headers } = message
  if (key !== undefined && key !== null && typeof key !== 'string' && !Buffer.isBuffer(key)) throw new ConfigError(`the key of a message for topic "${topic}" must be a string or a Buffer; got ${describeType(key)}`)
  if (headers === undefined) return
  if (typeof headers !== 'object' || headers === null || Array.isArray(headers)) throw new ConfigError(`the headers of a message for topic "${topic}" must be an object of strings; got ${describeType(headers)}`)
  for (const [name, value] of Object.entries(headers)) {
    if (typeof value !== 'string') throw new ConfigError(`header "${name}" of a message for topic "${topic}" must be a string; got ${describeType(value)}`)
  }
}

/** Sends messages of one type, built by `harbor.producer()`. */
export interface Producer<T = unknown> {
  /** Produces one message and resolves once the broker acknowledged it: `sendBatch` with a batch of one. */
  send (topic: string, message: OutgoingMessage<T>): Promise<void>
  /**
   * Produces every message and resolves once the broker acknowledged all of
   * them; the harbor connects first if it has not yet. Serialization happens
   * before any byte leaves the process, so a batch with one unencodable
   * value produces nothing. A `null` value is a tombstone: no value bytes at
   * all, which is what a compacted topic reads as "delete this key"; the
   * serializer never sees it.
   *
   * A batch that the broker acknowledged is reported as `batchProduced`
   * with kind `send`; one that failed after the retries is reported as
   * `error` with the producer scope and rejected to the caller. A connection
   * that fails is the harbor's to report (once, with the adapter scope) and
   * rejects the send without a producer error.
   */
  sendBatch (topic: string, messages: readonly OutgoingMessage<T>[]): Promise<void>
}

/** The producer the harbor builds; `Producer` is what the application sees of it. */
export class ProducerPipeline<T = unknown> implements Producer<T> {
  private readonly context: ProducerContext
  private readonly serializer: Serializer<T>
  private readonly policy: RetryPolicy

  /** @internal Built by `harbor.producer()`, the one way to get a producer. */
  constructor (context: ProducerContext, options: ProducerOptions<T> = {}) {
    this.context = context
    this.serializer = options.serializer ?? (context.serializer as Serializer<T>)
    this.policy = buildProducerRetry(options.retry)
  }

  async send (topic: string, message: OutgoingMessage<T>): Promise<void> {
    await this.sendBatch(topic, [message])
  }

  async sendBatch (topic: string, messages: readonly OutgoingMessage<T>[]): Promise<void> {
    await this.context.track(this.produce(topic, messages))
  }

  private async produce (topic: string, messages: readonly OutgoingMessage<T>[]): Promise<void> {
    const records = await toRecords(this.context, this.serializer, topic, messages)
    if (records.length === 0) return
    await this.context.ensureConnected()
    try {
      await produceRecords(this.context, { topic, kind: 'send' }, records, this.policy)
    } catch (error) {
      // Refused because the harbor released the client (a retry cut short):
      // the caller's ClosedError, not a failure of the broker, as when the
      // send is refused before it starts.
      if (error !== this.context.closed.reason) this.context.emit('error', { error, scope: 'producer', topic })
      throw error
    }
  }
}

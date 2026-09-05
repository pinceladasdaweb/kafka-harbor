import type { HeaderNames } from './headers'
import type { Serializer } from './serializer'
import type { ClientAdapter, RawRecord } from './adapter'
import type { Clock, MessageHeaders, OutgoingMessage } from './types'
import { ConfigError, isRetryable } from './errors'
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

/** What the core needs from the harbor to build a producer. */
export interface ProducerContext {
  readonly adapter: ClientAdapter
  readonly clientId: string
  readonly serializer: Serializer
  readonly headerNames: HeaderNames
  readonly correlationId: () => string
  readonly clock: Clock
  readonly ensureConnected: () => Promise<void>
}

export function buildProducerRetry (options: ProducerRetryOptions | undefined): RetryPolicy {
  const attempts = options?.attempts ?? 5
  if (!Number.isInteger(attempts) || attempts < 1) {
    throw new ConfigError(`producer retry.attempts must be an integer >= 1; got ${String(attempts)}`)
  }
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
   * them. Serialization happens before any byte leaves the process, so a
   * batch with one unencodable value produces nothing.
   */
  async sendBatch (topic: string, messages: readonly OutgoingMessage<T>[]): Promise<void> {
    if (typeof topic !== 'string' || topic === '') {
      throw new ConfigError('topic must be a non-empty string')
    }
    const records = messages.map((message) => this.toRecord(topic, message))
    if (records.length === 0) return
    await this.context.ensureConnected()
    await this.policy.execute(() => this.context.adapter.produce(records))
  }

  private toRecord (topic: string, message: OutgoingMessage<T>): RawRecord {
    const names = this.context.headerNames
    const headers: MessageHeaders = { ...message.headers }
    headers[names.correlationId] ??= this.context.correlationId()
    headers[names.producedAt] = new Date(this.context.clock.now()).toISOString()
    headers[names.producer] = this.context.clientId
    return {
      topic,
      key: message.key === undefined || message.key === null ? null : Buffer.from(message.key, 'utf8'),
      value: this.serializer.serialize(message.value, topic),
      headers,
      partition: message.partition
    }
  }
}

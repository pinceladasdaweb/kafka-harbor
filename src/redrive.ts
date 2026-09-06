import type { RetryPolicy } from 'breakwater'

import type { ClientAdapter, ConsumerHandle, RawMessage, RawRecord } from './adapter'
import { parseDuration } from './duration'
import { ClosedError, ConfigError, describeError, isSerializationError } from './errors'
import { decodeHeaders, readRetryInfo, type HeaderNames } from './headers'
import type { Serializer } from './serializer'
import type { Clock, Duration, Logger, Message, MessageHeaders } from './types'

export interface RedriveOptions {
  /** The dead-letter topic to drain. */
  from: string
  /**
   * Where to re-inject. Default: each message's own `x-original-topic`
   * header, which is what the DLQ hop wrote. A message without that header
   * fails the redrive (nothing is committed for it) unless `to` is given.
   */
  to?: string
  /** Consumer group used to read the DLQ. Default: `${from}-redrive`. */
  groupId?: string
  /** Stop after this many messages were re-injected or skipped. Default: no limit. */
  max?: number
  /** Stop once no message arrived for this long. Default: '5s'. */
  idleTimeout?: Duration
  /**
   * Decides per message. `false` skips it: the offset is committed and the
   * message is not re-injected. The message is deserialized with the
   * serializer given (default: the harbor's); one that does not deserialize
   * is skipped and reported through `error`.
   */
  filter?: (message: Message) => boolean | Promise<boolean>
  serializer?: Serializer
}

export interface RedriveResult {
  readonly from: string
  readonly reprocessed: number
  readonly skipped: number
}

export interface RedriveEvents extends Record<string, unknown> {
  messageRedriven: { from: string, to: string, partition: number, offset: string, groupId: string, correlationId: string | undefined }
  error: { error: unknown, scope: 'consumer' | 'producer' | 'adapter' | 'listener', groupId?: string, topic?: string }
}

export interface RedriveContext {
  readonly adapter: ClientAdapter
  readonly clientId: string
  readonly serializer: Serializer
  readonly headerNames: HeaderNames
  readonly logger: Logger
  readonly clock: Clock
  readonly producePolicy: RetryPolicy
  readonly emit: <K extends keyof RedriveEvents>(event: K, payload: RedriveEvents[K]) => void
  readonly isClosed: () => boolean
  readonly ensureConnected: () => Promise<void>
}

/**
 * Drains a dead-letter topic back into service. Every message is re-produced
 * with its ORIGINAL key and value; the tracking headers of the failed run
 * are removed so the message starts a fresh ladder, and two headers record
 * the redrive itself. The DLQ offset is committed only after the broker
 * acknowledged the re-produce: a redrive interrupted halfway resumes where
 * it stopped, and never loses a message on the way back.
 */
export async function redrive (context: RedriveContext, options: RedriveOptions): Promise<RedriveResult> {
  if (typeof options?.from !== 'string' || options.from === '') throw new ConfigError('redrive.from must be a non-empty string')
  if (options.to !== undefined && (typeof options.to !== 'string' || options.to === '')) throw new ConfigError('redrive.to must be a non-empty string')
  if (options.to === options.from) throw new ConfigError('redrive.to must differ from redrive.from: re-injecting a topic into itself never ends')
  if (options.max !== undefined && (!Number.isInteger(options.max) || options.max < 1)) {
    throw new ConfigError(`redrive.max must be an integer >= 1; got ${String(options.max)}`)
  }
  const idleMs = parseDuration(options.idleTimeout ?? '5s', 'redrive.idleTimeout')
  const groupId = options.groupId ?? `${options.from}-redrive`
  const serializer = options.serializer ?? context.serializer
  const names = context.headerNames
  const tracking = new Set([names.retryCount, names.originalTopic, names.firstFailureAt, names.lastError, names.deadLetteredAt])
  if (context.isClosed()) throw new ClosedError('harbor')
  await context.ensureConnected()

  let reprocessed = 0
  let skipped = 0
  let lastActivity = context.clock.now()
  let failure: unknown
  const stopController = new AbortController()
  const finish = (): void => { stopController.abort() }
  const inFlight = new Set<Promise<void>>()
  // The handle arrives when consume() resolves; an adapter may deliver
  // before that resolution is observed here, so the closures read it late.
  const consumption: { handle?: ConsumerHandle } = {}

  const commit = async (raw: RawMessage): Promise<void> => {
    await (consumption.handle as ConsumerHandle).commit([{ topic: raw.topic, partition: raw.partition, offset: (BigInt(raw.offset) + 1n).toString() }])
  }

  const one = async (raw: RawMessage): Promise<void> => {
    if (stopController.signal.aborted) return
    lastActivity = context.clock.now()
    const headers = decodeHeaders(raw.headers)
    const target = options.to ?? headers[names.originalTopic]
    if (target === undefined || target.trim() === '') {
      throw new ConfigError(`message ${raw.topic}[${raw.partition}]@${raw.offset} carries no "${names.originalTopic}" header; pass redrive.to to choose the destination`)
    }
    // The header comes from the network: one that names the topic being
    // drained would send the message round in circles, growing the topic
    // until the process runs out of memory.
    if (target === options.from) {
      throw new ConfigError(`message ${raw.topic}[${raw.partition}]@${raw.offset} names the topic being drained as its "${names.originalTopic}"; pass redrive.to to choose another destination`)
    }
    if (options.filter !== undefined) {
      let keep: boolean
      try {
        const message = toMessage(raw, headers, serializer, names)
        keep = await options.filter(message)
      } catch (error) {
        if (!isSerializationError(error)) throw error
        context.emit('error', { error, scope: 'consumer', groupId, topic: raw.topic })
        keep = false
      }
      if (!keep) {
        await commit(raw)
        skipped++
        return
      }
    }
    const outgoing: MessageHeaders = {}
    for (const [name, value] of Object.entries(headers)) {
      if (!tracking.has(name)) outgoing[name] = value
    }
    const now = new Date(context.clock.now()).toISOString()
    outgoing[names.redrivenFrom] = raw.topic
    outgoing[names.redrivenAt] = now
    outgoing[names.producedAt] = now
    outgoing[names.producer] = context.clientId
    const record: RawRecord = { topic: target, key: raw.key, value: raw.value, headers: outgoing }
    await context.producePolicy.execute(() => context.adapter.produce([record]))
    await commit(raw)
    reprocessed++
    context.emit('messageRedriven', { from: raw.topic, to: target, partition: raw.partition, offset: raw.offset, groupId, correlationId: headers[names.correlationId] })
  }

  consumption.handle = await context.adapter.consume({
    groupId,
    topics: [options.from],
    fromBeginning: true,
    concurrency: 1,
    eachMessage: (raw) => {
      const work = one(raw)
        .then(() => {
          if (options.max !== undefined && reprocessed + skipped >= options.max) finish()
        })
        .catch((error: unknown) => {
          failure ??= error
          finish()
        })
      inFlight.add(work)
      return work.finally(() => inFlight.delete(work))
    },
    onError: (error) => context.emit('error', { error, scope: 'adapter', groupId, topic: options.from })
  })

  try {
    // Idle detection: wake up every idleMs and check whether anything landed
    // since the last check. A stop (max reached, or a failure) cuts the wait.
    while (!stopController.signal.aborted) {
      await context.clock.sleep(idleMs, stopController.signal)
      if (stopController.signal.aborted) break
      if (context.clock.now() - lastActivity >= idleMs && inFlight.size === 0) break
    }
    await Promise.allSettled([...inFlight])
  } finally {
    stopController.abort()
    await consumption.handle.stop()
  }
  if (failure !== undefined) {
    context.logger.error(`[kafka-harbor] redrive of "${options.from}" stopped: ${describeError(failure)}`)
    throw failure
  }
  return { from: options.from, reprocessed, skipped }
}

const toMessage = (raw: RawMessage, headers: MessageHeaders, serializer: Serializer, names: HeaderNames): Message => {
  const retry = readRetryInfo(headers, names)
  return {
    topic: raw.topic,
    partition: raw.partition,
    offset: raw.offset,
    key: raw.key === null ? null : raw.key.toString('utf8'),
    value: raw.value === null ? null : serializer.deserialize(raw.value, retry?.originalTopic ?? raw.topic),
    headers,
    timestamp: new Date(raw.timestamp),
    ...(retry !== undefined && { retry })
  }
}

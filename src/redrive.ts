import { toMessage } from './message'
import { commitAfter } from './commit'
import { parseDuration } from './duration'
import type { Serializer } from './serializer'
import { produceHop, type ProduceEvents } from './produce'
import type { CoreContext, HarborErrorEvent } from './context'
import type { Duration, Message, MessageHeaders } from './types'
import type { ConsumerHandle, RawMessage } from './adapter'
import { decodeHeaders, readRetryInfo } from './headers'
import { requireNonEmptyString, requirePositiveInteger } from './validate'
import { ClosedError, ConfigError, describeError, isSerializationError } from './errors'

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

export interface RedriveEvents extends ProduceEvents {
  messageRedriven: { from: string, to: string, partition: number, offset: string, groupId: string, correlationId: string | undefined }
  error: HarborErrorEvent
}

export type RedriveContext = CoreContext<RedriveEvents>

/**
 * Drains a dead-letter topic back into service. Every message is re-produced
 * with its ORIGINAL key and value; the tracking headers of the failed run
 * are removed so the message starts a fresh ladder, and two headers record
 * the redrive itself. The DLQ offset is committed only after the broker
 * acknowledged the re-produce: a redrive interrupted halfway resumes where
 * it stopped, and never loses a message on the way back.
 */
export async function redrive (context: RedriveContext, options: RedriveOptions): Promise<RedriveResult> {
  requireNonEmptyString(options?.from, 'redrive.from')
  if (options.to !== undefined) requireNonEmptyString(options.to, 'redrive.to')
  if (options.to === options.from) throw new ConfigError('redrive.to must differ from redrive.from: re-injecting a topic into itself never ends')
  if (options.max !== undefined) requirePositiveInteger(options.max, 'redrive.max')
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
  // before that resolution is observed here, so every delivery waits for it
  // and no message is handled without a way to commit it.
  const consumption: { handle?: ConsumerHandle } = {}
  let markReady!: () => void
  const ready = new Promise<void>((resolve) => { markReady = resolve })

  // The DLQ offset advances the same way a consumer's does: a commit that
  // fails after the re-produce was acknowledged is reported and the run goes
  // on, since the only consequence is a repeat on the next redrive, which
  // at-least-once already allows.
  const commit = async (raw: RawMessage): Promise<boolean> =>
    await commitAfter(consumption.handle as ConsumerHandle, raw, { groupId, logger: context.logger, emit: (event, payload) => context.emit(event, payload) })

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
        const retry = readRetryInfo(headers, names)
        const message = toMessage(raw, headers, serializer, retry?.originalTopic ?? raw.topic, retry)
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
    const kept: MessageHeaders = {}
    for (const [name, value] of Object.entries(headers)) {
      if (!tracking.has(name)) kept[name] = value
    }
    const at = new Date(context.clock.now())
    const record = await produceHop(context, raw, headers, target, 'redrive', { ...kept, [names.redrivenFrom]: raw.topic, [names.redrivenAt]: at.toISOString() }, at)
    await commit(raw)
    reprocessed++
    context.emit('messageRedriven', { from: raw.topic, to: target, partition: raw.partition, offset: raw.offset, groupId, correlationId: record.headers[names.correlationId] })
  }

  consumption.handle = await context.adapter.consume({
    groupId,
    topics: [options.from],
    fromBeginning: true,
    concurrency: 1,
    eachMessage: (raw) => {
      const work = ready
        .then(() => one(raw))
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
  markReady()

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

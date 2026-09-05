import {
  ClosedError,
  ConfigError,
  ShutdownTimeoutError,
  TopicMissingError,
  describeError,
  isAbortProcessingError,
  isRetryable,
  isSerializationError
} from './errors'
import {
  TopicPlan,
  defaultDlqTopicNaming,
  defaultRetryTopicNaming,
  resolveRetryLevels,
  type DlqTopicNaming,
  type ResolvedRetryLevel,
  type RetryLevel,
  type RetryTopicNaming
} from './retry-topics'
import { parseDuration } from './duration'
import type { RetryPolicy } from 'breakwater'
import type { Serializer } from './serializer'
import type { Clock, Duration, MessageHeaders, Logger, Message } from './types'
import { decodeHeaders, readRetryInfo, writeRetryInfo, type HeaderNames } from './headers'
import type { ClientAdapter, ConsumerHandle, RawMessage, RawRecord, TopicSpec } from './adapter'

/** What a handler receives besides the message. */
export interface HandlerContext {
  /** The correlation id read from the headers, if the producer sent one. */
  readonly correlationId: string | undefined
  readonly logger: Logger
  /**
   * Aborts when the harbor gave up waiting for this handler during shutdown.
   * Long-running handlers observe it to stop early; the offset of an
   * aborted handler is not committed and the message is redelivered.
   */
  readonly signal: AbortSignal
  /** 1 on the first delivery; the retry count plus one on a retry topic. */
  readonly attempt: number
}

export type Handler<T = unknown> = (message: Message<T>, context: HandlerContext) => Promise<void> | void

export interface SubscribeOptions<T = unknown> {
  /** Overrides the consumer's serializer for this topic. */
  serializer?: Serializer<T>
}

export interface ConsumerRetryOptions {
  /**
   * The retry ladder: one topic per level, each with its own delay. An
   * empty ladder (the default) sends a failed message straight to the DLQ.
   */
  levels?: readonly RetryLevel[]
  /**
   * Decides whether a failure is worth retrying. Default: every error is
   * retried unless it carries `retryable: false`.
   */
  retryIf?: (error: unknown) => boolean
  topicNaming?: RetryTopicNaming
}

export interface ConsumerDlqOptions {
  /** Default: true. With the DLQ off and no retry level left, a failure stops the consumer. */
  enabled?: boolean
  topicNaming?: DlqTopicNaming
}

export interface TopicDefaults {
  /** Partitions for auto-created retry and DLQ topics. Default: 1. */
  partitions?: number
  /** Replication factor for auto-created topics. Default: 1. */
  replicationFactor?: number
}

export interface ConsumerOptions {
  groupId: string
  /** Overrides the harbor-wide serializer for every topic of this consumer. */
  serializer?: Serializer
  retry?: ConsumerRetryOptions
  dlq?: ConsumerDlqOptions
  /** How many partitions are processed at the same time. Order is preserved within each. Default: 1. */
  concurrency?: number
  /** Create the retry and DLQ topics through the Admin API on start. Default: false. */
  autoCreateTopics?: boolean
  topicDefaults?: TopicDefaults
  /** Whether a brand-new group starts from the earliest offset. Default: false. */
  fromBeginning?: boolean
  /**
   * The longest a message may sit in the pipeline (retry delay included)
   * before the group would consider the consumer dead. Default: '5m',
   * Kafka's `max.poll.interval.ms`. Every retry delay must fit under it.
   */
  maxProcessingTime?: Duration
}

export interface ConsumerEvents extends Record<string, unknown> {
  messageProcessed: { topic: string, partition: number, offset: string, groupId: string, durationMs: number, correlationId: string | undefined }
  messageRetried: { topic: string, partition: number, offset: string, groupId: string, retryTopic: string, level: number, attempt: number, error: unknown, correlationId: string | undefined }
  messageDeadLettered: { topic: string, partition: number, offset: string, groupId: string, dlqTopic: string, attempts: number, error: unknown, correlationId: string | undefined }
  messageFailed: { topic: string, partition: number, offset: string, groupId: string, error: unknown, durationMs: number, outcome: FailureOutcome, correlationId: string | undefined }
  consumerStopped: { groupId: string, reason: StopReason }
  error: { error: unknown, scope: 'consumer' | 'producer' | 'adapter' | 'listener', groupId?: string, topic?: string }
}

export type FailureOutcome = 'retry' | 'dead-letter' | 'abort' | 'crash'
export type StopReason = 'shutdown' | 'abort' | 'crash'
export type ConsumerState = 'idle' | 'starting' | 'running' | 'stopping' | 'stopped'

/** What the core needs from the harbor to build a consumer. */
export interface ConsumerContext {
  readonly adapter: ClientAdapter
  readonly clientId: string
  readonly serializer: Serializer
  readonly headerNames: HeaderNames
  readonly logger: Logger
  readonly clock: Clock
  readonly emit: <K extends keyof ConsumerEvents>(event: K, payload: ConsumerEvents[K]) => void
  readonly producePolicy: RetryPolicy
  readonly isClosed: () => boolean
  readonly ensureConnected: () => Promise<void>
}

interface Subscription {
  readonly plan: TopicPlan
  readonly handler: Handler
  readonly serializer: Serializer
}

/** One consumed topic: the subscription it belongs to and its place on the ladder. */
interface Route {
  readonly subscription: Subscription
  /** 0 for the original topic, 1..N for a retry topic. */
  readonly level: number
  /** How long a message waits on this topic before the handler runs. */
  readonly delayMs: number
}

const DEFAULT_MAX_PROCESSING_TIME_MS = 300_000

export class Consumer {
  readonly groupId: string
  private readonly context: ConsumerContext
  private readonly options: ConsumerOptions
  private readonly levels: readonly ResolvedRetryLevel[]
  private readonly retryIf: (error: unknown) => boolean
  private readonly retryNaming: RetryTopicNaming
  private readonly dlqNaming: DlqTopicNaming | undefined
  private readonly maxProcessingTimeMs: number
  private readonly concurrency: number
  private readonly subscriptions = new Map<string, Subscription>()
  private readonly routes = new Map<string, Route>()
  /**
   * Every delivery the adapter handed over and has not been told is done,
   * with whether its handler has started. A message waiting for its retry
   * delay is a delivery, not a running handler: shutdown cuts the wait short
   * at once and only ever waits for handlers.
   */
  private readonly deliveries = new Map<Promise<void>, { active: boolean }>()
  /** Aborts as soon as a stop begins: wakes retry waits, blocks new handlers. */
  private readonly drainController = new AbortController()
  /** Aborts when shutdown gives up on running handlers (the handler's `signal`). */
  private readonly shutdownController = new AbortController()

  private handle: ConsumerHandle | undefined
  private state: ConsumerState = 'idle'
  private stopping: Promise<void> | undefined
  private readonly ready: Promise<void>
  private markReady!: () => void

  constructor (context: ConsumerContext, options: ConsumerOptions) {
    if (typeof options.groupId !== 'string' || options.groupId === '') {
      throw new ConfigError('consumer groupId must be a non-empty string')
    }
    this.context = context
    this.options = options
    this.groupId = options.groupId
    this.maxProcessingTimeMs = parseDuration(options.maxProcessingTime ?? DEFAULT_MAX_PROCESSING_TIME_MS, 'maxProcessingTime')
    this.levels = resolveRetryLevels(options.retry?.levels ?? [], this.maxProcessingTimeMs)
    this.retryIf = options.retry?.retryIf ?? isRetryable
    this.retryNaming = options.retry?.topicNaming ?? defaultRetryTopicNaming
    this.dlqNaming = (options.dlq?.enabled ?? true) ? (options.dlq?.topicNaming ?? defaultDlqTopicNaming) : undefined
    const concurrency = options.concurrency ?? 1
    if (!Number.isInteger(concurrency) || concurrency < 1) {
      throw new ConfigError(`concurrency must be an integer >= 1; got ${String(concurrency)}`)
    }
    this.concurrency = concurrency
    this.ready = new Promise<void>((resolve) => {
      this.markReady = resolve
    })
  }

  get status (): ConsumerState {
    return this.state
  }

  /** Registers a handler for a topic. Its retry ladder is consumed as well. */
  subscribe<T = unknown> (topic: string, handler: Handler<T>, options: SubscribeOptions<T> = {}): this {
    if (this.state !== 'idle') throw new ConfigError('subscribe() must be called before start()')
    if (typeof topic !== 'string' || topic === '') throw new ConfigError('topic must be a non-empty string')
    if (typeof handler !== 'function') throw new ConfigError(`handler for "${topic}" must be a function`)
    const plan = new TopicPlan(topic, this.levels.length, this.retryNaming, this.dlqNaming)
    const subscription: Subscription = {
      plan,
      handler: handler as Handler,
      serializer: (options.serializer ?? this.options.serializer ?? this.context.serializer) as Serializer
    }
    for (const consumed of plan.consumedTopics) {
      const owner = this.routes.get(consumed)
      if (owner !== undefined) {
        throw new ConfigError(`topic "${consumed}" is claimed by both "${owner.subscription.plan.original}" and "${topic}"`)
      }
    }
    this.subscriptions.set(topic, subscription)
    this.routes.set(topic, { subscription, level: 0, delayMs: 0 })
    for (const { level, delayMs } of this.levels) {
      this.routes.set(plan.retryTopic(level) as string, { subscription, level, delayMs })
    }
    return this
  }

  /** Connects, prepares the topics and starts fetching. */
  async start (): Promise<void> {
    if (this.context.isClosed()) throw new ClosedError('harbor')
    if (this.state !== 'idle') throw new ConfigError(`start() called while the consumer is ${this.state}`)
    if (this.subscriptions.size === 0) throw new ConfigError('start() called with no subscription')
    this.state = 'starting'
    try {
      await this.context.ensureConnected()
      await this.prepareTopics()
      this.handle = await this.context.adapter.consume({
        groupId: this.groupId,
        topics: [...this.routes.keys()],
        concurrency: this.concurrency,
        fromBeginning: this.options.fromBeginning ?? false,
        eachMessage: (raw) => this.receive(raw),
        onError: (error) => this.context.emit('error', { error, scope: 'adapter', groupId: this.groupId })
      })
      // A stop that began while the client was joining the group has been
      // waiting for this handle; the consumer never becomes 'running', so no
      // delivery is processed, and start() reports it as closed.
      if (this.stopping === undefined) this.state = 'running'
      this.markReady()
    } catch (error) {
      this.state = 'stopped'
      this.markReady()
      throw error
    }
    if (this.stopping !== undefined) {
      await this.stopping.catch(() => undefined)
      throw new ClosedError('consumer')
    }
  }

  /**
   * Stops fetching, waits for in-flight handlers up to `timeoutMs`, then
   * leaves the group. Handlers that finish in time commit their own offsets
   * on the way out; the ones that do not are abandoned (their signal aborts)
   * and reported through `ShutdownTimeoutError`, so at-least-once stays
   * honest: those messages will be redelivered.
   */
  async stop (timeoutMs = 30_000): Promise<void> {
    if (this.stopping !== undefined) return await this.stopping
    this.stopping = this.doStop('shutdown', timeoutMs)
    return await this.stopping
  }

  /**
   * The pipeline decided to stop (abort or crash). Runs detached: the
   * message being processed is itself in flight, so awaiting the stop from
   * inside it would deadlock against an adapter that waits for deliveries
   * to settle before leaving the group.
   */
  private stopFromPipeline (reason: StopReason): void {
    if (this.stopping !== undefined) return
    this.stopping = this.doStop(reason, 0).catch((error: unknown) => {
      this.context.emit('error', { error, scope: 'consumer', groupId: this.groupId })
    })
  }

  private async doStop (reason: StopReason, timeoutMs: number): Promise<void> {
    if (this.state === 'starting') {
      // Wait for start() to obtain the handle (or to fail), so the group
      // membership it is creating is the one closed below.
      await this.ready
    }
    if (this.state === 'idle' || this.state === 'stopped') {
      this.state = 'stopped'
      return
    }
    this.state = 'stopping'
    // Deliveries that never reached their handler (waiting on a retry delay,
    // or queued behind readiness) unwind at once and are left uncommitted;
    // running handlers get the timeout.
    this.drainController.abort()
    const deadlineController = new AbortController()
    await Promise.race([
      Promise.all([...this.deliveries].filter(([, entry]) => entry.active).map(([work]) => work)),
      this.context.clock.sleep(timeoutMs, deadlineController.signal)
    ])
    // A timer nobody waits for anymore must not keep the process alive.
    deadlineController.abort()
    const abandoned = [...this.deliveries.values()].filter((entry) => entry.active).length
    if (abandoned > 0) this.shutdownController.abort()
    try {
      // The handle exists whenever the state got past 'starting'.
      await (this.handle as ConsumerHandle).stop()
    } finally {
      this.handle = undefined
      this.state = 'stopped'
      this.context.emit('consumerStopped', { groupId: this.groupId, reason })
    }
    if (abandoned > 0 && reason === 'shutdown') {
      const error = new ShutdownTimeoutError(abandoned, timeoutMs)
      this.context.logger.warn(`[kafka-harbor] group "${this.groupId}": ${error.message}`)
      throw error
    }
  }

  private async prepareTopics (): Promise<void> {
    const derived: TopicSpec[] = []
    for (const { plan } of this.subscriptions.values()) {
      for (const topic of plan.retryTopics) derived.push(this.topicSpec(topic))
      if (plan.dlqTopic !== undefined) derived.push(this.topicSpec(plan.dlqTopic))
    }
    if (derived.length === 0) return
    if (this.options.autoCreateTopics === true) {
      await this.context.adapter.admin.createTopics(derived)
      return
    }
    for (const spec of derived) {
      if (!await this.context.adapter.admin.topicExists(spec.topic)) throw new TopicMissingError(spec.topic)
    }
  }

  private topicSpec (topic: string): TopicSpec {
    return {
      topic,
      partitions: this.options.topicDefaults?.partitions ?? 1,
      replicationFactor: this.options.topicDefaults?.replicationFactor ?? 1
    }
  }

  /**
   * The adapter's entry point. Never rejects: every outcome is either a
   * commit or an explicit stop, and the adapter only learns that the
   * message is done.
   */
  private receive (raw: RawMessage): Promise<void> {
    // An adapter may deliver before start() has finished wiring the handle;
    // processing waits for that, so no message is handled without a way to
    // commit it. A delivery that lands while the consumer is stopping (or
    // stopped) is neither processed nor committed: the next member of the
    // group picks it up.
    const entry = { active: false }
    const work: Promise<void> = this.ready
      .then(() => this.process(raw, entry))
      .catch((error: unknown) => this.crash(error, raw))
    this.deliveries.set(work, entry)
    // Once shutdown abandons the handler, the adapter must not keep waiting
    // for it: the delivery is reported settled (uncommitted) and the handler
    // keeps running detached, its outcome ignored. One listener per delivery,
    // removed when it settles: a shared promise raced per message would keep
    // a reaction alive for every message ever received.
    const signal = this.shutdownController.signal
    return new Promise<void>((resolve) => {
      const onAbort = (): void => { resolve() }
      signal.addEventListener('abort', onAbort)
      work.finally(() => {
        this.deliveries.delete(work)
        signal.removeEventListener('abort', onAbort)
        resolve()
      }).catch(() => undefined)
    })
  }

  private async process (raw: RawMessage, entry: { active: boolean }): Promise<void> {
    const route = this.routes.get(raw.topic)
    if (route === undefined) {
      throw new ConfigError(`received a message on "${raw.topic}", a topic this consumer never subscribed to`)
    }
    const { subscription, level } = route
    const names = this.context.headerNames
    const headers = decodeHeaders(raw.headers)
    const retry = level > 0 ? readRetryInfo(headers, names) : undefined
    const attempt = (retry?.count ?? level) + 1
    const correlationId = headers[names.correlationId]

    if (!await this.waitUntilDue(raw, route.delayMs)) return
    entry.active = true

    const startedAt = this.context.clock.now()
    let error: unknown
    try {
      const message = this.toMessage(raw, headers, subscription, retry)
      await subscription.handler(message, {
        correlationId,
        logger: this.context.logger,
        signal: this.shutdownController.signal,
        attempt
      })
    } catch (thrown) {
      error = thrown
    }
    const durationMs = this.context.clock.now() - startedAt
    const at = { topic: raw.topic, partition: raw.partition, offset: raw.offset, groupId: this.groupId, correlationId }

    if (this.shutdownController.signal.aborted) {
      // The handler was abandoned by shutdown; whatever it returned or threw
      // is no longer a verdict on the message. No commit, no retry: the
      // message is redelivered to the next member.
      return
    }
    if (error === undefined) {
      if (await this.commit(raw)) this.context.emit('messageProcessed', { ...at, durationMs })
      return
    }
    if (isAbortProcessingError(error)) {
      this.context.emit('messageFailed', { ...at, error, durationMs, outcome: 'abort' })
      this.context.emit('error', { error, scope: 'consumer', groupId: this.groupId, topic: raw.topic })
      this.stopFromPipeline('abort')
      return
    }

    const nextLevel = level + 1
    const retryTopic = subscription.plan.retryTopic(nextLevel)
    // A message that cannot be decoded will not decode next time either.
    const wantsRetry = !isSerializationError(error) && this.retryIf(error)
    const tracking = writeRetryInfo(headers, names, {
      previous: retry,
      originalTopic: subscription.plan.original,
      error: describeError(error),
      now: new Date(this.context.clock.now())
    })

    if (wantsRetry && retryTopic !== undefined) {
      await this.forward(raw, retryTopic, tracking)
      if (!await this.commit(raw)) return
      this.context.emit('messageFailed', { ...at, error, durationMs, outcome: 'retry' })
      this.context.emit('messageRetried', { ...at, retryTopic, level: nextLevel, attempt, error })
      return
    }
    if (subscription.plan.dlqTopic !== undefined) {
      const dlqTopic = subscription.plan.dlqTopic
      tracking[names.deadLetteredAt] = new Date(this.context.clock.now()).toISOString()
      await this.forward(raw, dlqTopic, tracking)
      if (!await this.commit(raw)) return
      this.context.emit('messageFailed', { ...at, error, durationMs, outcome: 'dead-letter' })
      this.context.emit('messageDeadLettered', { ...at, dlqTopic, attempts: attempt, error })
      return
    }
    // No retry level left and no DLQ: the only honest outcome is to stop
    // without committing, so nothing is lost and someone has to look.
    this.context.emit('messageFailed', { ...at, error, durationMs, outcome: 'crash' })
    throw error
  }

  /**
   * A retry topic message becomes due `delay` after it was produced. The
   * wait is bounded by construction (every delay fits under
   * maxProcessingTime) and ends early on shutdown, in which case the message
   * is left uncommitted for the next member.
   */
  private async waitUntilDue (raw: RawMessage, delayMs: number): Promise<boolean> {
    const wait = raw.timestamp + delayMs - this.context.clock.now()
    if (wait > 0) await this.context.clock.sleep(wait, this.drainController.signal)
    // A stop aborts the drain signal and leaves 'running' in the same breath.
    return this.state === 'running'
  }

  private toMessage (raw: RawMessage, headers: MessageHeaders, subscription: Subscription, retry: Message['retry']): Message {
    const original = subscription.plan.original
    return {
      topic: raw.topic,
      partition: raw.partition,
      offset: raw.offset,
      key: raw.key === null ? null : raw.key.toString('utf8'),
      value: raw.value === null ? null : subscription.serializer.deserialize(raw.value, original),
      headers,
      timestamp: new Date(raw.timestamp),
      ...(retry !== undefined && { retry })
    }
  }

  /**
   * Re-produces the ORIGINAL bytes (key and value untouched) to the next
   * topic with the tracking headers, and resolves only after the broker
   * acknowledged. The offset is committed after this resolves, never
   * before: a produce that fails leaves the message where it is.
   */
  private async forward (raw: RawMessage, topic: string, headers: MessageHeaders): Promise<void> {
    const names = this.context.headerNames
    const record: RawRecord = {
      topic,
      key: raw.key,
      value: raw.value,
      headers: {
        ...headers,
        [names.producedAt]: new Date(this.context.clock.now()).toISOString(),
        [names.producer]: this.context.clientId
      }
    }
    await this.context.producePolicy.execute(() => this.context.adapter.produce([record]))
  }

  /**
   * Commits the offset after this message. A commit that fails (a rebalance
   * in progress, a coordinator timeout) is reported through `error` and the
   * consumer carries on: the work behind it is safe (handler done, or the
   * retry/DLQ produce acknowledged), and the only consequence of the missing
   * commit is a redelivery, which at-least-once already allows. Returns
   * whether the offset was committed.
   */
  private async commit (raw: RawMessage): Promise<boolean> {
    if (this.handle === undefined) return false
    try {
      await this.handle.commit([{ topic: raw.topic, partition: raw.partition, offset: (BigInt(raw.offset) + 1n).toString() }])
      return true
    } catch (error) {
      this.context.logger.warn(`[kafka-harbor] commit failed for ${raw.topic}[${raw.partition}]@${raw.offset}; the message will be redelivered: ${describeError(error)}`)
      this.context.emit('error', { error, scope: 'consumer', groupId: this.groupId, topic: raw.topic })
      return false
    }
  }

  /**
   * The pipeline itself failed (the retry or DLQ produce did not get an
   * acknowledgment, the commit failed, a topic is missing): the message
   * stays uncommitted and the consumer stops. Silence is the one outcome
   * that is not allowed.
   */
  private async crash (error: unknown, raw: RawMessage): Promise<void> {
    this.context.logger.error(`[kafka-harbor] consumer "${this.groupId}" stopped on ${raw.topic}[${raw.partition}]@${raw.offset}: ${describeError(error)}`)
    this.context.emit('error', { error, scope: 'consumer', groupId: this.groupId, topic: raw.topic })
    this.stopFromPipeline('crash')
  }
}

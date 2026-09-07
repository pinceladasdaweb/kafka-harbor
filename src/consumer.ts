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
import { toMessage } from './message'
import { commitAfter } from './commit'
import { parseDuration } from './duration'
import type { Serializer } from './serializer'
import type { CoreContext, HarborErrorEvent } from './context'
import type { Duration, MessageHeaders, Logger, Message } from './types'
import { decodeHeaders, readRetryInfo, stampProducer, writeRetryInfo } from './headers'
import { firstRejection, requireNonEmptyString, requirePositiveInteger } from './validate'
import type { ConsumerHandle, RawMessage, RawRecord, TopicPartition, TopicSpec } from './adapter'

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

export interface ConsumerEvents {
  messageProcessed: { topic: string, partition: number, offset: string, groupId: string, durationMs: number, correlationId: string | undefined }
  messageRetried: { topic: string, partition: number, offset: string, groupId: string, retryTopic: string, level: number, attempt: number, error: unknown, correlationId: string | undefined }
  messageDeadLettered: { topic: string, partition: number, offset: string, groupId: string, dlqTopic: string, attempts: number, error: unknown, correlationId: string | undefined }
  messageFailed: { topic: string, partition: number, offset: string, groupId: string, error: unknown, durationMs: number, outcome: FailureOutcome, correlationId: string | undefined }
  consumerStopped: { groupId: string, reason: StopReason }
  error: HarborErrorEvent
}

export type FailureOutcome = 'retry' | 'dead-letter' | 'abort' | 'crash'
export type StopReason = 'shutdown' | 'abort' | 'crash'
export type ConsumerState = 'idle' | 'starting' | 'running' | 'stopping' | 'stopped'

/** What the core needs from the harbor to build a consumer. */
export type ConsumerContext = CoreContext<ConsumerEvents>

interface Subscription {
  readonly plan: TopicPlan
  readonly handler: Handler
  readonly serializer: Serializer
}

/**
 * One consumed topic: its place on the ladder and the subscriptions it
 * serves. An original topic serves exactly one. A retry topic serves one per
 * subscription whose naming produced it, so a naming function that returns
 * the same name for every original topic of a level (`svc-retry-1`) makes
 * that topic shared: the message's original-topic header says which handler
 * it belongs to.
 */
interface Route {
  readonly owners: Map<string, Subscription>
  /** 0 for the original topic, 1..N for a retry topic. */
  readonly level: number
  /** How long a message waits on this topic before the handler runs. */
  readonly delayMs: number
}

/**
 * A delivery the adapter handed over and has not been told is done. The
 * handler has started once `active` is set; `release` tells the adapter the
 * delivery settled, which shutdown does early for the handlers it abandons.
 */
interface Delivery {
  readonly raw: RawMessage
  active: boolean
  readonly release: () => void
}

const DEFAULT_MAX_PROCESSING_TIME_MS = 300_000
const DEFAULT_STOP_TIMEOUT_MS = 30_000

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
  private readonly routes = new Map<string, Route>()
  /**
   * Every delivery in flight. A message waiting for its retry delay is a
   * delivery, not a running handler: shutdown cuts the wait short at once
   * and only ever waits for handlers.
   */
  private readonly deliveries = new Map<Promise<void>, Delivery>()
  /** Aborts as soon as a stop begins: wakes retry waits, blocks new handlers. */
  private readonly drainController = new AbortController()
  /** Aborts when shutdown gives up on running handlers (the handler's `signal`). */
  private readonly shutdownController = new AbortController()

  /**
   * The running consumptions, one per ladder level, keyed by every topic
   * they consume. Each level is a group member of its own: a message
   * sleeping out its retry delay occupies a worker of that level's client,
   * never one the original topic (or another level) is waiting for.
   */
  private readonly handles = new Map<string, ConsumerHandle>()
  private state: ConsumerState = 'idle'
  private stopReason: StopReason | undefined
  private stopping: Promise<void> | undefined
  /** Set when a stop gave up waiting for start(): whatever start() reports from then on is the closure. */
  private startAbandoned = false
  private readonly ready: Promise<void>
  private markReady!: () => void

  constructor (context: ConsumerContext, options: ConsumerOptions) {
    this.groupId = requireNonEmptyString(options.groupId, 'consumer groupId')
    this.context = context
    this.options = options
    this.maxProcessingTimeMs = parseDuration(options.maxProcessingTime ?? DEFAULT_MAX_PROCESSING_TIME_MS, 'maxProcessingTime')
    this.levels = resolveRetryLevels(options.retry?.levels ?? [], this.maxProcessingTimeMs)
    this.retryIf = options.retry?.retryIf ?? isRetryable
    this.retryNaming = options.retry?.topicNaming ?? defaultRetryTopicNaming
    this.dlqNaming = (options.dlq?.enabled ?? true) ? (options.dlq?.topicNaming ?? defaultDlqTopicNaming) : undefined
    this.concurrency = requirePositiveInteger(options.concurrency ?? 1, 'concurrency')
    this.ready = new Promise<void>((resolve) => {
      this.markReady = resolve
    })
  }

  get status (): ConsumerState {
    return this.state
  }

  /** Why the consumer stopped, once it did. */
  get stoppedBecause (): StopReason | undefined {
    return this.stopReason
  }

  /** Registers a handler for a topic. Its retry ladder is consumed as well. */
  subscribe<T = unknown> (topic: string, handler: Handler<T>, options: SubscribeOptions<T> = {}): this {
    if (this.state !== 'idle') throw new ConfigError('subscribe() must be called before start()')
    requireNonEmptyString(topic, 'topic')
    if (typeof handler !== 'function') throw new ConfigError(`handler for "${topic}" must be a function`)
    const plan = new TopicPlan(topic, this.levels.length, this.retryNaming, this.dlqNaming)
    const subscription: Subscription = {
      plan,
      handler: handler as Handler,
      serializer: (options.serializer ?? this.options.serializer ?? this.context.serializer) as Serializer
    }
    // An original topic belongs to one subscription. A retry topic may be
    // shared, but only among topics of the same level: the level decides the
    // delay, and a message must not wait one topic's delay on another's
    // ladder. A DLQ is never consumed by the consumer that fills it.
    const taken = this.routes.get(topic)
    if (taken !== undefined) {
      throw new ConfigError(`topic "${topic}" is claimed by both "${[...taken.owners.keys()].join('", "')}" and "${topic}"`)
    }
    for (const { level } of this.levels) {
      const retryTopic = plan.retryTopic(level) as string
      const existing = this.routes.get(retryTopic)
      if (existing !== undefined && existing.level !== level) {
        throw new ConfigError(`topic "${retryTopic}" is level ${level} of "${topic}" but ${existing.level === 0 ? 'the original topic' : `level ${existing.level}`} of "${[...existing.owners.keys()].join('", "')}"`)
      }
    }
    if (plan.dlqTopic !== undefined && this.routes.has(plan.dlqTopic)) {
      throw new ConfigError(`dlq.topicNaming produced "${plan.dlqTopic}" for "${topic}", a topic this consumer already consumes`)
    }
    for (const route of this.routes.values()) {
      for (const owner of route.owners.values()) {
        if (owner.plan.dlqTopic !== undefined && plan.consumedTopics.includes(owner.plan.dlqTopic)) {
          throw new ConfigError(`topic "${owner.plan.dlqTopic}" is the DLQ of "${owner.plan.original}" and would be consumed through "${topic}"`)
        }
      }
    }
    this.routes.set(topic, { owners: new Map([[topic, subscription]]), level: 0, delayMs: 0 })
    for (const { level, delayMs } of this.levels) {
      const retryTopic = plan.retryTopic(level) as string
      const route = this.routes.get(retryTopic) ?? { owners: new Map<string, Subscription>(), level, delayMs }
      route.owners.set(topic, subscription)
      this.routes.set(retryTopic, route)
    }
    return this
  }

  /** Connects, prepares the topics and starts fetching. */
  async start (): Promise<void> {
    if (this.context.isClosed()) throw new ClosedError('harbor')
    if (this.state !== 'idle') throw new ConfigError(`start() called while the consumer is ${this.state}`)
    if (this.routes.size === 0) throw new ConfigError('start() called with no subscription')
    this.state = 'starting'
    try {
      await this.context.ensureConnected()
      this.assertNotStopping()
      await this.prepareTopics()
      this.assertNotStopping()
      await this.join()
    } catch (error) {
      this.state = 'stopped'
      this.markReady()
      // A start that fails after the stop gave up on it failed because the
      // client is gone (or about to be); the caller asked for a closed
      // consumer and gets told so.
      if (this.startAbandoned) throw new ClosedError('consumer')
      throw error
    }
    if (this.stopping !== undefined) {
      // A stop began while the client was joining the group. Either it is
      // waiting for these handles, or it gave up at its deadline; in both
      // cases the memberships are closed and no delivery is processed.
      this.markReady()
      await this.stopping.catch(() => undefined)
      await this.leave()
      throw new ClosedError('consumer')
    }
    this.state = 'running'
    this.markReady()
  }

  /** A stop that began during start() wins: the start does not go further. */
  private assertNotStopping (): void {
    if (this.stopping !== undefined) throw new ClosedError('consumer')
  }

  /**
   * The topics of one ladder level, for every subscription, form one
   * consumption. Level 0 is the original topics; level N the N-th retry
   * topics. Kafka assigns each topic among the members subscribed to it, so
   * the members of one group may subscribe to different topics.
   */
  private consumptions (): string[][] {
    const byLevel = new Map<number, string[]>()
    for (const [topic, route] of this.routes) {
      const topics = byLevel.get(route.level) ?? []
      topics.push(topic)
      byLevel.set(route.level, topics)
    }
    return [...byLevel.values()]
  }

  /** Opens every consumption; if one fails, the ones that opened are closed again. */
  private async join (): Promise<void> {
    const results = await Promise.allSettled(this.consumptions().map(async (topics) => {
      const handle = await this.context.adapter.consume({
        groupId: this.groupId,
        topics,
        concurrency: this.concurrency,
        fromBeginning: this.options.fromBeginning ?? false,
        maxProcessingTimeMs: this.maxProcessingTimeMs,
        eachMessage: (raw) => this.receive(raw),
        onPartitionsRevoked: (partitions) => this.settleRevoked(partitions),
        onError: (error) => this.context.emit('error', { error, scope: 'adapter', groupId: this.groupId })
      })
      return { topics, handle }
    }))
    const failed = firstRejection(results)
    const opened = results.filter((result): result is PromiseFulfilledResult<{ topics: string[], handle: ConsumerHandle }> => result.status === 'fulfilled')
    if (failed !== undefined) {
      await Promise.allSettled(opened.map((result) => result.value.handle.stop()))
      throw failed.reason
    }
    for (const { value } of opened) {
      for (const topic of value.topics) this.handles.set(topic, value.handle)
    }
  }

  /** Leaves the group on every consumption; the first failure is rethrown once all were tried. */
  private async leave (): Promise<void> {
    const handles = [...new Set(this.handles.values())]
    this.handles.clear()
    const failed = firstRejection(await Promise.allSettled(handles.map((handle) => handle.stop())))
    if (failed !== undefined) throw failed.reason
  }

  /**
   * A rebalance is taking these partitions away. Their running handlers get
   * to finish and commit before the adapter releases them, so the next owner
   * starts after that work instead of repeating it. Bounded by
   * maxProcessingTime, the longest a handler may take anyway.
   */
  private async settleRevoked (partitions: readonly TopicPartition[]): Promise<void> {
    const revoked = new Set(partitions.map(({ topic, partition }) => `${topic}\u0000${partition}`))
    const running = [...this.deliveries].filter(([, entry]) => entry.active && revoked.has(`${entry.raw.topic}\u0000${entry.raw.partition}`))
    await this.settledInTime(Promise.all(running.map(([work]) => work)), this.maxProcessingTimeMs)
  }

  /**
   * Stops fetching, waits for in-flight handlers up to `timeoutMs`, then
   * leaves the group. Handlers that finish in time commit their own offsets
   * on the way out; the ones that do not are abandoned (their signal aborts)
   * and reported through `ShutdownTimeoutError`, so at-least-once stays
   * honest: those messages will be redelivered.
   */
  async stop (timeoutMs = DEFAULT_STOP_TIMEOUT_MS): Promise<void> {
    if (this.stopping !== undefined) return await this.stopping
    this.stopping = this.doStop('shutdown', timeoutMs)
    return await this.stopping
  }

  /**
   * The pipeline decided to stop (abort or crash). Runs detached: the
   * message being processed is itself in flight, so awaiting the stop from
   * inside it would deadlock against an adapter that waits for deliveries
   * to settle before leaving the group. Handlers running on other partitions
   * get the same grace a shutdown gives them: their work is not the reason
   * the consumer stops, and finishing lets them commit instead of being
   * redelivered as duplicates.
   */
  private stopFromPipeline (reason: StopReason): void {
    if (this.stopping !== undefined) return
    this.stopping = this.doStop(reason, DEFAULT_STOP_TIMEOUT_MS).catch((error: unknown) => {
      this.context.emit('error', { error, scope: 'consumer', groupId: this.groupId })
    })
  }

  private async doStop (reason: StopReason, timeoutMs: number): Promise<void> {
    const deadline = this.context.clock.now() + timeoutMs
    if (this.state === 'starting') {
      // Wait for start() to obtain its handles (or to fail), so the group
      // memberships it is creating are the ones closed below. The wait is
      // bounded by the same timeout: a start stuck on an unreachable broker
      // must not hold a shutdown hostage. Nothing runs and nothing is lost
      // when it is given up on; start() closes whatever it still opens.
      if (!await this.settledInTime(this.ready, timeoutMs)) {
        this.context.logger.warn(`[kafka-harbor] group "${this.groupId}": start() did not finish within ${timeoutMs}ms; stopping without waiting for it`)
        this.startAbandoned = true
        this.state = 'stopped'
        this.stopReason = reason
        this.context.emit('consumerStopped', { groupId: this.groupId, reason })
        return
      }
    }
    if (this.state === 'idle' || this.state === 'stopped') {
      this.state = 'stopped'
      return
    }
    this.state = 'stopping'
    // Known from the moment the stop begins, so a health probe sees a crash
    // or an abort while the client is still being released.
    this.stopReason = reason
    // Deliveries that never reached their handler (waiting on a retry delay,
    // or queued behind readiness) unwind at once and are left uncommitted;
    // running handlers get what is left of the timeout.
    this.drainController.abort()
    await this.settledInTime(
      Promise.all([...this.deliveries].filter(([, entry]) => entry.active).map(([work]) => work)),
      Math.max(0, deadline - this.context.clock.now())
    )
    // Once shutdown abandons a handler, the adapter must not keep waiting
    // for it: the delivery is reported settled (uncommitted) and the handler
    // keeps running detached, its outcome ignored.
    const abandoned = [...this.deliveries.values()].filter((entry) => entry.active)
    if (abandoned.length > 0) {
      this.shutdownController.abort()
      for (const entry of abandoned) entry.release()
    }
    try {
      await this.leave()
    } finally {
      this.state = 'stopped'
      this.context.emit('consumerStopped', { groupId: this.groupId, reason })
    }
    if (abandoned.length > 0 && reason === 'shutdown') {
      const error = new ShutdownTimeoutError(abandoned.length, timeoutMs)
      this.context.logger.warn(`[kafka-harbor] group "${this.groupId}": ${error.message}`)
      throw error
    }
  }

  /**
   * Whether `promise` settles within `timeoutMs`. The timer is cancelled as
   * soon as the race is decided: a timer nobody waits for anymore must not
   * keep the process alive.
   */
  private async settledInTime (promise: Promise<unknown>, timeoutMs: number): Promise<boolean> {
    const deadlineController = new AbortController()
    // The sleep resolves to nothing, so only the promise settling yields true.
    const settled = await Promise.race([
      Promise.allSettled([promise]).then(() => true),
      this.context.clock.sleep(timeoutMs, deadlineController.signal)
    ])
    deadlineController.abort()
    return settled === true
  }

  private async prepareTopics (): Promise<void> {
    // A shared retry topic or DLQ appears in several plans and is created once.
    const names = new Set<string>()
    for (const route of this.routes.values()) {
      for (const { plan } of route.owners.values()) {
        for (const topic of plan.retryTopics) names.add(topic)
        if (plan.dlqTopic !== undefined) names.add(plan.dlqTopic)
      }
    }
    const derived = [...names].map((topic) => this.topicSpec(topic))
    if (derived.length === 0) return
    if (this.options.autoCreateTopics === true) {
      await this.context.adapter.admin.createTopics(derived)
      return
    }
    // One round-trip per topic, all at once rather than one after the other.
    const present = await Promise.all(derived.map(async (spec) => await this.context.adapter.admin.topicExists(spec.topic)))
    const missing = derived.find((_spec, index) => present[index] === false)
    if (missing !== undefined) throw new TopicMissingError(missing.topic)
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
    return new Promise<void>((resolve) => {
      const entry: Delivery = { raw, active: false, release: resolve }
      // An adapter may deliver before start() has finished wiring the
      // handles; processing waits for that, so no message is handled without
      // a way to commit it. A delivery that lands while the consumer is
      // stopping (or stopped) is neither processed nor committed: the next
      // member of the group picks it up.
      const work: Promise<void> = (this.state === 'running' ? this.process(raw, entry) : this.ready.then(() => this.process(raw, entry)))
        .catch((error: unknown) => this.crash(error, raw))
      this.deliveries.set(work, entry)
      work.finally(() => {
        this.deliveries.delete(work)
        resolve()
      }).catch(() => undefined)
    })
  }

  private async process (raw: RawMessage, entry: Delivery): Promise<void> {
    const route = this.routes.get(raw.topic)
    if (route === undefined) {
      throw new ConfigError(`received a message on "${raw.topic}", a topic this consumer never subscribed to`)
    }
    const { level } = route
    const names = this.context.headerNames
    const headers = decodeHeaders(raw.headers)
    const retry = level > 0 ? readRetryInfo(headers, names) : undefined
    const attempt = (retry?.count ?? level) + 1
    const correlationId = headers[names.correlationId]
    const subscription = this.ownerOf(route, retry?.originalTopic)
    if (subscription === undefined) {
      await this.deadLetterUnroutable(raw, route, headers, retry, correlationId)
      return
    }

    // A retry topic message becomes due `delay` after it was produced. The
    // wait is bounded by construction (every delay fits under
    // maxProcessingTime) and ends early on shutdown, in which case the
    // message is left uncommitted for the next member. It is capped at the
    // level's delay: the timestamp comes from another process's clock, and
    // one ahead of ours would otherwise stretch the wait by the skew. With a
    // delay of zero, which is what the original topic has, nothing sleeps.
    const wait = Math.min(route.delayMs, raw.timestamp + route.delayMs - this.context.clock.now())
    if (wait > 0) await this.context.clock.sleep(wait, this.drainController.signal)
    // The check and the flag are one synchronous step: a stop that begins
    // in between would snapshot the handler as not running and then find it
    // running. A stop aborts the drain signal and leaves 'running' together.
    if (this.state !== 'running') return
    entry.active = true

    const startedAt = this.context.clock.now()
    let error: unknown
    try {
      const message = toMessage(raw, headers, subscription.serializer, subscription.plan.original, retry)
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
   * The subscription a message on this topic belongs to. An original topic
   * and an unshared retry topic have one owner, whatever the headers say (a
   * corrupt tracking block there means a first delivery, not a lost
   * message). A shared retry topic has to trust the original-topic header,
   * validated as network input; a message it cannot place has no owner.
   */
  private ownerOf (route: Route, originalTopic: string | undefined): Subscription | undefined {
    if (route.owners.size === 1) return route.owners.values().next().value
    // No tracking block, no original topic: the empty name is never a key.
    return route.owners.get(originalTopic ?? '')
  }

  /**
   * A message on a shared retry topic whose original-topic header names no
   * subscription of this consumer cannot be handled by anyone here. When the
   * owners of the topic share one DLQ it goes there, tracking headers as they
   * are, so someone can look; when they do not, there is no honest
   * destination and the consumer stops with the offset uncommitted.
   */
  private async deadLetterUnroutable (raw: RawMessage, route: Route, headers: MessageHeaders, retry: Message['retry'], correlationId: string | undefined): Promise<void> {
    const names = this.context.headerNames
    const claimed = retry?.originalTopic ?? headers[names.originalTopic]
    const error = new ConfigError(`message on shared retry topic "${raw.topic}" names ${claimed === undefined ? 'no original topic' : `"${claimed}" as its original topic`}, which this consumer does not subscribe to`)
    const dlqTopics = new Set([...route.owners.values()].map(({ plan }) => plan.dlqTopic))
    const [dlqTopic] = dlqTopics
    if (dlqTopics.size !== 1 || dlqTopic === undefined) throw error
    const at = { topic: raw.topic, partition: raw.partition, offset: raw.offset, groupId: this.groupId, correlationId }
    const tracking: MessageHeaders = { ...headers, [names.lastError]: describeError(error), [names.deadLetteredAt]: new Date(this.context.clock.now()).toISOString() }
    await this.forward(raw, dlqTopic, tracking)
    if (!await this.commit(raw)) return
    this.context.emit('messageFailed', { ...at, error, durationMs: 0, outcome: 'dead-letter' })
    this.context.emit('messageDeadLettered', { ...at, dlqTopic, attempts: (retry?.count ?? 0) + 1, error })
  }

  /**
   * Re-produces the ORIGINAL bytes (key and value untouched) to the next
   * topic with the tracking headers, and resolves only after the broker
   * acknowledged. The offset is committed after this resolves, never
   * before: a produce that fails leaves the message where it is.
   */
  private async forward (raw: RawMessage, topic: string, headers: MessageHeaders): Promise<void> {
    const record: RawRecord = {
      topic,
      key: raw.key,
      value: raw.value,
      headers: stampProducer(headers, this.context.headerNames, {
        clientId: this.context.clientId,
        at: new Date(this.context.clock.now()),
        correlationId: this.context.correlationId
      })
    }
    await this.context.producePolicy.execute(() => this.context.adapter.produce([record]))
  }

  /** Commits the offset after this message; see `commitAfter` for what a failure means. */
  private async commit (raw: RawMessage): Promise<boolean> {
    // Every consumed topic has its handle while a handler can reach this
    // point: the handles are released only after running handlers were
    // waited for or abandoned, and an abandoned handler never commits.
    return await commitAfter(this.handles.get(raw.topic) as ConsumerHandle, raw, {
      groupId: this.groupId,
      logger: this.context.logger,
      emit: (event, payload) => this.context.emit(event, payload)
    })
  }

  /**
   * The pipeline itself failed (the retry or DLQ produce did not get an
   * acknowledgment, a topic is missing, a message arrived on a topic nobody
   * subscribed to): the message stays uncommitted and the consumer stops.
   * Silence is the one outcome that is not allowed.
   */
  private async crash (error: unknown, raw: RawMessage): Promise<void> {
    this.context.logger.error(`[kafka-harbor] consumer "${this.groupId}" stopped on ${raw.topic}[${raw.partition}]@${raw.offset}: ${describeError(error)}`)
    this.context.emit('error', { error, scope: 'consumer', groupId: this.groupId, topic: raw.topic })
    this.stopFromPipeline('crash')
  }
}

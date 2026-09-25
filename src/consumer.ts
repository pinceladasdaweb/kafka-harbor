import {
  AdapterError,
  ClosedError,
  ConfigError,
  ShutdownTimeoutError,
  TopicMissingError,
  HoldExpiredError,
  describeError,
  isAbortProcessingError,
  isBatchFailedError,
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
import { parseDuration } from './duration'
import { wrapped } from './instrumentation'
import type { Serializer } from './serializer'
import { partitionKey } from './topic-partition'
import { produceHop, type ProduceEvents } from './produce'
import type { CoreContext, HarborErrorEvent } from './context'
import { OFFSET_PATTERN, commitAfter, offsetDistance } from './commit'
import type { Duration, MessageHeaders, Logger, Message } from './types'
import { decodeHeaders, readRetryInfo, writeRetryInfo } from './headers'
import { defaultIdempotencyKey, type IdempotencyOptions } from './idempotency'
import type { BreakerState } from 'breakwater'

import { isCircuitRejection, resolveBreaker, type Breaker, type CircuitStateChange, type ConsumerBreakerOptions } from './breaker'
import type { ConsumerHandle, RawMessage, TopicPartition, TopicSpec } from './adapter'
import { firstRejection, requireNonEmptyString, requirePositiveInteger } from './validate'

/** What a handler receives besides the message. */
export interface HandlerContext {
  /** The consumer group the handler runs in. */
  readonly groupId: string
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

/** What a batch handler receives besides the messages. */
export interface BatchContext {
  /** The consumer group the handler runs in. */
  readonly groupId: string
  /** The topic and partition every message of the batch came from. */
  readonly topic: string
  readonly partition: number
  readonly logger: Logger
  /** Aborts when the harbor gave up waiting for this handler during shutdown; see `HandlerContext.signal`. */
  readonly signal: AbortSignal
}

/**
 * Handles one batch: consecutive messages of one partition, in offset
 * order. Resolving commits the offset after the last message; throwing
 * sends every message of the batch down the retry ladder with that error,
 * and throwing a `BatchFailedError` sends only the messages it names. Each
 * message keeps its own `retry` information, so a batch on a retry topic
 * may mix attempts.
 */
export type BatchHandler<T = unknown> = (messages: Array<Message<T>>, context: BatchContext) => Promise<void> | void

export interface SubscribeOptions<T = unknown> {
  /** Overrides the consumer's serializer for this topic. */
  serializer?: Serializer<T>
  /** Overrides the consumer's idempotency for this topic; the key function sees the topic's message type. */
  idempotency?: IdempotencyOptions<T>
  /** Overrides the consumer's circuit breaker for this topic; `false` runs this topic without one. */
  breaker?: ConsumerBreakerOptions | false
}

export interface SubscribeBatchOptions<T = unknown> {
  /** Overrides the consumer's serializer for this topic. */
  serializer?: Serializer<T>
  /** Overrides the consumer's circuit breaker for this topic; `false` runs this topic without one. */
  breaker?: ConsumerBreakerOptions | false
  /** The most messages a batch carries. Default: 100. */
  size?: number
  /** How long a partial batch waits for more messages before it runs, counted from its first message. Default: '1s'. */
  maxWait?: Duration
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
   * Runs every handler of this consumer through an idempotency engine: one
   * run per key, a repeat replays the first outcome and is committed without
   * running the handler again. See `IdempotencyOptions` for the key.
   */
  idempotency?: IdempotencyOptions
  /**
   * A circuit breaker per topic, in front of the handler: once the failures
   * cross the threshold the partition is held, nothing committed and
   * nothing sent down the ladder, until the breaker lets a probe through
   * or `hold` runs out. See `ConsumerBreakerOptions`.
   */
  breaker?: ConsumerBreakerOptions
  /**
   * The longest a message may sit in the pipeline (retry delay included)
   * before the group would consider the consumer dead. Default: '5m',
   * Kafka's `max.poll.interval.ms`. Every retry delay must fit under it.
   */
  maxProcessingTime?: Duration
}

export interface ConsumerEvents extends ProduceEvents {
  /** A message committed as processed. From a batch, `durationMs` is the batch's and `batch` its size; see `batchProcessed` for the batch itself. */
  messageProcessed: { topic: string, partition: number, offset: string, groupId: string, durationMs: number, correlationId: string | undefined, replayed: boolean, batch?: number }
  messageRetried: { topic: string, partition: number, offset: string, groupId: string, retryTopic: string, level: number, attempt: number, error: unknown, correlationId: string | undefined }
  messageDeadLettered: { topic: string, partition: number, offset: string, groupId: string, dlqTopic: string, attempts: number, error: unknown, correlationId: string | undefined }
  messageFailed: { topic: string, partition: number, offset: string, groupId: string, error: unknown, durationMs: number, outcome: FailureOutcome, correlationId: string | undefined, batch?: number }
  /** One batch handler run that ended: `outcome` is `processed` when the handler resolved (messages it named as failed notwithstanding), the failure outcome otherwise. */
  batchProcessed: { topic: string, partition: number, groupId: string, size: number, durationMs: number, outcome: 'processed' | FailureOutcome }
  consumerStopped: { groupId: string, reason: StopReason }
  /** The circuit breaker of a topic changed state; `open` means the partitions of that topic are being held. */
  circuitStateChanged: { groupId: string, topic: string, from: BreakerState, to: BreakerState }
  error: HarborErrorEvent
}

export type FailureOutcome = 'retry' | 'dead-letter' | 'abort' | 'crash'

/** How far behind a consumer group is on one partition it consumes. */
export interface PartitionLag {
  readonly groupId: string
  readonly topic: string
  readonly partition: number
  /** The first offset still held on the partition. */
  readonly low: string
  /** The high watermark: the offset the next record produced gets. */
  readonly high: string
  /** What the group committed, or null when it never committed on this partition. */
  readonly committed: string | null
  /**
   * Records between the group's position and the high watermark. The
   * position is the committed offset while there is one inside the
   * partition's range; until the first commit, or when the committed offset
   * fell out of range (the records expired, the topic was recreated), it is
   * where the group would start: the first offset still held with
   * `fromBeginning`, the high watermark otherwise.
   */
  readonly lag: number
}
export type StopReason = 'shutdown' | 'abort' | 'crash'
export type ConsumerState = 'idle' | 'starting' | 'running' | 'stopping' | 'stopped'

/** What the core needs from the harbor to build a consumer. */
export type ConsumerContext = CoreContext<ConsumerEvents>

interface BatchRun {
  readonly kind: 'batch'
  readonly handler: BatchHandler
  readonly size: number
  readonly maxWaitMs: number
}

interface Subscription {
  readonly plan: TopicPlan
  readonly serializer: Serializer
  readonly idempotency: IdempotencyOptions | undefined
  readonly breaker: Breaker | undefined
  readonly run: { readonly kind: 'each', readonly handler: Handler } | BatchRun
}

/** A delivery decoded as far as the pipeline needs before a handler runs. */
interface Pending {
  readonly raw: RawMessage
  readonly headers: MessageHeaders
  readonly retry: Message['retry']
  readonly attempt: number
  readonly correlationId: string | undefined
  /** When the adapter delivered it: what a hold is counted from. */
  readonly receivedAt: number
}

/** Thrown out of a hold when the consumer began stopping or the partition is being taken away: the message is left uncommitted, not failed. */
const HELD_UNTIL_RELEASED = Symbol('held until released')

/** How often a held delivery asks the breaker again while the circuit is open. */
const HOLD_POLL_MS = 1_000

/**
 * The messages of one partition waiting to run as a batch. The adapter has
 * been told all but the last of them are done (the partition would not
 * deliver the next one otherwise); none is committed until the batch ran.
 */
interface Batch {
  readonly subscription: Subscription
  readonly run: BatchRun
  readonly level: number
  readonly items: Pending[]
  /** Cancels the maxWait timer. */
  readonly timer: AbortController
}

/** What happened to a message the handler failed on, once it was forwarded. */
type Verdict =
  | { readonly outcome: 'retry', readonly retryTopic: string, readonly level: number }
  | { readonly outcome: 'dead-letter', readonly dlqTopic: string }
  | { readonly outcome: 'crash' }

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
  /** The breakers of the guarded topics, with the listener that reports their state changes, released on stop. */
  private readonly breakers: Array<{ breaker: Breaker, listener: (change: CircuitStateChange) => void }> = []
  /** The batch being collected on each partition of a batch subscription, keyed by `partitionKey`. */
  private readonly batches = new Map<string, Batch>()
  /**
   * The batch running on each partition. A delivery that arrives meanwhile
   * waits for it before joining the next batch, so the partition delivers
   * nothing else until then: one batch per partition at a time, at most
   * `size` messages buffered, and a delivery never outlives one batch run.
   */
  private readonly runningBatches = new Map<string, Promise<void>>()
  /** How many times each partition was revoked, so a delivery that waited through a revocation does not start a batch on a partition this member lost. */
  private readonly revocations = new Map<string, number>()
  /** The partitions whose revocation is being settled: nothing new is collected on them meanwhile. */
  private readonly revoking = new Set<string>()
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
    this.assertSubscribable(topic, handler, 'subscribe')
    const breaker = this.guard(topic, options.breaker)
    try {
      return this.route(topic, {
        run: { kind: 'each', handler: handler as Handler },
        serializer: (options.serializer ?? this.options.serializer ?? this.context.serializer) as Serializer,
        idempotency: (options.idempotency as IdempotencyOptions | undefined) ?? this.options.idempotency,
        breaker
      })
    } catch (error) {
      this.releaseBreaker(breaker)
      throw error
    }
  }

  /**
   * Like `subscribe()`, for a handler that takes the messages of one
   * partition in batches: up to `size` consecutive messages, or fewer once
   * `maxWait` passed since the first one arrived. The offset after the last
   * message of a batch is committed once the batch resolved; a batch that
   * throws sends every one of its messages down the retry ladder, each with
   * its own tracking headers. A message that does not deserialize is
   * dead-lettered on its own and the rest of the batch runs. Idempotency is
   * not applied to batches: a consumer configured with it refuses this.
   */
  subscribeBatch<T = unknown> (topic: string, handler: BatchHandler<T>, options: SubscribeBatchOptions<T> = {}): this {
    this.assertSubscribable(topic, handler, 'subscribeBatch')
    if (this.options.idempotency !== undefined) {
      throw new ConfigError(`subscribeBatch("${topic}"): idempotency applies to single-message handlers; deduplicate inside the batch handler, or use another consumer`)
    }
    const breaker = this.guard(topic, options.breaker)
    try {
      return this.route(topic, {
        run: {
          kind: 'batch',
          handler: handler as BatchHandler,
          size: requirePositiveInteger(options.size ?? 100, 'size'),
          maxWaitMs: parseDuration(options.maxWait ?? 1_000, 'maxWait')
        },
        serializer: (options.serializer ?? this.options.serializer ?? this.context.serializer) as Serializer,
        idempotency: undefined,
        breaker
      })
    } catch (error) {
      this.releaseBreaker(breaker)
      throw error
    }
  }

  /** The topic's breaker, from its own options or the consumer's, reporting its state changes as events. */
  private guard (topic: string, override: ConsumerBreakerOptions | false | undefined): Breaker | undefined {
    const breaker = resolveBreaker(override ?? this.options.breaker, this.groupId, topic, this.maxProcessingTimeMs)
    if (breaker === undefined) return undefined
    const listener = ({ from, to }: CircuitStateChange): void => {
      const line = `[kafka-harbor] group "${this.groupId}": circuit for "${topic}" ${from} -> ${to}`
      if (to === 'open' || to === 'isolated') this.context.logger.warn(line)
      else this.context.logger.info(line)
      this.context.emit('circuitStateChanged', { groupId: this.groupId, topic, from, to })
    }
    breaker.policy.on('stateChange', listener)
    this.breakers.push({ breaker, listener })
    return breaker
  }

  /** Stops listening to a breaker; one built here is released as well. */
  private releaseBreaker (breaker: Breaker | undefined): void {
    const index = this.breakers.findIndex((entry) => entry.breaker === breaker)
    if (index < 0) return
    const [{ listener }] = this.breakers.splice(index, 1) as [{ breaker: Breaker, listener: (change: CircuitStateChange) => void }]
    breaker?.policy.off('stateChange', listener)
    if (breaker?.owned === true) breaker.policy.dispose()
  }

  /** The last state, from every path that ends a consumer: the breakers go with it. */
  private finish (): void {
    this.state = 'stopped'
    for (const { breaker } of [...this.breakers]) this.releaseBreaker(breaker)
  }

  private assertSubscribable (topic: string, handler: unknown, method: string): void {
    if (this.state !== 'idle') throw new ConfigError(`${method}() must be called before start()`)
    requireNonEmptyString(topic, 'topic')
    if (typeof handler !== 'function') throw new ConfigError(`handler for "${topic}" must be a function`)
  }

  private route (topic: string, settings: Omit<Subscription, 'plan'>): this {
    const plan = new TopicPlan(topic, this.levels.length, this.retryNaming, this.dlqNaming)
    const subscription: Subscription = { plan, ...settings }
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
      if (existing === undefined) continue
      if (existing.level !== level) {
        throw new ConfigError(`topic "${retryTopic}" is level ${level} of "${topic}" but ${existing.level === 0 ? 'the original topic' : `level ${existing.level}`} of "${[...existing.owners.keys()].join('", "')}"`)
      }
      // A batch is formed per partition, not per owner: on a shared retry
      // topic it would mix the owners' messages and commit across them.
      if (settings.run.kind === 'batch' || [...existing.owners.values()].some((owner) => owner.run.kind === 'batch')) {
        throw new ConfigError(`retry topic "${retryTopic}" would be shared between "${topic}" and "${[...existing.owners.keys()].join('", "')}", and a batch subscription cannot share a retry topic; give it a retry.topicNaming of its own`)
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

  /**
   * How far behind the group is on every topic this consumer consumes, the
   * original ones and the retry ladder. One round of admin calls per
   * invocation, never per message: a metrics scrape is the caller this is
   * meant for. Connects the harbor if it is not yet; rejects with a
   * ConfigError when the adapter does not report offsets and with a
   * ClosedError once the harbor is shutting down.
   */
  async lag (): Promise<PartitionLag[]> {
    const { admin, name } = this.context.adapter
    if (admin.fetchTopicOffsets === undefined || admin.fetchCommittedOffsets === undefined) {
      throw new ConfigError(`adapter "${name}" does not report offsets (admin.fetchTopicOffsets and admin.fetchCommittedOffsets), so lag is not available`)
    }
    // Connecting a closed harbor is a ClosedError, which is the right answer here too.
    await this.context.ensureConnected()
    const topics = [...this.routes.keys()]
    const [watermarks, committed] = await Promise.all([admin.fetchTopicOffsets(topics), admin.fetchCommittedOffsets(this.groupId, topics)])
    const committedAt = new Map(committed.map((entry) => [partitionKey(entry.topic, entry.partition), entry.offset]))
    const fromBeginning = this.options.fromBeginning ?? false
    // Offsets cross the adapter boundary as strings; an adapter that hands
    // back a sentinel or garbage must not turn into a negative or NaN lag.
    const checked = (value: string, what: string, at: TopicPartition): string => {
      if (!OFFSET_PATTERN.test(value)) throw new AdapterError(`adapter "${name}" reported an invalid ${what} for ${at.topic}[${at.partition}]: ${JSON.stringify(value)}`, { retryable: false })
      return value
    }
    return watermarks.map((partition): PartitionLag => {
      const low = checked(partition.low, 'low watermark', partition)
      const high = checked(partition.high, 'high watermark', partition)
      const offset = committedAt.get(partitionKey(partition.topic, partition.partition)) ?? null
      const committedOffset = offset === null ? null : checked(offset, 'committed offset', partition)
      // A committed offset outside [low, high] is one the broker would reset
      // (the records expired, the topic was recreated): the group's position
      // is then where a fresh group would start, not the stale number.
      const inRange = committedOffset !== null && offsetDistance(low, committedOffset) >= 0 && offsetDistance(committedOffset, high) >= 0
      const position = inRange ? committedOffset : fromBeginning ? low : high
      return { groupId: this.groupId, topic: partition.topic, partition: partition.partition, low, high, committed: committedOffset, lag: offsetDistance(position, high) }
    })
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
      this.finish()
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
    const revoked = new Set(partitions.map(({ topic, partition }) => partitionKey(topic, partition)))
    // A batch still collecting on a revoked partition runs now, so its
    // messages are committed before the next owner starts, like a handler
    // already running would be; the wait below covers it.
    for (const key of revoked) {
      this.revocations.set(key, (this.revocations.get(key) ?? 0) + 1)
      this.revoking.add(key)
      const batch = this.batches.get(key)
      if (batch !== undefined) this.flushBatch(key, batch)
    }
    const running = [...this.deliveries].filter(([, entry]) => entry.active && revoked.has(partitionKey(entry.raw.topic, entry.raw.partition)))
    try {
      await this.settledInTime(Promise.all(running.map(([work]) => work)), this.maxProcessingTimeMs)
    } finally {
      for (const key of revoked) this.revoking.delete(key)
    }
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
        this.finish()
        this.stopReason = reason
        this.context.emit('consumerStopped', { groupId: this.groupId, reason })
        return
      }
    }
    if (this.state === 'idle' || this.state === 'stopped') {
      this.finish()
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
    this.discardBatches([...this.batches.keys()])
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
      this.finish()
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
    const receivedAt = this.context.clock.now()
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
    const item: Pending = { raw, headers, retry, attempt, correlationId, receivedAt }
    if (subscription.run.kind === 'batch') {
      await this.collect(item, entry, subscription, subscription.run, level)
      return
    }
    entry.active = true

    const startedAt = this.context.clock.now()
    let error: unknown
    let replayed = false
    try {
      const message = await toMessage(raw, headers, subscription.serializer, subscription.plan.original, retry)
      // Decoding may take a while (a schema registry); a shutdown that gave
      // up meanwhile must not see the handler start on a stopped consumer.
      if (this.shutdownController.signal.aborted) return
      const handlerContext: HandlerContext = {
        groupId: this.groupId,
        correlationId,
        logger: this.context.logger,
        signal: this.shutdownController.signal,
        attempt
      }
      const { handler } = subscription.run
      const { idempotency, breaker } = subscription
      const wrapHandler = this.context.instrumentation?.wrapHandler
      const runHandler = async (): Promise<void> => {
        if (wrapHandler === undefined && idempotency === undefined) {
          await handler(message, handlerContext)
          return
        }
        // The engine runs inside the instrumentation, so a span covers the
        // lookup as well as the handler, and a replay shows as a short span.
        const work = async (): Promise<void> => {
          if (idempotency === undefined) {
            await handler(message, handlerContext)
            return
          }
          const input = (idempotency.key ?? defaultIdempotencyKey)(message, handlerContext)
          const outcome = await idempotency.engine.executeWithMetadata(input, async () => { await handler(message, handlerContext) })
          replayed = outcome.replayed
        }
        await wrapped(wrapHandler === undefined ? undefined : (run) => wrapHandler(message, handlerContext, run), work, this.context.logger)
      }
      if (breaker === undefined) await runHandler()
      else await this.guarded(breaker, item, runHandler)
    } catch (thrown) {
      error = thrown
    }
    // Held by the breaker until the consumer began stopping: neither a
    // verdict nor a failure. Left uncommitted for the next member.
    if (error === HELD_UNTIL_RELEASED) return
    const durationMs = this.context.clock.now() - startedAt
    const at = this.locate(item)

    if (this.shutdownController.signal.aborted) {
      // The handler was abandoned by shutdown; whatever it returned or threw
      // is no longer a verdict on the message. No commit, no retry: the
      // message is redelivered to the next member.
      return
    }
    if (error === undefined) {
      if (await this.commit(raw)) this.context.emit('messageProcessed', { ...at, durationMs, replayed })
      return
    }
    if (isAbortProcessingError(error)) {
      this.context.emit('messageFailed', { ...at, error, durationMs, outcome: 'abort' })
      this.context.emit('error', { error, scope: 'consumer', groupId: this.groupId, topic: raw.topic })
      this.stopFromPipeline('abort')
      return
    }

    const verdict = await this.forwardFailed(item, subscription, level, error)
    if (verdict.outcome === 'crash') {
      this.context.emit('messageFailed', { ...at, error, durationMs, outcome: 'crash' })
      throw error
    }
    if (!await this.commit(raw)) return
    this.emitFailure(item, verdict, error, durationMs)
  }

  /**
   * Runs the handler through the topic's circuit breaker. While the circuit
   * is open the delivery is held instead of failed: the partition waits
   * (the adapter delivers nothing else on it), nothing is committed and
   * nothing goes down the ladder, until the breaker lets a probe through
   * or the hold runs out. The hold is counted from the moment the handler
   * would have run and never reaches past what the client tolerates for
   * the delivery as a whole (`maxProcessingTime` from when it was received,
   * retry delay included). A hold that runs out fails the message with a
   * retryable HoldExpiredError, so it walks the ladder and comes back when
   * the dependency may be up. A stop, or a rebalance taking the partition
   * away, ends the hold with the message uncommitted.
   */
  private async guarded (breaker: Breaker, item: Pending, run: () => Promise<void>): Promise<void> {
    const key = partitionKey(item.raw.topic, item.raw.partition)
    const tolerated = item.receivedAt + this.maxProcessingTimeMs
    const deadline = breaker.holdMs === undefined ? tolerated : Math.min(this.context.clock.now() + breaker.holdMs, tolerated)
    let heldSince: number | undefined
    for (;;) {
      // Only a rejection WITHOUT a run is a hold. A handler that threw the
      // same code (its own inner breaker, say) ran and failed: that is its
      // verdict, counted by the breaker, and the ladder gets it.
      let entered = false
      try {
        await breaker.policy.execute(async () => {
          entered = true
          await run()
        }, { signal: this.shutdownController.signal })
        return
      } catch (error) {
        if (entered || !isCircuitRejection(error)) throw error
        const now = this.context.clock.now()
        heldSince ??= now
        if (this.drainController.signal.aborted || this.revoking.has(key)) throw HELD_UNTIL_RELEASED
        if (now >= deadline) throw new HoldExpiredError(breaker.topic, now - heldSince, { cause: error })
        await this.context.clock.sleep(Math.min(deadline - now, HOLD_POLL_MS), this.drainController.signal)
        if (this.drainController.signal.aborted || this.revoking.has(key)) throw HELD_UNTIL_RELEASED
      }
    }
  }

  /**
   * Sends a message the handler failed on to the next topic of its ladder
   * (retry level, then DLQ) and says where it went; the caller commits. A
   * message that cannot be decoded will not decode next time either, so it
   * skips the retry levels. Without a level left and without a DLQ the
   * verdict is a crash: the only honest outcome is to stop without
   * committing, so nothing is lost and someone has to look.
   */
  private async forwardFailed (item: Pending, subscription: Subscription, level: number, error: unknown): Promise<Verdict> {
    const names = this.context.headerNames
    const nextLevel = level + 1
    const retryTopic = subscription.plan.retryTopic(nextLevel)
    const wantsRetry = !isSerializationError(error) && this.retryIf(error)
    const tracking = writeRetryInfo(item.headers, names, {
      previous: item.retry,
      originalTopic: subscription.plan.original,
      error: describeError(error),
      now: new Date(this.context.clock.now())
    })
    if (wantsRetry && retryTopic !== undefined) {
      await produceHop(this.context, item.raw, item.headers, retryTopic, 'retry', tracking)
      return { outcome: 'retry', retryTopic, level: nextLevel }
    }
    const dlqTopic = subscription.plan.dlqTopic
    if (dlqTopic !== undefined) {
      tracking[names.deadLetteredAt] = new Date(this.context.clock.now()).toISOString()
      await produceHop(this.context, item.raw, item.headers, dlqTopic, 'dead-letter', tracking)
      return { outcome: 'dead-letter', dlqTopic }
    }
    return { outcome: 'crash' }
  }

  /** Where a message is, as every event names it. */
  private locate (item: Pending): { topic: string, partition: number, offset: string, groupId: string, correlationId: string | undefined } {
    return { topic: item.raw.topic, partition: item.raw.partition, offset: item.raw.offset, groupId: this.groupId, correlationId: item.correlationId }
  }

  /** The events of a forwarded failure, once its offset is committed. */
  private emitFailure (item: Pending, verdict: Exclude<Verdict, { outcome: 'crash' }>, error: unknown, durationMs: number, batch?: number): void {
    const at = this.locate(item)
    this.context.emit('messageFailed', { ...at, error, durationMs, outcome: verdict.outcome, batch })
    if (verdict.outcome === 'retry') this.context.emit('messageRetried', { ...at, retryTopic: verdict.retryTopic, level: verdict.level, attempt: item.attempt, error })
    else this.context.emit('messageDeadLettered', { ...at, dlqTopic: verdict.dlqTopic, attempts: item.attempt, error })
  }

  /**
   * Adds a delivery to its partition's batch. Every delivery but the one
   * that fills the batch is reported settled to the adapter at once (the
   * partition would not deliver the next one otherwise); the filling one
   * holds its delivery open while the batch runs. A delivery that arrives
   * while a batch of its partition runs waits for it first, so at most
   * `size` messages are buffered per partition and one batch runs at a
   * time. A batch that does not fill runs `maxWait` after its first message.
   */
  private async collect (item: Pending, entry: Delivery, subscription: Subscription, run: BatchRun, level: number): Promise<void> {
    const key = partitionKey(item.raw.topic, item.raw.partition)
    // A message on a partition being taken away is the next owner's; so is
    // one that waited for a batch while the partition was taken away, or
    // while that batch stopped the consumer. Left uncommitted either way.
    if (this.revoking.has(key)) return
    const running = this.runningBatches.get(key)
    if (running !== undefined) {
      const revocation = this.revocations.get(key)
      await running
      if (this.state !== 'running' || this.revocations.get(key) !== revocation) return
    }
    let batch = this.batches.get(key)
    if (batch === undefined) {
      const started: Batch = { subscription, run, level, items: [], timer: new AbortController() }
      batch = started
      this.batches.set(key, started)
      this.context.clock.sleep(run.maxWaitMs, started.timer.signal).then(() => {
        if (!started.timer.signal.aborted && this.batches.get(key) === started) this.flushBatch(key, started)
      }).catch(() => undefined)
    }
    batch.items.push(item)
    if (batch.items.length < run.size) return
    this.batches.delete(key)
    batch.timer.abort()
    entry.active = true
    await this.runBatch(key, batch)
  }

  /**
   * Runs a batch that did not fill (its wait passed, or its partition is
   * being revoked) on a delivery of its own, so shutdown and revocation
   * wait for it like for any running handler.
   */
  private flushBatch (key: string, batch: Batch): void {
    this.batches.delete(key)
    batch.timer.abort()
    const last = batch.items[batch.items.length - 1] as Pending
    const owned: Delivery = { raw: last.raw, active: true, release: () => {} }
    const work = this.runBatch(key, batch)
    this.deliveries.set(work, owned)
    work.finally(() => { this.deliveries.delete(work) }).catch(() => undefined)
  }

  /** Forgets the batches collected on these partitions without running them. */
  private discardBatches (keys: readonly string[]): void {
    for (const key of keys) {
      this.batches.get(key)?.timer.abort()
      this.batches.delete(key)
    }
  }

  /**
   * Runs a batch and lets the partition's next delivery wait for it. A
   * pipeline failure crashes the consumer here, before the waiting delivery
   * resumes, so what it finds is a consumer already stopping.
   */
  private async runBatch (key: string, batch: Batch): Promise<void> {
    const last = batch.items[batch.items.length - 1] as Pending
    const run = this.handleBatch(batch).catch(async (error: unknown) => { await this.crash(error, last.raw) })
    this.runningBatches.set(key, run)
    try {
      await run
    } finally {
      if (this.runningBatches.get(key) === run) this.runningBatches.delete(key)
    }
  }

  private async handleBatch (batch: Batch): Promise<void> {
    const { subscription, run, level, items } = batch
    const first = items[0] as Pending
    const last = items[items.length - 1] as Pending
    const context: BatchContext = {
      groupId: this.groupId,
      topic: first.raw.topic,
      partition: first.raw.partition,
      logger: this.context.logger,
      signal: this.shutdownController.signal
    }
    // A message that does not decode is failed on its own, before the
    // handler runs; the rest of the batch is what the handler gets.
    const failures = new Map<Pending, unknown>()
    const messages: Message[] = []
    const decoded: Pending[] = []
    // Decoding counts as processing, as it does for a single message.
    const startedAt = this.context.clock.now()
    for (const item of items) {
      try {
        messages.push(await toMessage(item.raw, item.headers, subscription.serializer, subscription.plan.original, item.retry))
        decoded.push(item)
      } catch (error) {
        failures.set(item, error)
      }
    }
    if (this.shutdownController.signal.aborted) return
    let error: unknown
    if (messages.length > 0) {
      const wrapBatch = this.context.instrumentation?.wrapBatchHandler
      const runHandler = async (): Promise<void> => {
        await wrapped(wrapBatch === undefined ? undefined : (wrappedRun) => wrapBatch(messages, context, wrappedRun), async () => { await run.handler(messages, context) }, this.context.logger)
      }
      try {
        const { breaker } = subscription
        if (breaker === undefined) await runHandler()
        else await this.guarded(breaker, first, runHandler)
      } catch (thrown) {
        error = thrown
      }
    }
    if (error === HELD_UNTIL_RELEASED) return
    const durationMs = this.context.clock.now() - startedAt
    if (this.shutdownController.signal.aborted) return
    const size = items.length
    const batchAt = { topic: first.raw.topic, partition: first.raw.partition, groupId: this.groupId, size, durationMs }
    // The handler's own failure, when it failed the whole batch: what the
    // batch's outcome follows. A BatchFailedError is a handler that resolved
    // for the rest, and a message that did not decode is its own failure.
    const handlerFailure = error !== undefined && !isBatchFailedError(error) ? error : undefined
    let outcome: ConsumerEvents['batchProcessed']['outcome'] = 'processed'
    if (error !== undefined) {
      if (isAbortProcessingError(error)) {
        for (const item of decoded) this.context.emit('messageFailed', { ...this.locate(item), error, durationMs, outcome: 'abort', batch: size })
        this.context.emit('batchProcessed', { ...batchAt, outcome: 'abort' })
        this.context.emit('error', { error, scope: 'consumer', groupId: this.groupId, topic: first.raw.topic })
        this.stopFromPipeline('abort')
        return
      }
      if (isBatchFailedError(error)) {
        // The handler named its failures; naming a message that is not in
        // the batch is a bug in the handler, and stopping is the honest
        // answer to a bug: nothing is committed, someone has to look.
        const byOffset = new Map(decoded.map((item) => [item.raw.offset, item]))
        for (const message of error.failed) {
          const item = message.topic === first.raw.topic && message.partition === first.raw.partition ? byOffset.get(message.offset) : undefined
          if (item === undefined) throw new ConfigError(`BatchFailedError names ${message.topic}[${message.partition}]@${message.offset}, which is not in the batch of ${first.raw.topic}[${first.raw.partition}] ${first.raw.offset}..${last.raw.offset}`)
          failures.set(item, error.cause ?? error)
        }
      } else {
        for (const item of decoded) failures.set(item, error)
      }
    }
    // Forward in offset order, then one commit for the whole batch: a
    // failure to forward leaves everything uncommitted, as for one message.
    const verdicts = new Map<Pending, Exclude<Verdict, { outcome: 'crash' }>>()
    for (const item of items) {
      const failure = failures.get(item)
      if (failure === undefined) continue
      const verdict = await this.forwardFailed(item, subscription, level, failure)
      if (verdict.outcome === 'crash') {
        this.context.emit('messageFailed', { ...this.locate(item), error: failure, durationMs, outcome: 'crash', batch: size })
        this.context.emit('batchProcessed', { ...batchAt, outcome: 'crash' })
        throw failure
      }
      verdicts.set(item, verdict)
      if (failure === handlerFailure) outcome = verdict.outcome
    }
    if (!await this.commit(last.raw)) return
    for (const item of items) {
      const verdict = verdicts.get(item)
      if (verdict !== undefined) {
        this.emitFailure(item, verdict, failures.get(item), durationMs, size)
        continue
      }
      this.context.emit('messageProcessed', { ...this.locate(item), durationMs, replayed: false, batch: size })
    }
    // A batch with nothing to hand to the handler had no run to report.
    if (messages.length > 0) this.context.emit('batchProcessed', { ...batchAt, outcome })
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
    await produceHop(this.context, raw, headers, dlqTopic, 'dead-letter', tracking)
    if (!await this.commit(raw)) return
    this.context.emit('messageFailed', { ...at, error, durationMs: 0, outcome: 'dead-letter' })
    this.context.emit('messageDeadLettered', { ...at, dlqTopic, attempts: (retry?.count ?? 0) + 1, error })
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

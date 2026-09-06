import { systemClock } from './clock'
import { randomUUID } from 'node:crypto'
import { parseDuration } from './duration'
import type { CoreContext } from './context'
import type { RetryPolicy } from 'breakwater'
import { requireNonEmptyString } from './validate'
import type { Clock, Duration, Logger } from './types'
import { createEmitter, type Observable } from './events'
import { headerNames, type HeaderNames } from './headers'
import { jsonSerializer, type Serializer } from './serializer'
import type { BrokerConfig, ClientAdapter, SaslConfig } from './adapter'
import { AbortProcessingError, ClosedError, ConfigError, describeError } from './errors'
import { redrive, type RedriveEvents, type RedriveOptions, type RedriveResult } from './redrive'
import { Producer, buildProducerRetry, type ProducerOptions, type ProducerRetryOptions } from './producer'
import { Consumer, type ConsumerEvents, type ConsumerOptions, type ConsumerState, type StopReason } from './consumer'

export interface HeaderOptions {
  /** Prefix of every header the library writes. Default: 'x-'. */
  prefix?: string
  /** Generates a correlation id when the outgoing message has none. Default: UUID v4. */
  correlationId?: () => string
}

export interface HarborConfig {
  clientId: string
  brokers: readonly string[]
  /**
   * The Kafka client behind the harbor, e.g. `confluentAdapter()` from
   * 'kafka-harbor/adapters/confluent'. Explicit on purpose: the core has no
   * dependency on any client, so the install that never touches the native
   * Confluent binding is the one that never imports that entry point.
   */
  adapter: ClientAdapter
  ssl?: boolean
  sasl?: SaslConfig
  /** Harbor-wide serializer, overridable per producer, consumer and topic. Default: strict JSON. */
  serializer?: Serializer
  headers?: HeaderOptions
  logger?: Logger
  /**
   * Retry of the produce calls the library makes on its own behalf (retry
   * topics, DLQ). Producers created with `harbor.producer()` take their own.
   */
  produceRetry?: ProducerRetryOptions
  /** Time source. Tests inject a manual clock; production leaves the default. */
  clock?: Clock
}

export type HarborState = 'idle' | 'connecting' | 'connected' | 'closing' | 'closed'

export interface ConsumerHealth {
  readonly groupId: string
  readonly status: ConsumerState
  /** Set once the consumer stopped; 'abort' and 'crash' make the harbor unhealthy. */
  readonly stoppedBecause: StopReason | undefined
}

export interface HarborHealth {
  /**
   * True while the harbor can do work: it is not shutting down and no
   * consumer stopped on its own (abort or crash). A harbor that has not
   * connected yet is healthy: connection is lazy by design.
   */
  readonly healthy: boolean
  readonly state: HarborState
  readonly adapter: string
  readonly consumers: readonly ConsumerHealth[]
}

export interface HarborEvents extends ConsumerEvents, RedriveEvents {
  connected: { adapter: string }
  disconnected: { adapter: string }
}

/**
 * The configuration as the harbor exposes it back: everything given, minus
 * the SASL password, so logging or serializing a harbor never prints a
 * credential.
 */
export type ExposedHarborConfig = Readonly<Omit<HarborConfig, 'sasl'> & { sasl?: Readonly<Omit<SaslConfig, 'password'>> }>

const ADAPTER_METHODS = ['connect', 'disconnect', 'produce', 'consume'] as const

const defaultLogger: Logger = {
  debug: undefined,
  info: (message, ...args) => console.info(message, ...args),
  warn: (message, ...args) => console.warn(message, ...args),
  error: (message, ...args) => console.error(message, ...args)
}

/**
 * The application-level Kafka client. `createHarbor()` does not connect:
 * the connection is opened lazily by the first `send()` or `start()`, or
 * explicitly by `connect()`.
 */
export class Harbor implements Observable<HarborEvents> {
  readonly config: ExposedHarborConfig
  private readonly fullConfig: Readonly<HarborConfig>
  private readonly adapter: ClientAdapter
  private readonly serializer: Serializer
  private readonly names: HeaderNames
  private readonly correlationId: () => string
  private readonly logger: Logger
  private readonly clock: Clock
  private readonly emitter = createEmitter<HarborEvents>((error) => {
    this.logger.error(`[kafka-harbor] event listener threw: ${describeError(error)}`)
  })

  private readonly producePolicy: RetryPolicy
  private readonly consumers = new Set<Consumer>()
  private state: HarborState = 'idle'
  /** Whether the adapter holds an open connection that shutdown must release. */
  private adapterConnected = false
  private connecting: Promise<void> | undefined
  private closing: Promise<void> | undefined
  private signalHandlers: Array<{ signal: NodeJS.Signals, handler: () => void }> = []

  constructor (config: HarborConfig) {
    requireNonEmptyString(config?.clientId, 'clientId')
    if (!Array.isArray(config.brokers) || config.brokers.length === 0 || config.brokers.some((b) => typeof b !== 'string' || b === '')) {
      throw new ConfigError('brokers must be a non-empty array of "host:port" strings')
    }
    const adapter = config.adapter as Partial<ClientAdapter> | undefined
    if (adapter === undefined || ADAPTER_METHODS.some((method) => typeof adapter[method] !== 'function') || typeof adapter.admin?.createTopics !== 'function' || typeof adapter.admin.topicExists !== 'function') {
      throw new ConfigError('adapter must implement ClientAdapter (connect, disconnect, produce, consume, admin.createTopics, admin.topicExists)')
    }
    this.fullConfig = config
    const { sasl, ...rest } = config
    this.config = Object.freeze({ ...rest, ...(sasl !== undefined && { sasl: Object.freeze({ mechanism: sasl.mechanism, username: sasl.username }) }) })
    this.adapter = config.adapter
    this.serializer = config.serializer ?? jsonSerializer()
    this.names = headerNames(config.headers?.prefix)
    this.correlationId = config.headers?.correlationId ?? randomUUID
    this.logger = config.logger ?? defaultLogger
    this.clock = config.clock ?? systemClock
    this.producePolicy = buildProducerRetry(config.produceRetry)
  }

  get status (): HarborState {
    return this.state
  }

  /** The header names in effect, for applications that read them directly. */
  get headerNames (): HeaderNames {
    return this.names
  }

  /**
   * A snapshot for readiness and liveness probes. Cheap and synchronous: it
   * reads the state the harbor already tracks and never calls the broker.
   */
  health (): HarborHealth {
    const consumers = [...this.consumers].map((consumer) => ({
      groupId: consumer.groupId,
      status: consumer.status,
      stoppedBecause: consumer.stoppedBecause
    }))
    const failed = consumers.some((entry) => entry.stoppedBecause === 'abort' || entry.stoppedBecause === 'crash')
    return {
      healthy: !this.isClosed() && !failed,
      state: this.state,
      adapter: this.adapter.name,
      consumers
    }
  }

  isHealthy (): boolean {
    return this.health().healthy
  }

  on<K extends keyof HarborEvents> (event: K, listener: (payload: HarborEvents[K]) => void): this {
    this.emitter.on(event, listener)
    return this
  }

  off<K extends keyof HarborEvents> (event: K, listener: (payload: HarborEvents[K]) => void): this {
    this.emitter.off(event, listener)
    return this
  }

  producer<T = unknown> (options: ProducerOptions<T> = {}): Producer<T> {
    return new Producer<T>(this.pipelineContext(), options)
  }

  consumer (options: ConsumerOptions): Consumer {
    const consumer = new Consumer(this.pipelineContext(), options)
    this.consumers.add(consumer)
    return consumer
  }

  /** What every pipeline gets from the harbor. Events narrow to each pipeline's own map. */
  private pipelineContext<E extends { [K in keyof E]: K extends keyof HarborEvents ? HarborEvents[K] : never }> (): CoreContext<E> {
    return {
      adapter: this.adapter,
      clientId: this.fullConfig.clientId,
      serializer: this.serializer,
      headerNames: this.names,
      correlationId: this.correlationId,
      logger: this.logger,
      clock: this.clock,
      producePolicy: this.producePolicy,
      emit: (event, payload) => this.emitter.emit(event as keyof HarborEvents, payload as HarborEvents[keyof HarborEvents]),
      isClosed: () => this.isClosed(),
      ensureConnected: () => this.connect()
    }
  }

  /**
   * Drains a dead-letter topic back into service: every message is
   * re-produced with its original key and value to its original topic (or to
   * `to`), the failed run's tracking headers removed, and the DLQ offset
   * committed only after the broker acknowledged. Stops after `max`
   * messages or once the DLQ has been idle for `idleTimeout`.
   */
  async redrive (options: RedriveOptions): Promise<RedriveResult> {
    return await redrive(this.pipelineContext(), options)
  }

  /** Opens the client connection. Idempotent and single-flight. */
  async connect (): Promise<void> {
    if (this.isClosed()) throw new ClosedError('harbor')
    if (this.connecting === undefined) {
      this.state = 'connecting'
      const broker: BrokerConfig = {
        clientId: this.fullConfig.clientId,
        brokers: this.fullConfig.brokers,
        ...(this.fullConfig.ssl !== undefined && { ssl: this.fullConfig.ssl }),
        ...(this.fullConfig.sasl !== undefined && { sasl: this.fullConfig.sasl })
      }
      // The transitions are guarded: a shutdown that began while the connect
      // was in flight owns the state from then on, and a connect that lands
      // after the shutdown stopped waiting for it releases the client itself.
      this.connecting = this.adapter.connect(broker).then(() => {
        if (this.state === 'closed') {
          return this.adapter.disconnect().catch((error: unknown) => {
            this.logger.error(`[kafka-harbor] disconnect after a late connect failed: ${describeError(error)}`)
          })
        }
        this.adapterConnected = true
        if (this.state === 'connecting') {
          this.state = 'connected'
          this.emitter.emit('connected', { adapter: this.adapter.name })
        }
      }, (error: unknown) => {
        if (this.state === 'connecting') this.state = 'idle'
        this.connecting = undefined
        throw error
      })
    }
    await this.connecting
  }

  /**
   * Graceful shutdown: stop fetching on every consumer, wait for in-flight
   * handlers up to `timeout`, let them commit, leave the groups, then
   * disconnect. Rejects with `ShutdownTimeoutError` when handlers had to be
   * abandoned; the disconnect still happens first.
   */
  async shutdown (timeout: Duration = 30_000): Promise<void> {
    const timeoutMs = parseDuration(timeout, 'shutdown timeout')
    if (this.closing === undefined) this.closing = this.doShutdown(timeoutMs)
    return await this.closing
  }

  private async doShutdown (timeoutMs: number): Promise<void> {
    const deadline = this.clock.now() + timeoutMs
    this.state = 'closing'
    this.disableSignalHandlers()
    const results = await Promise.allSettled([...this.consumers].map((consumer) => consumer.stop(timeoutMs)))
    try {
      // A connect still in flight gets what is left of the timeout; one that
      // never settles must not hold the shutdown, so it is left to release
      // the client on its own when (if) it lands.
      if (this.connecting !== undefined && !await this.settledInTime(this.connecting, Math.max(0, deadline - this.clock.now()))) {
        this.logger.warn(`[kafka-harbor] connect still pending after ${timeoutMs}ms; the client is released when it settles`)
      }
      if (this.adapterConnected) {
        await this.adapter.disconnect()
        this.emitter.emit('disconnected', { adapter: this.adapter.name })
      }
    } finally {
      this.state = 'closed'
    }
    const failed = results.find((result): result is PromiseRejectedResult => result.status === 'rejected')
    if (failed !== undefined) throw failed.reason
  }

  /** Whether `promise` settles within `timeoutMs`; the timer is cancelled either way. */
  private async settledInTime (promise: Promise<unknown>, timeoutMs: number): Promise<boolean> {
    const deadlineController = new AbortController()
    // The sleep resolves to nothing, so only the promise settling yields true.
    const settled = await Promise.race([
      Promise.allSettled([promise]).then(() => true),
      this.clock.sleep(timeoutMs, deadlineController.signal)
    ])
    deadlineController.abort()
    return settled === true
  }

  /**
   * Builds the error a handler throws to stop the consumer WITHOUT
   * committing: `throw harbor.abort(cause)`. For infrastructure bugs where
   * reprocessing after a restart is the right outcome and neither a retry
   * topic nor the DLQ is.
   */
  abort (cause: unknown): AbortProcessingError {
    return new AbortProcessingError(cause)
  }

  /**
   * Opt-in: a SIGTERM or SIGINT triggers `shutdown()`. Returns a function
   * that removes the handlers again. Errors from the shutdown are logged;
   * the process exit itself is left to the application.
   */
  enableSignalHandlers (signals: readonly NodeJS.Signals[] = ['SIGTERM', 'SIGINT'], timeout: Duration = 30_000): () => void {
    // Validated now, not when the signal arrives: a bad value discovered at
    // SIGTERM time would leave the process without a shutdown at all.
    parseDuration(timeout, 'enableSignalHandlers timeout')
    this.disableSignalHandlers()
    for (const signal of signals) {
      const handler = (): void => {
        this.logger.info(`[kafka-harbor] ${signal} received, shutting down`)
        this.shutdown(timeout).catch((error: unknown) => {
          this.logger.error(`[kafka-harbor] shutdown after ${signal} failed: ${describeError(error)}`)
        })
      }
      process.on(signal, handler)
      this.signalHandlers.push({ signal, handler })
    }
    return () => this.disableSignalHandlers()
  }

  private disableSignalHandlers (): void {
    for (const { signal, handler } of this.signalHandlers) process.off(signal, handler)
    this.signalHandlers = []
  }

  private isClosed (): boolean {
    return this.state === 'closing' || this.state === 'closed'
  }
}

export function createHarbor (config: HarborConfig): Harbor {
  return new Harbor(config)
}

/** Standalone form of `harbor.abort()`, for handlers without a harbor in scope. */
export function abortProcessing (cause: unknown): AbortProcessingError {
  return new AbortProcessingError(cause)
}

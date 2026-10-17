/**
 * ClientAdapter over @platformatic/kafka, a pure TypeScript client: no
 * native binding, so it installs on every image Node runs on (Alpine,
 * arm64, Node 26) without a compiler. The client is a peer dependency loaded
 * on `connect()`, never at import time.
 *
 * The client hands messages out as one stream per consumption with every
 * partition interleaved; the adapter turns that into the gate the contract
 * asks for: a queue per partition, one worker per partition, `concurrency`
 * workers at a time, and the stream is read only while fewer than
 * `bufferedMessages` messages are waiting. Heartbeats run on the client's
 * own timer, so a slow handler never gets the member kicked out, and
 * `maxProcessingTimeMs` has nothing to map to.
 */
import type {
  Admin,
  AdminOptions,
  BaseOptions,
  Consumer,
  ConsumeBaseOptions,
  ConsumeOptions as ClientConsumeOptions,
  ConsumerOptions as ClientConsumerOptions,
  GroupAssignment,
  GroupOptions,
  GroupPartitionsAssigner,
  GroupPartitionsAssignments,
  Message,
  MessagesStream,
  Producer,
  ProducerOptions,
  SASLOptions
} from '@platformatic/kafka'

import {
  AdapterError,
  ConfigError,
  FencedError,
  describeError,
  parseDuration,
  partitionKey,
  splitPartitionKey,
  type BrokerConfig,
  type ClientAdapter,
  type CommittedOffset,
  type ConsumeOptions,
  type ConsumerHandle,
  type Duration,
  type PartitionOffsets,
  type RawMessage,
  type RawRecord,
  type TopicPartition,
  type TopicPartitionOffset,
  type TopicSpec,
  type TransactionHandle
} from '../../index'

type Bytes = Buffer
type ClientProducer = Producer<Bytes, Bytes, Bytes, Bytes>
type ClientConsumer = Consumer<Bytes, Bytes, Bytes, Bytes>
type ClientMessage = Message<Bytes, Bytes, Bytes, Bytes>
type ClientStream = MessagesStream<Bytes, Bytes, Bytes, Bytes>
type StreamOptions = ClientConsumeOptions<Bytes, Bytes, Bytes, Bytes>

/** The part of the client module the adapter uses; injectable for tests. */
export interface PlatformaticClientModule {
  Producer: new (options: ProducerOptions<Bytes, Bytes, Bytes, Bytes>) => ClientProducer
  Consumer: new (options: ClientConsumerOptions<Bytes, Bytes, Bytes, Bytes>) => ClientConsumer
  Admin: new (options: AdminOptions) => Admin
}

/** Client options shared by every client the adapter opens: timeouts, retries, TLS details, SASL variants the harbor config has no field for. */
export type PlatformaticClientConfig = Partial<Omit<BaseOptions, 'clientId' | 'bootstrapBrokers'>>
/** Producer options the adapter does not own (compression, partitioner, ...). */
export type PlatformaticProducerConfig = PlatformaticClientConfig &
  Partial<Omit<ProducerOptions<Bytes, Bytes, Bytes, Bytes>, keyof BaseOptions | 'acks' | 'idempotent' | 'serializers' | 'registry' | 'beforeSerialization' | 'transactionalId'>>
/** Consumer options the adapter does not own: group timing, fetch sizes, the client's own stream buffer. */
export type PlatformaticConsumerConfig = PlatformaticClientConfig &
  Partial<GroupOptions> &
  Partial<Omit<ConsumeBaseOptions<Bytes, Bytes, Bytes, Bytes>, 'autocommit' | 'deserializers' | 'registry' | 'beforeDeserialization'>>

export interface PlatformaticAdapterOptions {
  /**
   * The client module. Default: `import('@platformatic/kafka')` on first
   * connect. Inject a fake to unit-test the adapter without a broker.
   */
  client?: PlatformaticClientModule | (() => Promise<PlatformaticClientModule>)
  /** Extra client options for every client (producer, consumers, admin). */
  global?: PlatformaticClientConfig
  /** Extra client options for the producer. */
  producer?: PlatformaticProducerConfig
  /** Extra client options for every consumer. */
  consumer?: PlatformaticConsumerConfig
  /** Extra client options for the admin client. */
  admin?: PlatformaticClientConfig
  /** How long `createTopics` waits for a created topic to be visible, and how long a transaction's begin is tried. A duration (`'30s'`) or milliseconds. Default: '30s'. */
  adminTimeoutMs?: Duration
  /**
   * Messages one consumption holds, all partitions together, before the
   * adapter stops reading the client's stream. A paused partition (a retry
   * message parked by the core, say) keeps filling its share, and once the
   * total is reached the stream is left alone for every partition of the
   * consumption until the paused one drains; a retry level with a long
   * delay and a backlog behind it may need a larger value. Default: 1000.
   */
  bufferedMessages?: number
  /** The wait before a consumption whose stream failed opens a new one. A duration or milliseconds. Default: '1s'. */
  reconnectDelayMs?: Duration
  /**
   * How long the broker lets a transaction stay open before it aborts it
   * (at most the broker's `transaction.max.timeout.ms`, 15 minutes by
   * default). The client uses the same value as the request timeout of the
   * transactional producer. Default: the `timeout` of `producer`, or of
   * `global`, when one is set; '1m' otherwise.
   */
  transactionTimeout?: Duration
}

const SASL_MECHANISMS = {
  plain: 'PLAIN',
  'scram-sha-256': 'SCRAM-SHA-256',
  'scram-sha-512': 'SCRAM-SHA-512'
} as const satisfies Record<NonNullable<BrokerConfig['sasl']>['mechanism'], SASLOptions['mechanism']>

/**
 * Options the adapter owns because the core's delivery guarantees depend
 * on them: a consumer that committed on its own would turn at-least-once
 * into at-most-once, and a producer that did not wait for every replica
 * would let the core commit a source offset before the record is safe.
 */
const RESERVED_CONSUMER_KEYS = ['autocommit', 'deserializers', 'registry', 'beforeDeserialization', 'groupId']
const RESERVED_PRODUCER_KEYS = ['acks', 'idempotent', 'serializers', 'registry', 'beforeSerialization', 'transactionalId']
const RESERVED_CLIENT_KEYS = ['clientId', 'bootstrapBrokers']

/**
 * The client registers its request timeout as the transaction timeout, and
 * its default of five seconds aborts a transaction whose handler takes
 * longer; a minute is what the Confluent client defaults to.
 */
const DEFAULT_TRANSACTION_TIMEOUT = '1m'

/** The retries the producer makes on its own before the adapter reports the batch; the client would retry forever with idempotence on. */
const DEFAULT_PRODUCER_RETRIES = 3

/**
 * Partitions of every topic spread round-robin over the members that
 * subscribed to that topic. The client's own assigner spreads them over
 * every member of the group whatever each subscribed to, which leaves a
 * partition with a member that never fetches it as soon as members
 * subscribe to different topics; the core opens one member per retry level
 * of a group, so that is the normal layout here, and a rolling deploy that
 * adds a topic goes through it as well. The topics to place are every
 * member's, not only the leader's own, which is all the client hands over
 * in `topics`; the metadata covers them all. Members whose subscription the
 * leader could not read count as subscribed to everything, the way the
 * client would treat them.
 */
export const partitionAssignerBySubscription: GroupPartitionsAssigner = (_current, members, topics, metadata): GroupPartitionsAssignments[] => {
  const memberIds = [...members.keys()].sort()
  const assignments = new Map(memberIds.map((memberId) => [memberId, { memberId, assignments: new Map<string, GroupAssignment>() }]))
  const subscribedTopics = new Set(topics)
  for (const member of members.values()) for (const topic of member.topics ?? []) subscribedTopics.add(topic)
  // One cursor for every topic, as the client's own round-robin keeps: a
  // fresh one per topic would hand the first member the odd partition of
  // every topic.
  let cursor = 0
  for (const topic of [...subscribedTopics].sort()) {
    const subscribed = memberIds.filter((memberId) => members.get(memberId)?.topics?.includes(topic) ?? true)
    const count = metadata.topics.get(topic)?.partitionsCount ?? 0
    for (let partition = 0; partition < count && subscribed.length > 0; partition++) {
      const member = assignments.get(subscribed[cursor++ % subscribed.length] as string) as GroupPartitionsAssignments
      let entry = member.assignments.get(topic)
      if (entry === undefined) {
        entry = { topic, partitions: [] }
        member.assignments.set(topic, entry)
      }
      entry.partitions.push(partition)
    }
  }
  return [...assignments.values()]
}

/** The client declares `close(force)` twice, callback first; this is the promise form. */
const closeConsumer = (consumer: ClientConsumer): Promise<void> => (consumer.close as (force?: boolean) => Promise<void>)(true)

/** The wait between two looks at the metadata while a created topic propagates: 50ms, doubling up to 500ms. */
const nextPollDelay = (previous: number): number => Math.min(previous * 2, 500)

interface ClientErrorShape {
  code?: unknown
  canRetry?: unknown
  apiId?: unknown
  errors?: unknown
  cause?: unknown
}

/** Every error in the tree the client throws: it nests attempts in AggregateErrors and wraps causes. */
const errorNodes = (error: unknown): ClientErrorShape[] => {
  const nodes: ClientErrorShape[] = []
  const visit = (node: unknown, depth: number): void => {
    if (typeof node !== 'object' || node === null || depth > 8) return
    const shaped = node as ClientErrorShape
    nodes.push(shaped)
    if (Array.isArray(shaped.errors)) for (const nested of shaped.errors) visit(nested, depth + 1)
    if (shaped.cause !== undefined) visit(shaped.cause, depth + 1)
  }
  visit(error, 0)
  return nodes
}

/**
 * Failures that will not go away by trying again: a broker answer Kafka
 * itself marks as not retriable (authorization, an oversized record, an
 * invalid request), an authentication failure, an argument the client
 * refused, a codec or an API the client does not have. Everything else
 * (transport, timeouts, leadership changes, metadata still propagating) is
 * worth the producer's retry ladder.
 */
const DEFINITIVE_CODES = new Set([
  'PLT_KFK_AUTHENTICATION',
  'PLT_KFK_USER',
  'PLT_KFK_UNSUPPORTED',
  'PLT_KFK_UNSUPPORTED_API',
  'PLT_KFK_UNSUPPORTED_COMPRESSION',
  'PLT_KFK_UNSUPPORTED_FORMAT',
  'PLT_KFK_OUT_OF_BOUNDS',
  'PLT_KFK_UNEXPECTED_CORRELATION_ID'
])
const isDefinitive = (node: ClientErrorShape): boolean =>
  (typeof node.code === 'string' && DEFINITIVE_CODES.has(node.code)) || (node.code === 'PLT_KFK_PROTOCOL' && node.canRetry === false)
const isTransient = (error: unknown): boolean => !errorNodes(error).some(isDefinitive)

const wrap = (error: unknown, what: string): AdapterError =>
  new AdapterError(`${what}: ${describeError(error)}`, { cause: error, retryable: isTransient(error) })

/** Whether the broker refused the producer because another one took its transactional id since (a newer epoch). */
const isFenced = (error: unknown): boolean =>
  errorNodes(error).some((node) => node.apiId === 'PRODUCER_FENCED' || node.apiId === 'INVALID_PRODUCER_EPOCH')

/** The pause between two attempts to begin a transaction while the coordinator is not ready (a fresh cluster creating its log). */
const TRANSACTION_RETRY_DELAY_MS = 250

/** Waits `ms`, or less when `signal` aborts. */
const pause = async (ms: number, signal: AbortSignal): Promise<void> => {
  await new Promise<void>((resolve) => {
    const done = (): void => {
      clearTimeout(timer)
      signal.removeEventListener('abort', done)
      resolve()
    }
    const timer = setTimeout(done, ms)
    signal.addEventListener('abort', done, { once: true })
  })
}

/** Whether every broker answer in the tree says the metadata the request was built on is stale: a leader moved, a partition is between leaders. */
const isStaleMetadata = (error: unknown): boolean => {
  const answers = errorNodes(error).filter((node) => node.code === 'PLT_KFK_PROTOCOL')
  return answers.length > 0 && answers.every((node) => (node as { hasStaleMetadata?: unknown }).hasStaleMetadata === true)
}

/** Whether the only thing the broker refused was creating a topic that is already there. */
const isAlreadyExists = (error: unknown): boolean => {
  const answers = errorNodes(error).filter((node) => node.code === 'PLT_KFK_PROTOCOL')
  return answers.length > 0 && answers.every((node) => node.apiId === 'TOPIC_ALREADY_EXISTS')
}

const toRaw = (message: ClientMessage): RawMessage => {
  const headers: Record<string, Bytes | Bytes[]> = {}
  for (const [name, value] of message.headerEntries) {
    const key = name.toString('utf8')
    const existing = headers[key]
    if (existing === undefined) headers[key] = value
    else if (Array.isArray(existing)) existing.push(value)
    else headers[key] = [existing, value]
  }
  // A record without a timestamp reads as zero; the wire has no negative time.
  const timestamp = Number(message.timestamp)
  return {
    topic: message.topic,
    partition: message.partition,
    offset: message.offset.toString(),
    key: message.key ?? null,
    value: message.value ?? null,
    headers,
    timestamp: timestamp > 0 ? timestamp : Date.now()
  }
}

const toClientMessage = (record: RawRecord): { topic: string, key?: Bytes, value?: Bytes, headers: Map<Bytes, Bytes>, partition?: number } => ({
  topic: record.topic,
  // The client writes an absent key or value as null on the wire: a tombstone stays a tombstone.
  ...(record.key !== null && { key: record.key }),
  ...(record.value !== null && { value: record.value }),
  headers: new Map(Object.entries(record.headers).map(([name, value]) => [Buffer.from(name, 'utf8'), Buffer.from(value, 'utf8')])),
  ...(record.partition !== undefined && { partition: record.partition })
})

const baseOptions = (config: BrokerConfig): Pick<BaseOptions, 'clientId' | 'bootstrapBrokers' | 'tls' | 'sasl'> => ({
  clientId: config.clientId,
  bootstrapBrokers: [...config.brokers],
  ...(config.ssl === true && { tls: {} }),
  ...(config.sasl !== undefined && {
    sasl: { mechanism: SASL_MECHANISMS[config.sasl.mechanism], username: config.sasl.username, password: config.sasl.password }
  })
})

const refuse = (sources: ReadonlyArray<Record<string, unknown> | undefined>, keys: readonly string[], why: string): void => {
  for (const source of sources) {
    if (source === undefined) continue
    for (const key of keys) {
      if (key in source) throw new ConfigError(`platformaticAdapter: "${key}" ${why} and cannot be overridden`)
    }
  }
}

interface ConsumptionSettings {
  readonly bufferedMessages: number
  readonly reconnectDelayMs: number
}

/**
 * One `consume()` call: a client consumer of its own (one group member per
 * call, as the Confluent adapter does), its message stream, and the
 * per-partition gate in front of `eachMessage`.
 */
class Consumption {
  /** The client consumer behind this consumption: what a transaction commits offsets through. */
  get client (): ClientConsumer {
    return this.consumer
  }

  private readonly queues = new Map<string, ClientMessage[]>()
  /**
   * Partitions with a message waiting for a worker slot, in the order they
   * became ready. A slot takes one message and goes back to the end of the
   * line, so a partition with a long queue never starves the others.
   */
  private readonly ready: string[] = []
  private readonly waiting = new Set<string>()
  private readonly running = new Set<string>()
  private readonly paused = new Set<string>()
  /** The last offset queued per partition: a message the stream fetches again after a rebalance is not delivered twice in one consumption. */
  private readonly lastQueued = new Map<string, bigint>()
  /** The leader epoch of the last delivered record per partition, what OffsetCommit wants next to the offset. */
  private readonly epochs = new Map<string, number>()
  /** What this member owned at the last join, and what it lost since the stream was opened. */
  private assigned = new Set<string>()
  private readonly lost = new Set<string>()
  private readonly concurrency: number
  private active = 0
  private queued = 0
  private roomWaiters: Array<() => void> = []
  private wake: (() => void) | undefined
  private stopped = false
  private rotating = false
  private stream: ClientStream | undefined
  private loop: Promise<void> = Promise.resolve()

  constructor (
    private readonly consumer: ClientConsumer,
    private readonly options: ConsumeOptions,
    private readonly streamOptions: StreamOptions,
    private readonly settings: ConsumptionSettings
  ) {
    this.concurrency = options.concurrency ?? 1
    // The client emits `error` when a rejoin gave up; unheard, an
    // EventEmitter error brings the process down. The membership went with
    // it, and a stream that keeps fetching on the last assignment would be
    // a zombie: it is dropped, and reopening it joins the group again.
    consumer.on('error', (error) => this.dropStream(error))
    consumer.on('consumer:heartbeat:error', ({ error }) => this.report(error, 'heartbeat failed'))
    consumer.on('consumer:group:join', () => this.joined())
  }

  async start (): Promise<void> {
    this.stream = await this.open()
    this.assigned = this.assignedNow()
    this.loop = this.run(this.stream)
  }

  async commit (offsets: readonly TopicPartitionOffset[]): Promise<void> {
    try {
      await this.consumer.commit({
        offsets: offsets.map(({ topic, partition, offset }) => ({
          topic,
          partition,
          offset: BigInt(offset),
          leaderEpoch: this.epochs.get(partitionKey(topic, partition)) ?? -1
        }))
      })
    } catch (error) {
      throw wrap(error, 'commit failed')
    }
  }

  pause (partitions: readonly TopicPartition[]): void {
    for (const { topic, partition } of partitions) this.paused.add(partitionKey(topic, partition))
  }

  resume (partitions: readonly TopicPartition[]): void {
    for (const { topic, partition } of partitions) {
      const key = partitionKey(topic, partition)
      this.paused.delete(key)
      this.markReady(key)
    }
    this.dispatch()
  }

  async stop (): Promise<void> {
    this.stopped = true
    this.wake?.()
    this.release()
    const stream = this.stream
    this.stream = undefined
    let failure: unknown
    try {
      // Closes the stream, which ends the read loop, then leaves the group.
      await closeConsumer(this.consumer)
    } catch (error) {
      failure = error
      // The loop must end whatever the client managed to close.
      stream?.destroy()
    }
    await this.loop
    this.queues.clear()
    this.ready.length = 0
    this.waiting.clear()
    this.queued = 0
    if (failure !== undefined) throw wrap(failure, 'consumer disconnect failed')
  }

  /** The client writes defaults into the options it is given; a copy per stream keeps them the adapter's. */
  private async open (): Promise<ClientStream> {
    return await this.consumer.consume({ ...this.streamOptions })
  }

  /**
   * Reads the stream for as long as the consumption lives. A stream the
   * client gives up on (a fetch that failed past the client's own retries)
   * is reported and replaced after a wait; a stream the adapter replaced on
   * purpose (see `joined`) is reopened at once. The group membership is the
   * consumer's, not the stream's, so a new stream carries on where the
   * committed offsets say.
   */
  private async run (first: ClientStream): Promise<void> {
    let stream: ClientStream | undefined = first
    while (!this.stopped) {
      if (stream === undefined) {
        try {
          stream = await this.open()
        } catch (error) {
          if (this.stopped) return
          this.report(error, 'consume failed')
          await this.backoff()
          continue
        }
        this.stream = stream
        this.assigned = this.assignedNow()
        this.lost.clear()
      }
      const failure = await this.pump(stream)
      // The stream is dead from here, whoever ended it: a client error or a
      // regained partition meanwhile has no stream to drop or rotate.
      this.stream = undefined
      stream = undefined
      if (this.stopped) return
      if (this.rotating) {
        this.rotating = false
        continue
      }
      this.report(failure ?? new AdapterError('the client closed the message stream'), 'consume failed')
      await this.backoff()
    }
  }

  private async pump (stream: ClientStream): Promise<unknown> {
    try {
      for await (const message of stream) {
        if (this.queued >= this.settings.bufferedMessages) await this.room()
        if (this.stopped || this.stream !== stream) return undefined
        this.enqueue(message)
      }
      return undefined
    } catch (error) {
      return error
    }
  }

  private enqueue (message: ClientMessage): void {
    const key = partitionKey(message.topic, message.partition)
    if (!this.isAssigned(message.topic, message.partition)) return
    const last = this.lastQueued.get(key)
    if (last !== undefined && message.offset <= last) return
    this.lastQueued.set(key, message.offset)
    let queue = this.queues.get(key)
    if (queue === undefined) {
      queue = []
      this.queues.set(key, queue)
    }
    queue.push(message)
    this.queued++
    this.markReady(key)
    this.dispatch()
  }

  /**
   * Whether this member still owns the partition. The client replaces its
   * assignments when a rebalance completes; until then the old owner keeps
   * delivering, as it does under any client, and afterwards what it still
   * holds for a lost partition is skipped rather than processed and
   * committed over the new owner's work.
   */
  private isAssigned (topic: string, partition: number): boolean {
    const assignments = this.consumer.assignments
    if (assignments === null) return true
    return assignments.some((assignment) => assignment.topic === topic && assignment.partitions.includes(partition))
  }

  private assignedNow (): Set<string> {
    return new Set((this.consumer.assignments ?? []).flatMap(({ topic, partitions }) => partitions.map((partition) => partitionKey(topic, partition))))
  }

  /**
   * The group was joined again, so the assignment may have changed. What was
   * queued for a partition this member lost is dropped: processing it would
   * commit over the new owner's work. The client keeps what it had already
   * fetched in the stream's buffer and rewinds a kept partition to its
   * committed offset, so a partition that comes BACK cannot be read from the
   * old stream: its stale buffer would be delivered ahead of the refetch,
   * and with nothing committed there would be no refetch at all. A regained
   * partition gets a new stream, which starts from what is committed (or
   * from `fromBeginning`) for everything this member owns now.
   */
  private joined (): void {
    const now = this.assignedNow()
    const held = new Set([...this.assigned, ...this.queues.keys(), ...this.lastQueued.keys(), ...this.epochs.keys(), ...this.paused])
    const lostNow: TopicPartition[] = []
    for (const key of held) {
      if (now.has(key)) continue
      this.lost.add(key)
      this.discard(key, true)
      lostNow.push(splitPartitionKey(key))
    }
    // The client released these already, so the core cannot be given time
    // to finish on them; it is told after the fact, which lets it drop what
    // it holds for them (a retry message parked in memory, for one) instead
    // of finishing it on the next owner's partition.
    if (lostNow.length > 0) this.options.onPartitionsRevoked?.(lostNow).catch((error: unknown) => this.report(error, 'revocation failed'))
    let regained = false
    for (const key of this.lost) {
      if (!now.has(key)) continue
      this.lost.delete(key)
      regained = true
    }
    this.assigned = now
    if (regained) this.rotate()
    this.release()
  }

  /** Replaces the stream: the queues go, the stream is closed, and the read loop opens a fresh one at once. Deliveries in flight finish on their own. */
  private rotate (): void {
    const stream = this.stream
    if (stream === undefined || this.stopped) return
    for (const key of [...this.queues.keys()]) this.discard(key, false)
    // The new stream starts from the committed offsets: what was queued and
    // dropped comes again, and must not be taken for a repeat.
    this.lastQueued.clear()
    this.lost.clear()
    this.rotating = true
    this.stream = undefined
    stream.close().catch(() => undefined)
  }

  private dropStream (error: unknown): void {
    const stream = this.stream
    if (stream === undefined || this.stopped) {
      this.report(error, 'consumer failed')
      return
    }
    this.stream = undefined
    stream.destroy(error instanceof Error ? error : new Error(describeError(error)))
  }

  /** Forgets a partition's queue; with `forget`, its bookkeeping as well. */
  private discard (key: string, forget: boolean): void {
    const queue = this.queues.get(key)
    if (queue !== undefined) {
      this.queued -= queue.length
      this.queues.delete(key)
    }
    if (this.waiting.delete(key)) this.ready.splice(this.ready.indexOf(key), 1)
    if (forget) {
      this.lastQueued.delete(key)
      this.epochs.delete(key)
      // A pause belongs to the assignment: the partition comes back unpaused.
      this.paused.delete(key)
    }
  }

  private markReady (key: string): void {
    if (this.waiting.has(key) || this.running.has(key) || this.paused.has(key)) return
    if ((this.queues.get(key)?.length ?? 0) === 0) return
    this.waiting.add(key)
    this.ready.push(key)
  }

  /**
   * Every free slot takes the partition that has been waiting longest. A
   * partition lines up with a message queued and neither running nor paused
   * (`markReady`), and leaves the line with its queue (`discard`); only a
   * pause can come between the two.
   */
  private dispatch (): void {
    while (!this.stopped && this.active < this.concurrency && this.ready.length > 0) {
      const key = this.ready.shift() as string
      this.waiting.delete(key)
      if (this.paused.has(key)) continue
      const message = (this.queues.get(key) as ClientMessage[]).shift() as ClientMessage
      this.queued--
      this.running.add(key)
      this.active++
      this.deliver(key, message).catch(() => undefined)
    }
  }

  private async deliver (key: string, message: ClientMessage): Promise<void> {
    try {
      this.release()
      if (!this.isAssigned(message.topic, message.partition)) {
        this.lastQueued.delete(key)
        this.epochs.delete(key)
        return
      }
      this.epochs.set(key, message.leaderEpoch)
      try {
        await this.options.eachMessage(toRaw(message))
      } catch (failure) {
        // A rejected delivery is settled like any other (the contract):
        // reported, and the partition moves on.
        this.options.onError?.(failure)
      }
    } finally {
      this.running.delete(key)
      this.active--
      if (!this.stopped) {
        this.markReady(key)
        this.dispatch()
      }
    }
  }

  private room (): Promise<void> {
    return new Promise((resolve) => this.roomWaiters.push(resolve))
  }

  private release (): void {
    if (this.roomWaiters.length === 0) return
    if (this.queued >= this.settings.bufferedMessages && !this.stopped) return
    const waiters = this.roomWaiters
    this.roomWaiters = []
    for (const resolve of waiters) resolve()
  }

  private backoff (): Promise<void> {
    if (this.stopped) return Promise.resolve()
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.wake = undefined
        resolve()
      }, this.settings.reconnectDelayMs)
      this.wake = () => {
        clearTimeout(timer)
        this.wake = undefined
        resolve()
      }
    })
  }

  private report (error: unknown, what: string): void {
    if (this.stopped) return
    this.options.onError?.(error instanceof AdapterError ? error : wrap(error, what))
  }
}

/** The client as installed next to the application; a `client` option replaces it. */
async function loadInstalledClient (): Promise<PlatformaticClientModule> {
  // The unit suite loads the installed client through here; V8 still reports
  // a branch of its own inside the dynamic import, which no test can take.
  /* c8 ignore next */
  const installed: unknown = await import('@platformatic/kafka')
  return installed as PlatformaticClientModule
}

export function platformaticAdapter (options: PlatformaticAdapterOptions = {}): ClientAdapter {
  refuse([options.global, options.producer, options.consumer, options.admin], RESERVED_CLIENT_KEYS, 'comes from the harbor configuration')
  refuse([options.global, options.consumer], RESERVED_CONSUMER_KEYS, 'is managed by the adapter (the core commits after the handler and reads bytes)')
  refuse([options.global, options.producer], RESERVED_PRODUCER_KEYS, 'is managed by the adapter (every produce waits for all in-sync replicas and is idempotent; the core commits only after that)')

  const adminTimeout = parseDuration(options.adminTimeoutMs ?? 30_000, 'platformaticAdapter adminTimeoutMs')
  const settings: ConsumptionSettings = {
    bufferedMessages: options.bufferedMessages ?? 1000,
    reconnectDelayMs: parseDuration(options.reconnectDelayMs ?? 1000, 'platformaticAdapter reconnectDelayMs')
  }
  for (const [name, value] of Object.entries({ adminTimeoutMs: adminTimeout, ...settings })) {
    if (!Number.isInteger(value) || value < 1) throw new ConfigError(`platformaticAdapter: ${name} must be a positive integer, got ${String(value)}`)
  }
  // Named after the option it came from, so a refusal points at what was set.
  const [timeoutOption, timeoutValue] = options.transactionTimeout !== undefined
    ? ['transactionTimeout', options.transactionTimeout]
    : options.producer?.timeout !== undefined
      ? ['producer.timeout', options.producer.timeout]
      : options.global?.timeout !== undefined ? ['global.timeout', options.global.timeout] : ['transactionTimeout', DEFAULT_TRANSACTION_TIMEOUT]
  const transactionTimeoutMs = parseDuration(timeoutValue, `platformaticAdapter ${timeoutOption}`)
  if (transactionTimeoutMs < 1) throw new ConfigError(`platformaticAdapter: ${timeoutOption}, the transaction timeout, must be positive`)
  // An idempotent producer with fewer than two retries (or `true`, or none
  // at all) is one the client retries forever: a produce on a partition that
  // is gone would never return. What counts is what the spread hands it.
  const producerRetries = { retries: DEFAULT_PRODUCER_RETRIES, ...options.global, ...options.producer }.retries
  if (producerRetries === undefined || producerRetries === true || (typeof producerRetries === 'number' && producerRetries < 2)) {
    throw new ConfigError(`platformaticAdapter: producer retries of ${String(producerRetries)} make the client retry forever (the producer is idempotent); use 2 or more, or false for none`)
  }
  /**
   * The open connection. `closed` aborts when it is released, so work still
   * reopening a producer on it gives up; `fenced` is set once another
   * producer took the transactional id, and refuses every transaction from
   * then on.
   */
  let state: { module: PlatformaticClientModule, config: BrokerConfig, producer: ClientProducer, transactional: ClientProducer | undefined, admin: Admin, closed: AbortController, fenced: boolean } | undefined
  let connecting: Promise<void> | undefined
  /** Aborts when the adapter disconnects while a connection is being opened. */
  let opening: AbortController | undefined
  const consumptions = new Set<Consumption>()
  /** The consumption behind each handle, which a transaction needs to commit its offsets. */
  const consumptionsByHandle = new WeakMap<ConsumerHandle, Consumption>()

  const loadClient = async (): Promise<PlatformaticClientModule> => {
    try {
      if (typeof options.client === 'function') return await options.client()
      return options.client ?? await loadInstalledClient()
    } catch (cause) {
      throw new AdapterError('@platformatic/kafka could not be loaded; add it as a dependency to use platformaticAdapter()', { cause, retryable: false })
    }
  }

  /**
   * The transactional producer; its request timeout is what the broker
   * gives a transaction. It introduces itself as a new producer for its id
   * (-1, -1): the broker then fences whoever held the id before (this
   * process before a restart, or a producer this adapter gave up on), aborts
   * what that one left open, and bumps the epoch. The client's default
   * (0, 0) is answered as fenced instead. Introducing itself with the
   * identity of a producer given up on would not help tell a timeout from a
   * fence: the broker refuses that identity after either.
   */
  const openTransactional = (module: PlatformaticClientModule, config: BrokerConfig, transactionalId: string): ClientProducer =>
    new module.Producer({
      ...baseOptions(config),
      retries: DEFAULT_PRODUCER_RETRIES,
      ...options.global,
      ...options.producer,
      timeout: transactionTimeoutMs,
      acks: -1,
      idempotent: true,
      transactionalId,
      producerId: -1n,
      producerEpoch: -1
    })

  const requireConnected = (): NonNullable<typeof state> => {
    if (state === undefined) throw new AdapterError('platformatic adapter is not connected', { retryable: false })
    return state
  }

  const listTopics = async (admin: Admin): Promise<string[]> => {
    try {
      return await admin.listTopics()
    } catch (error) {
      throw wrap(error, 'listTopics failed')
    }
  }

  /** The partitions of each topic that have a leader serving; the others cannot answer an offsets request yet. */
  const partitionsOf = async (admin: Admin, topics: readonly string[]): Promise<Map<string, number[]>> => {
    let metadata
    try {
      // Fresh, not the client's cache: a partition between leaders answers on the next call, not five seconds later.
      metadata = await admin.metadata({ topics: [...topics], forceUpdate: true })
    } catch (error) {
      throw wrap(error, 'metadata failed')
    }

    const result = new Map<string, number[]>()

    for (const topic of topics) {
      const partitions = metadata.topics.get(topic)?.partitions ?? []
      result.set(topic, partitions.flatMap((entry, index) => entry.leader >= 0 ? [index] : []))
    }

    return result
  }

  /** Opens the clients of a connection; `connect` runs one at a time. */
  const open = async (config: BrokerConfig): Promise<void> => {
    const connection = new AbortController()
    opening = connection
    const module = await loadClient()
    if (connection.signal.aborted) throw new AdapterError('platformatic adapter disconnected while connecting', { retryable: false })
    const base = baseOptions(config)
    const producer = new module.Producer({
      ...base,
      retries: DEFAULT_PRODUCER_RETRIES,
      ...options.global,
      ...options.producer,
      // Never lose or duplicate by default: every produce waits for all
      // in-sync replicas, and the idempotent producer de-duplicates the
      // client's own retries.
      acks: -1,
      idempotent: true
    })

    // Transactions need a producer of their own: a transactional producer
    // holds one open transaction and every send on it is part of it.
    const transactional = config.transactionalId === undefined ? undefined : openTransactional(module, config, config.transactionalId)
    const admin = new module.Admin({ ...base, ...options.global, ...options.admin })

    try {
      // The client connects on first use; asking for the brokers now makes
      // a wrong address or a refused credential fail here, not on the first
      // produce.
      await admin.connectToBrokers()
      await producer.connectToBrokers()
      await transactional?.connectToBrokers()
    } catch (error) {
      await Promise.allSettled([producer.close(), transactional?.close(), admin.close()])
      throw wrap(error, 'connect failed')
    }
    // Released while it was being opened: nobody would close the clients.
    if (connection.signal.aborted) {
      await Promise.allSettled([producer.close(), transactional?.close(), admin.close()])
      throw new AdapterError('platformatic adapter disconnected while connecting', { retryable: false })
    }

    state = { module, config, producer, transactional, admin, closed: connection, fenced: false }
  }

  const adapter: ClientAdapter = {
    name: 'platformatic',

    async connect (config: BrokerConfig) {
      if (state !== undefined) return
      connecting ??= open(config).finally(() => {
        connecting = undefined
        opening = undefined
      })
      await connecting
    },

    async disconnect () {
      opening?.abort()
      const pending: Array<Promise<unknown>> = []
      for (const consumption of consumptions) pending.push(consumption.stop())
      consumptions.clear()
      if (state !== undefined) {
        state.closed.abort()
        pending.push(state.producer.close(), state.admin.close(), ...(state.transactional === undefined ? [] : [state.transactional.close()]))
      }
      state = undefined
      const results = await Promise.allSettled(pending)
      const failed = results.find((result): result is PromiseRejectedResult => result.status === 'rejected')
      if (failed !== undefined) throw wrap(failed.reason, 'disconnect failed')
    },

    async produce (records: readonly RawRecord[]) {
      const { producer } = requireConnected()
      try {
        // With acks=all the client resolves only once every record was
        // acknowledged, and rejects otherwise; the batch is all or nothing.
        await producer.send({ messages: records.map(toClientMessage) })
      } catch (error) {
        throw wrap(error, 'produce failed')
      }
    },

    async transaction (): Promise<TransactionHandle> {
      const current = requireConnected()
      const { module, config } = current
      const transactionalId = config.transactionalId
      if (transactionalId === undefined) throw new ConfigError('platformaticAdapter: transactions need transactionalId on the harbor configuration')
      const deadline = Date.now() + adminTimeout
      let transactional: ClientProducer
      let tx: Awaited<ReturnType<ClientProducer['beginTransaction']>>
      for (;;) {
        // Another producer took the id: opening one again would fence it
        // back, the zombie fencing the live one.
        if (current.fenced) throw new FencedError(transactionalId)
        // A producer given up on (an abort that failed, a transaction the
        // broker aborted) is opened again here, and its initialization
        // fences and aborts the old one.
        const reopened = current.transactional ?? openTransactional(module, config, transactionalId)
        current.transactional = reopened
        transactional = reopened
        try {
          tx = await transactional.beginTransaction()
          break
        } catch (error) {
          // The client keeps a begin it could not finish as its active
          // transaction: this producer is done either way.
          if (current.transactional === transactional) current.transactional = undefined
          await transactional.close().catch(() => undefined)
          // Refused at begin: another producer holds the id now.
          if (isFenced(error)) {
            current.fenced = true
            throw new FencedError(transactionalId, { cause: error })
          }
          const failure = wrap(error, 'transaction begin failed')
          // The coordinator of a fresh cluster creating its log, say: tried
          // again on a new producer until adminTimeoutMs.
          if (!failure.retryable || Date.now() >= deadline || current.closed.signal.aborted) throw failure
          await pause(TRANSACTION_RETRY_DELAY_MS, current.closed.signal)
          if (current.closed.signal.aborted) throw new AdapterError('platformatic adapter is not connected', { retryable: false })
        }
      }
      // A monotonic clock: a wall clock stepped by NTP would move a fence
      // across the timeout.
      const began = performance.now()
      let dropped = false
      /**
       * Gives the producer up: the next transaction opens a new one, whose
       * initialization aborts what the broker may still hold open under
       * this epoch.
       */
      const drop = async (): Promise<void> => {
        dropped = true
        if (current.transactional === transactional) current.transactional = undefined
        await transactional.close().catch(() => undefined)
      }
      /**
       * What a failed step of the transaction throws. A newer epoch on the
       * id is either another producer that took it, or the broker aborting
       * this transaction once `transactionTimeout` elapsed, which the broker
       * answers the same way: before the timeout it can only be the first.
       */
      const failed = async (error: unknown, what: string): Promise<Error> => {
        if (!isFenced(error)) return wrap(error, what)
        // Measured before the producer closes, which may wait on requests
        // in flight: a fence must not age into a timeout while it does.
        const elapsed = performance.now() - began
        await drop()
        if (elapsed < transactionTimeoutMs) {
          current.fenced = true
          return new FencedError(transactionalId, { cause: error })
        }
        return new AdapterError(`${what}: the broker aborted the transaction (open past transactionTimeout, or the id was taken): ${describeError(error)}`, { cause: error, retryable: true })
      }
      return {
        async produce (records) {
          try {
            await tx.send({ messages: records.map(toClientMessage) })
          } catch (error) {
            throw await failed(error, 'produce failed')
          }
        },
        async sendOffsets (consumption, offsets) {
          const owner = consumptionsByHandle.get(consumption)
          if (owner === undefined) throw new ConfigError('platformaticAdapter: the offsets belong to a consumption this adapter did not open')
          const consumer = owner.client
          try {
            // The client commits the offset AFTER a consumed message, from
            // the message and the group metadata it carries; the contract
            // hands the next offset to read, so the message stands one back.
            await tx.addConsumer(consumer)
            for (const { topic, partition, offset } of offsets) {
              await tx.addOffset({
                topic,
                partition,
                offset: BigInt(offset) - 1n,
                metadata: { consumer: { groupId: consumer.groupId, generationId: consumer.generationId, memberId: consumer.memberId, coordinatorId: consumer.coordinatorId } }
              } as unknown as ClientMessage)
            }
          } catch (error) {
            throw await failed(error, 'sendOffsets failed')
          }
        },
        async commit () {
          try {
            await tx.commit()
          } catch (error) {
            throw await failed(error, 'transaction commit failed')
          }
        },
        async abort () {
          // Given up on already: the next producer's initialization aborts
          // what the broker still holds.
          if (dropped) return
          try {
            await tx.abort()
          } catch (error) {
            // The broker may still hold the transaction open under this
            // producer's epoch: forgetting it on the client's side alone
            // would let the next transaction append to it, and its commit
            // make what was aborted visible.
            const failure = await failed(error, 'transaction abort failed')
            if (!dropped) await drop()
            throw failure
          }
        }
      }
    },

    async consume (consumeOptions: ConsumeOptions): Promise<ConsumerHandle> {
      const { module, config } = requireConnected()
      const partitionAssigner = options.consumer?.partitionAssigner ?? partitionAssignerBySubscription
      const consumer = new module.Consumer({
        ...baseOptions(config),
        ...options.global,
        ...options.consumer,
        // On the consumer as well as on the stream: a rejoin the client
        // starts on its own (a commit refused during a rebalance) reads the
        // consumer's options, and would otherwise assign round-robin over
        // every member whatever each subscribed to.
        partitionAssigner,
        groupId: consumeOptions.groupId,
        // The core commits after the handler finished; the client must
        // never commit on its own.
        autocommit: false
      })
      const consumption = new Consumption(consumer, consumeOptions, {
        topics: [...consumeOptions.topics],
        autocommit: false,
        // The join happens on the first consume, with these options.
        partitionAssigner,
        // Start where the group committed; a group that never did starts
        // where fromBeginning says.
        mode: 'committed',
        fallbackMode: consumeOptions.fromBeginning === true ? 'earliest' : 'latest'
      }, settings)

      try {
        await consumption.start()
      } catch (error) {
        await closeConsumer(consumer).catch(() => undefined)
        throw wrap(error, 'consume failed')
      }

      consumptions.add(consumption)

      const handle: ConsumerHandle = {
        commit: async (offsets) => await consumption.commit(offsets),
        stop: async () => {
          consumptions.delete(consumption)
          await consumption.stop()
        },
        pause: (partitions) => consumption.pause(partitions),
        resume: (partitions) => consumption.resume(partitions)
      }
      consumptionsByHandle.set(handle, consumption)
      return handle
    },

    admin: {
      async createTopics (specs: readonly TopicSpec[]) {
        const { admin } = requireConnected()
        const deadline = Date.now() + adminTimeout

        // The client takes one set of configs per call; a call per spec
        // keeps each topic's configs its own.
        for (const spec of specs) {
          try {
            await admin.createTopics({
              // Left out, the client asks for the broker's defaults (-1).
              topics: [{ topic: spec.topic, partitions: spec.partitions, replicas: spec.replicationFactor }],
              ...(spec.config !== undefined && { configs: Object.entries(spec.config).map(([name, value]) => ({ name, value })) })
            })
          } catch (error) {
            // An existing topic is the outcome this method promises, not an error.
            if (!isAlreadyExists(error)) throw wrap(error, 'createTopics failed')
          }
        }
        // The controller accepts the creation before every broker serves the
        // new metadata; a consumer subscribing right after must find the
        // topics, so wait until they are visible, bounded by the timeout.
        const wanted = specs.map((spec) => spec.topic)
        let delay = 50
        for (;;) {
          const visible = new Set(await listTopics(admin))
          const missing = wanted.filter((topic) => !visible.has(topic))

          if (missing.length === 0) return
          if (Date.now() + delay >= deadline) {
            throw new AdapterError(`createTopics: ${missing.join(', ')} not visible in metadata after ${adminTimeout}ms`)
          }

          await new Promise((resolve) => setTimeout(resolve, delay))
          delay = nextPollDelay(delay)
        }
      },
      async topicExists (topic: string) {
        const { admin } = requireConnected()
        return (await listTopics(admin)).includes(topic)
      },
      async fetchTopicOffsets (topics: readonly string[]): Promise<PartitionOffsets[]> {
        const { admin } = requireConnected()
        const partitions = await partitionsOf(admin, topics)
        const request = (timestamp: bigint) => ({
          topics: [...partitions].flatMap(([name, indexes]) => indexes.length === 0
            ? []
            : [{ name, partitions: indexes.map((partitionIndex) => ({ partitionIndex, timestamp })) }])
        })

        if (request(-1n).topics.length === 0) return []
        let high, low
        try {
          // -1 asks for the high watermark, -2 for the first offset still held.
          ;[high, low] = await Promise.all([admin.listOffsets(request(-1n)), admin.listOffsets(request(-2n))])
        } catch (error) {
          // A leader that moved between the metadata and the request fails
          // the whole call in the client; the contract lets a partition be
          // left out, and a scrape must not fail over an election.
          if (isStaleMetadata(error)) return []
          throw wrap(error, 'listOffsets failed')
        }

        const lows = new Map(low.flatMap((topic) => topic.partitions.map((entry) => [partitionKey(topic.name, entry.partitionIndex), entry.offset] as const)))

        return high.flatMap((topic) => topic.partitions.flatMap((entry) => {
          const first = lows.get(partitionKey(topic.name, entry.partitionIndex))
          return first === undefined ? [] : [{ topic: topic.name, partition: entry.partitionIndex, low: first.toString(), high: entry.offset.toString() }]
        }))
      },
      async fetchCommittedOffsets (groupId: string, topics: readonly string[]): Promise<CommittedOffset[]> {
        const { admin } = requireConnected()
        const partitions = await partitionsOf(admin, topics)

        try {
          const groups = await admin.listConsumerGroupOffsets({
            groups: [{ groupId, topics: [...partitions].map(([name, partitionIndexes]) => ({ name, partitionIndexes })) }]
          })
          // The broker reports a partition the group never committed on with -1.
          return groups.flatMap((group) => group.topics.flatMap((topic) => topic.partitions.map((entry) => ({
            topic: topic.name,
            partition: entry.partitionIndex,
            offset: entry.committedOffset >= 0n ? entry.committedOffset.toString() : null
          }))))
        } catch (error) {
          throw wrap(error, 'listConsumerGroupOffsets failed')
        }
      }
    }
  }
  return adapter
}

/**
 * ClientAdapter over @confluentinc/kafka-javascript, through its promisified
 * KafkaJS-compatible API. The client is a peer dependency loaded on
 * `connect()`, never at import time: an application that only uses the core
 * (or another adapter) never touches the native binding.
 */
import {
  AdapterError,
  OFFSET_PATTERN,
  ConfigError,
  FencedError,
  describeError,
  parseDuration,
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
import type { KafkaJS } from '@confluentinc/kafka-javascript'

/** The part of the client module the adapter uses; injectable for tests. */
export interface ConfluentClientModule {
  KafkaJS: {
    Kafka: new (config?: KafkaJS.CommonConstructorConfig) => KafkaJS.Kafka
  }
}

/** librdkafka properties passed straight through to the client. */
export type LibrdkafkaConfig = Record<string, string | number | boolean>

export interface ConfluentAdapterOptions {
  /**
   * The client module. Default: `import('@confluentinc/kafka-javascript')`
   * on first connect. Inject a fake to unit-test the adapter without the
   * native binding.
   */
  client?: ConfluentClientModule | (() => Promise<ConfluentClientModule>)
  /** Extra librdkafka properties for every client (producer, consumer, admin). */
  global?: LibrdkafkaConfig
  /** Extra librdkafka properties for the producer. */
  producer?: LibrdkafkaConfig
  /** Extra librdkafka properties for every consumer. */
  consumer?: LibrdkafkaConfig
  /** Timeout for Admin API calls, and how long a transactional producer's initialization is tried. A duration (`'30s'`) or milliseconds. Default: '30s'. */
  adminTimeoutMs?: Duration
  /** The client's own log level (KafkaJS.logLevel: 0 nothing, 1 error, 2 warn, 3 info, 4 debug). Default: 1. */
  logLevel?: 0 | 1 | 2 | 3 | 4
}

/** librdkafka codes surfaced through KafkaJSError.code. */
const ERR__ASSIGN_PARTITIONS = -175
const ERR__REVOKE_PARTITIONS = -174
const ERR_TOPIC_ALREADY_EXISTS = 36

/**
 * Failures that will not go away by trying again: bad arguments, oversized
 * records, authorization, a topic or partition that does not exist, a policy
 * the broker enforces. Everything else the client reports (transport,
 * timeouts, leadership changes, metadata still propagating) is transient
 * and worth the producer's retry ladder. The client's own `retriable` flag
 * is NOT consulted: it is only meaningful for the transactional producer and
 * defaults to false, which would silently disable every retry.
 */
const DEFINITIVE_CODES = new Set([
  -190, // ERR__UNKNOWN_PARTITION
  -188, // ERR__UNKNOWN_TOPIC
  -186, // ERR__INVALID_ARG
  10, // ERR_MSG_SIZE_TOO_LARGE
  17, // ERR_INVALID_TOPIC_EXCEPTION
  18, // ERR_RECORD_LIST_TOO_LARGE
  29, // ERR_TOPIC_AUTHORIZATION_FAILED
  30, // ERR_GROUP_AUTHORIZATION_FAILED
  31, // ERR_CLUSTER_AUTHORIZATION_FAILED
  32, // ERR_INVALID_TIMESTAMP
  33, // ERR_UNSUPPORTED_SASL_MECHANISM
  35, // ERR_UNSUPPORTED_VERSION
  37, // ERR_INVALID_PARTITIONS
  40, // ERR_INVALID_CONFIG
  42, // ERR_INVALID_REQUEST
  43, // ERR_UNSUPPORTED_FOR_MESSAGE_FORMAT
  44, // ERR_POLICY_VIOLATION
  58, // ERR_SASL_AUTHENTICATION_FAILED
  87, // ERR_INVALID_RECORD
  // A producer the broker fenced, or one that fell out of its own
  // idempotent sequence, is done: retrying it walks the whole ladder for
  // nothing. So are refusals of the client's identity or of the request.
  -150, // ERR__FATAL
  -144, // ERR__FENCED
  -169, // ERR__AUTHENTICATION
  -170, // ERR__NOT_IMPLEMENTED
  12, // ERR_OFFSET_METADATA_TOO_LARGE
  34, // ERR_ILLEGAL_SASL_STATE
  47, // ERR_INVALID_PRODUCER_EPOCH
  48, // ERR_INVALID_TXN_STATE
  49, // ERR_INVALID_PRODUCER_ID_MAPPING
  53, // ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED
  76, // ERR_UNSUPPORTED_COMPRESSION_TYPE
  90 // ERR_PRODUCER_FENCED
])

/** Whether a failure carrying `code` is worth another attempt. No code at all is transient: it was not the broker refusing. */
const isTransient = (code: number | undefined): boolean => code === undefined || !DEFINITIVE_CODES.has(code)

/**
 * librdkafka properties the adapter owns because the core's delivery
 * guarantees depend on them; a passthrough that flipped one would turn the
 * consumer into at-most-once, or let a produce resolve before the record is
 * safe, without anyone noticing.
 */
const RESERVED_CONSUMER_KEYS = ['enable.auto.commit', 'enable.auto.offset.store', 'auto.offset.reset', 'group.id', 'rebalance_cb', 'offset_commit_cb']
const RESERVED_PRODUCER_KEYS = ['acks', 'request.required.acks', 'enable.idempotence', 'transactional.id']

interface RebalanceAssignmentFns {
  assign: (assignment: KafkaJS.TopicPartition[]) => void
  unassign: (assignment: KafkaJS.TopicPartition[]) => void
}

/** The pause between two attempts to initialize the transactional producer while the coordinator is not ready yet. */
const TRANSACTIONAL_RETRY_DELAY_MS = 250

/**
 * A newer epoch on the transactional id: ERR__FENCED (librdkafka's own,
 * which it reports for both of the broker's below), INVALID_PRODUCER_EPOCH,
 * PRODUCER_FENCED. Another producer took the id, or the broker aborted a
 * transaction open past `transaction.timeout.ms`: librdkafka does not tell
 * the two apart.
 */
const FENCING_CODES = new Set([-144, 47, 90])

/** librdkafka's `transaction.timeout.ms` when the configuration leaves it out. */
const DEFAULT_TRANSACTION_TIMEOUT_MS = 60_000

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

const toRaw = (payload: KafkaJS.EachMessagePayload): RawMessage => {
  const { message } = payload
  // The client hands an empty string when the record has no timestamp, and
  // Number('') is 0: a positive check, not a finite one.
  const timestamp = Number(message.timestamp)
  return {
    topic: payload.topic,
    partition: payload.partition,
    offset: message.offset,
    key: message.key ?? null,
    value: message.value ?? null,
    headers: message.headers ?? {},
    timestamp: timestamp > 0 ? timestamp : Date.now()
  }
}

const errorCode = (error: unknown): number | undefined => {
  const code = (error as { code?: unknown } | null)?.code
  return typeof code === 'number' ? code : undefined
}

const wrap = (error: unknown, what: string): AdapterError =>
  new AdapterError(`${what}: ${describeError(error)}`, { cause: error, retryable: isTransient(errorCode(error)) })

/**
 * The client's logger contract, pointed at the adapter's error callback:
 * fetch-loop failures the client would only print become `error` events.
 * The other levels keep the client's own behaviour, printing to the console
 * when `logLevel` lets them through (2 warn, 3 info, 4 debug).
 */
const errorForwardingLogger = (onError: (error: unknown) => void, level: number): KafkaJS.Logger => {
  const logger: KafkaJS.Logger = {
    info: (message, extra) => { if (level >= 3) console.info(message, extra) },
    warn: (message, extra) => { if (level >= 2) console.warn(message, extra) },
    debug: (message, extra) => { if (level >= 4) console.debug(message, extra) },
    error: (message, extra) => {
      if (level >= 1) onError(new AdapterError(`client: ${message}`, { cause: extra }))
    },
    namespace: () => logger,
    setLogLevel: () => {}
  }
  return logger
}

/** The wait between two looks at the metadata while a created topic propagates: 50ms, doubling up to 500ms. */
const nextPollDelay = (previous: number): number => Math.min(previous * 2, 500)

const groupByTopic = (records: readonly RawRecord[]): KafkaJS.TopicMessages[] => {
  const byTopic = new Map<string, KafkaJS.Message[]>()
  for (const record of records) {
    let messages = byTopic.get(record.topic)
    if (messages === undefined) {
      messages = []
      byTopic.set(record.topic, messages)
    }
    messages.push({
      key: record.key,
      value: record.value,
      headers: record.headers,
      ...(record.partition !== undefined && { partition: record.partition })
    })
  }
  return [...byTopic].map(([topic, messages]) => ({ topic, messages }))
}

/** The client as installed next to the application; a `client` option replaces it. */
async function loadInstalledClient (): Promise<ConfluentClientModule> {
  // The unit suite loads the installed client through here; V8 still reports
  // a branch of its own inside the dynamic import, which no test can take.
  /* c8 ignore next */
  const installed: unknown = await import('@confluentinc/kafka-javascript')
  return installed as ConfluentClientModule
}

export function confluentAdapter (options: ConfluentAdapterOptions = {}): ClientAdapter {
  for (const source of [options.global, options.consumer]) {
    for (const key of RESERVED_CONSUMER_KEYS) {
      if (source !== undefined && key in source) {
        throw new ConfigError(`confluentAdapter: "${key}" is managed by the adapter (the core commits after the handler and settles revocations itself; the group and fromBeginning come from the consumer) and cannot be overridden`)
      }
    }
  }
  for (const source of [options.global, options.producer]) {
    for (const key of RESERVED_PRODUCER_KEYS) {
      if (source !== undefined && key in source) {
        throw new ConfigError(`confluentAdapter: "${key}" is managed by the adapter (every produce waits for all in-sync replicas and is idempotent; the core commits only after that) and cannot be overridden`)
      }
    }
  }
  let kafka: KafkaJS.Kafka | undefined
  /**
   * The connection open or being opened. `closed` aborts when it is
   * released, so a transactional producer still being opened on it gives
   * up; `fenced` is set once another producer took the transactional id,
   * and refuses every transaction on it from then on.
   */
  let link = { closed: new AbortController(), fenced: false }
  const configuredTimeout = options.producer?.['transaction.timeout.ms'] ?? options.global?.['transaction.timeout.ms']
  /** How long the broker lets a transaction stay open, which tells a fenced producer from a transaction it aborted. */
  const transactionTimeoutMs = configuredTimeout === undefined ? DEFAULT_TRANSACTION_TIMEOUT_MS : Number(configuredTimeout)
  if (!Number.isFinite(transactionTimeoutMs) || transactionTimeoutMs < 1) throw new ConfigError(`confluentAdapter: transaction.timeout.ms must be a positive number, got ${String(configuredTimeout)}`)
  let connecting: Promise<void> | undefined
  let producer: KafkaJS.Producer | undefined
  /** The transactional producer, opened when the harbor has a transactionalId; its transactions run one at a time. */
  let transactional: KafkaJS.Producer | undefined
  let transactionalId: string | undefined
  let admin: KafkaJS.Admin | undefined
  const consumers = new Set<KafkaJS.Consumer>()
  /** The client consumer behind each consumption, which a transaction needs to commit its offsets. */
  const consumersByHandle = new WeakMap<ConsumerHandle, KafkaJS.Consumer>()
  const adminTimeout = parseDuration(options.adminTimeoutMs ?? 30_000, 'confluentAdapter adminTimeoutMs')

  const loadClient = async (): Promise<ConfluentClientModule> => {
    try {
      if (typeof options.client === 'function') return await options.client()
      return options.client ?? await loadInstalledClient()
    } catch (cause) {
      throw new AdapterError('@confluentinc/kafka-javascript could not be loaded; add it as a dependency to use confluentAdapter()', { cause, retryable: false })
    }
  }

  const requireConnected = (): { kafka: KafkaJS.Kafka, producer: KafkaJS.Producer, admin: KafkaJS.Admin } => {
    if (kafka === undefined || producer === undefined || admin === undefined) {
      throw new AdapterError('confluent adapter is not connected', { retryable: false })
    }
    return { kafka, producer, admin }
  }

  const openTransactional = (instance: KafkaJS.Kafka, id: string): KafkaJS.Producer =>
    instance.producer({ kafkaJS: { acks: -1, idempotent: true, transactionalId: id }, ...options.producer })

  /**
   * Opens and initializes the transactional producer. The client gives the
   * initialization a fixed few seconds, which the first one on a fresh
   * cluster (the coordinator creating its log) can outlast: a transient
   * failure is tried again, on a new producer, until `adminTimeoutMs`.
   */
  const connectTransactional = async (instance: KafkaJS.Kafka, id: string, signal: AbortSignal): Promise<KafkaJS.Producer> => {
    const deadline = Date.now() + adminTimeout
    for (;;) {
      const candidate = openTransactional(instance, id)
      try {
        await candidate.connect()
      } catch (error) {
        await candidate.disconnect().catch(() => undefined)
        if (!isTransient(errorCode(error)) || Date.now() >= deadline || signal.aborted) throw error
        await pause(TRANSACTIONAL_RETRY_DELAY_MS, signal)
        continue
      }
      // Released while it was being opened: nobody would close it.
      if (signal.aborted) {
        await candidate.disconnect().catch(() => undefined)
        throw new AdapterError('confluent adapter is not connected', { retryable: false })
      }
      return candidate
    }
  }

  const discardTransactional = async (stuck: KafkaJS.Producer): Promise<void> => {
    if (transactional === stuck) transactional = undefined
    await stuck.disconnect().catch(() => undefined)
  }

  /**
   * Sends through the producer and resolves only once
   * every record was acknowledged: the client resolves a batch only when
   * every delivery report came back clean, and rejects with the first
   * refusal otherwise.
   */
  const sendThrough = async (through: Pick<KafkaJS.Producer, 'sendBatch'>, records: readonly RawRecord[]): Promise<void> => {
    try {
      await through.sendBatch({ topicMessages: groupByTopic(records) })
    } catch (error) {
      throw wrap(error, 'produce failed')
    }
  }

  /** Opens the clients of a connection; `connect` runs one at a time. */
  const open = async (config: BrokerConfig): Promise<void> => {
    const connection = { closed: new AbortController(), fenced: false }
    // A disconnect from now on aborts this connection, not the last one.
    link = connection
    const { KafkaJS: client } = await loadClient()
    if (connection.closed.signal.aborted) throw new AdapterError('confluent adapter disconnected while connecting', { retryable: false })
    const kafkaJS: KafkaJS.KafkaConfig = {
      brokers: [...config.brokers],
      clientId: config.clientId,
      // The client logs connection retries and group events at INFO on its
      // own logger; the harbor reports outcomes through events, so only the
      // client's errors are let through. Override with `global`.
      logLevel: options.logLevel ?? 1,
      ...(config.ssl !== undefined && { ssl: config.ssl }),
      ...(config.sasl !== undefined && { sasl: { mechanism: config.sasl.mechanism, username: config.sasl.username, password: config.sasl.password } as KafkaJS.SASLOptions })
    }
    const instance = new client.Kafka({ kafkaJS, ...options.global })
    const newProducer = instance.producer({
      // Never lose or duplicate by default: every produce waits for all
      // in-sync replicas, and the idempotent producer de-duplicates the
      // client's own retries.
      kafkaJS: { acks: -1, idempotent: true },
      ...options.producer
    })
    const newAdmin = instance.admin()
    // Transactions need a producer of their own: a transactional producer
    // holds one open transaction and every send on it is part of it.
    let newTransactional: KafkaJS.Producer | undefined
    try {
      await newProducer.connect()
      if (config.transactionalId !== undefined) newTransactional = await connectTransactional(instance, config.transactionalId, connection.closed.signal)
      await newAdmin.connect()
    } catch (error) {
      await Promise.allSettled([newProducer.disconnect(), newTransactional?.disconnect(), newAdmin.disconnect()])
      throw wrap(error, 'connect failed')
    }
    // Released while it was being opened: nobody would close the clients.
    if (connection.closed.signal.aborted) {
      await Promise.allSettled([newProducer.disconnect(), newTransactional?.disconnect(), newAdmin.disconnect()])
      throw new AdapterError('confluent adapter disconnected while connecting', { retryable: false })
    }
    kafka = instance
    producer = newProducer
    transactional = newTransactional
    transactionalId = config.transactionalId
    admin = newAdmin
  }

  const adapter: ClientAdapter = {
    name: 'confluent',

    async connect (config: BrokerConfig) {
      if (kafka !== undefined) return
      connecting ??= open(config).finally(() => { connecting = undefined })
      await connecting
    },

    async disconnect () {
      link.closed.abort()
      const pending: Array<Promise<unknown>> = []
      for (const consumer of consumers) pending.push(consumer.disconnect())
      consumers.clear()
      if (producer !== undefined) pending.push(producer.disconnect())
      if (transactional !== undefined) pending.push(transactional.disconnect())
      if (admin !== undefined) pending.push(admin.disconnect())
      kafka = undefined
      producer = undefined
      transactional = undefined
      transactionalId = undefined
      admin = undefined
      const results = await Promise.allSettled(pending)
      const failed = results.find((result): result is PromiseRejectedResult => result.status === 'rejected')
      if (failed !== undefined) throw wrap(failed.reason, 'disconnect failed')
    },

    async produce (records: readonly RawRecord[]) {
      const { producer: current } = requireConnected()
      await sendThrough(current, records)
    },

    async transaction (): Promise<TransactionHandle> {
      const { kafka: instance } = requireConnected()
      const id = transactionalId
      if (id === undefined) throw new ConfigError('confluentAdapter: transactions need transactionalId on the harbor configuration')
      const connection = link
      // Another producer took the id: opening one again would fence it
      // back, the zombie fencing the live one.
      if (connection.fenced) throw new FencedError(id)
      // A producer discarded after a transaction it could neither commit nor
      // abort is opened again here; its init clears the broker-side state.
      if (transactional === undefined) {
        try {
          transactional = await connectTransactional(instance, id, connection.closed.signal)
        } catch (error) {
          throw wrap(error, 'transaction begin failed')
        }
      }
      const current = transactional
      let tx: KafkaJS.Transaction
      try {
        tx = await current.transaction()
      } catch (error) {
        if (FENCING_CODES.has(errorCode(error) ?? 0)) {
          connection.fenced = true
          await discardTransactional(current)
          throw new FencedError(id, { cause: error })
        }
        throw wrap(error, 'transaction begin failed')
      }
      // A monotonic clock: a wall clock stepped by NTP would move a fence
      // across the timeout.
      const began = performance.now()
      let discarded = false
      /**
       * What a failed step of the transaction throws. A fenced producer is
       * fatal to librdkafka, and dropped; before `transaction.timeout.ms`
       * elapsed it can only be another producer holding the id, and after
       * it the broker aborting this transaction, which the next producer's
       * initialization recovers from.
       */
      const failed = async (error: unknown, what: string): Promise<Error> => {
        if (!FENCING_CODES.has(errorCode(error) ?? 0)) return wrap(error, what)
        // Measured before the producer is released, which flushes for up to
        // seconds: a fence must not age into a timeout while it does.
        const elapsed = performance.now() - began
        discarded = true
        await discardTransactional(current)
        if (elapsed < transactionTimeoutMs) {
          connection.fenced = true
          return new FencedError(id, { cause: error })
        }
        return new AdapterError(`${what}: the broker aborted the transaction (open past transaction.timeout.ms, or the id was taken): ${describeError(error)}`, { cause: error, retryable: true })
      }
      return {
        async produce (records) {
          // Resolves only once every record was acknowledged, as sendThrough.
          try {
            await tx.sendBatch({ topicMessages: groupByTopic(records) })
          } catch (error) {
            throw await failed(error, 'produce failed')
          }
        },
        async sendOffsets (consumption, offsets) {
          // The client asks for the consumer itself: the group metadata the
          // broker fences the commit with comes from it.
          const consumer = consumersByHandle.get(consumption)
          if (consumer === undefined) throw new ConfigError('confluentAdapter: the offsets belong to a consumption this adapter did not open')
          const byTopic = new Map<string, Array<{ partition: number, offset: string }>>()
          for (const { topic, partition, offset } of offsets) {
            const partitions = byTopic.get(topic) ?? []
            partitions.push({ partition, offset })
            byTopic.set(topic, partitions)
          }
          try {
            await tx.sendOffsets({ consumer, topics: [...byTopic].map(([topic, partitions]) => ({ topic, partitions })) })
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
          // Discarded already: the next producer's init clears what the
          // broker still holds.
          if (discarded) return
          try {
            await tx.abort()
          } catch (error) {
            // The client keeps the transaction marked as ongoing after a
            // failed abort, and would refuse every transaction from then on;
            // the producer is dropped and opened again on the next one,
            // unless another producer fenced it.
            const failure = await failed(error, 'transaction abort failed')
            if (!discarded) await discardTransactional(current)
            throw failure
          }
        }
      }
    },

    async consume (consumeOptions: ConsumeOptions): Promise<ConsumerHandle> {
      const { kafka: current } = requireConnected()
      const rebalanceCb = async (error: { code?: number }, assignment: KafkaJS.TopicPartition[], fns: RebalanceAssignmentFns): Promise<void> => {
        if (error.code === ERR__REVOKE_PARTITIONS) {
          // Awaited even when the group already took the partitions away (a
          // missed poll): the client releases them only once the deliveries
          // in flight settled anyway, and the core uses the wait for what it
          // holds outside deliveries (a batch it is running).
          try {
            await consumeOptions.onPartitionsRevoked?.(assignment.map(({ topic, partition }) => ({ topic, partition })))
          } finally {
            fns.unassign(assignment)
          }
        } else if (error.code === ERR__ASSIGN_PARTITIONS) {
          fns.assign(assignment)
        }
      }
      const consumer = current.consumer({
        kafkaJS: {
          groupId: consumeOptions.groupId,
          // The core commits after the handler finished; the client must
          // never commit on its own.
          autoCommit: false,
          fromBeginning: consumeOptions.fromBeginning ?? false,
          ...(consumeOptions.onError !== undefined && { logger: errorForwardingLogger(consumeOptions.onError, options.logLevel ?? 1) })
        },
        rebalance_cb: rebalanceCb,
        // The core's maxProcessingTime, so a retry delay it accepted is one
        // the client tolerates too. The passthrough below may still override.
        ...(consumeOptions.maxProcessingTimeMs !== undefined && { 'max.poll.interval.ms': consumeOptions.maxProcessingTimeMs }),
        ...options.consumer
      })
      try {
        await consumer.connect()
        await consumer.subscribe({ topics: [...consumeOptions.topics] })
        await consumer.run({
          partitionsConsumedConcurrently: consumeOptions.concurrency ?? 1,
          // A rejected delivery is settled like any other (the contract):
          // reported, never redelivered. Left to the client, a rejection
          // would rewind the partition and deliver the message again, for
          // as long as it kept failing.
          eachMessage: async (payload) => {
            try {
              await consumeOptions.eachMessage(toRaw(payload))
            } catch (failure) {
              // An error callback that throws must not hand the rejection back to the client either.
              try {
                consumeOptions.onError?.(failure)
              } catch {}
            }
          }
        })
      } catch (error) {
        await consumer.disconnect().catch(() => undefined)
        throw wrap(error, 'consume failed')
      }
      consumers.add(consumer)

      const handle: ConsumerHandle = {
        async commit (offsets: readonly TopicPartitionOffset[]) {
          try {
            await consumer.commitOffsets(offsets.map(({ topic, partition, offset }) => ({ topic, partition, offset })))
          } catch (error) {
            throw wrap(error, 'commit failed')
          }
        },
        async stop () {
          consumers.delete(consumer)
          try {
            await consumer.disconnect()
          } catch (error) {
            throw wrap(error, 'consumer disconnect failed')
          }
        },
        pause (partitions: readonly TopicPartition[]) {
          consumer.pause(partitions.map(({ topic, partition }) => ({ topic, partitions: [partition] })))
        },
        resume (partitions: readonly TopicPartition[]) {
          consumer.resume(partitions.map(({ topic, partition }) => ({ topic, partitions: [partition] })))
        }
      }
      consumersByHandle.set(handle, consumer)
      return handle
    },

    admin: {
      async createTopics (specs: readonly TopicSpec[]) {
        const { admin: current } = requireConnected()
        const topics: KafkaJS.ITopicConfig[] = specs.map((spec) => ({
          topic: spec.topic,
          // Left out, the client asks for the broker's defaults (-1).
          numPartitions: spec.partitions,
          replicationFactor: spec.replicationFactor,
          ...(spec.config !== undefined && {
            configEntries: Object.entries(spec.config).map(([name, value]) => ({ name, value }))
          })
        }))
        const deadline = Date.now() + adminTimeout
        try {
          await current.createTopics({ topics, timeout: adminTimeout })
        } catch (error) {
          // The client reports the whole call as failed when one topic
          // already exists; that is the outcome this method promises, not
          // an error. Anything else is.
          const nested = (error as { errors?: unknown[] } | null)?.errors
          const alreadyExists = errorCode(error) === ERR_TOPIC_ALREADY_EXISTS ||
            (Array.isArray(nested) && nested.length > 0 && nested.every((entry) => errorCode(entry) === ERR_TOPIC_ALREADY_EXISTS))
          if (!alreadyExists) throw wrap(error, 'createTopics failed')
        }
        // The controller accepts the creation before the metadata every
        // broker (and this client) serves reflects it. The promise here is
        // "the topics exist", so wait until they are visible, bounded by the
        // admin timeout: a consumer subscribing right after must find them.
        const wanted = specs.map((spec) => spec.topic)
        let delay = 50
        for (;;) {
          let visible: Set<string>
          try {
            visible = new Set(await current.listTopics({ timeout: adminTimeout }))
          } catch (error) {
            throw wrap(error, 'listTopics failed')
          }
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
        const { admin: current } = requireConnected()
        try {
          const topics = await current.listTopics({ timeout: adminTimeout })
          return topics.includes(topic)
        } catch (error) {
          throw wrap(error, 'listTopics failed')
        }
      },
      async fetchTopicOffsets (topics: readonly string[]): Promise<PartitionOffsets[]> {
        const { admin: current } = requireConnected()
        // The client answers one topic per call.
        const watermarksOf = async (topic: string): Promise<PartitionOffsets[]> => {
          let entries: Array<{ partition: number, high: string, low: string }>
          try {
            // The isolation level is optional at runtime (read uncommitted, the
            // broker default); the client's declaration marks it required.
            entries = await current.fetchTopicOffsets(topic, { timeout: adminTimeout } as Parameters<KafkaJS.Admin['fetchTopicOffsets']>[1])
          } catch (error) {
            throw wrap(error, 'fetchTopicOffsets failed')
          }
          // A partition whose leader is not serving (right after the topic
          // was created, during an election) answers -1. A watermark is a
          // count of records, never negative, so that partition is left out,
          // as the contract allows: its lag is unknown for now, and a
          // scrape must not wait for a leader.
          return entries
            .filter((entry) => OFFSET_PATTERN.test(entry.high) && OFFSET_PATTERN.test(entry.low))
            .map((entry) => ({ topic, partition: entry.partition, low: entry.low, high: entry.high }))
        }
        return (await Promise.all(topics.map(watermarksOf))).flat()
      },
      async fetchCommittedOffsets (groupId: string, topics: readonly string[]): Promise<CommittedOffset[]> {
        const { admin: current } = requireConnected()
        try {
          const result = await current.fetchOffsets({ groupId, topics: [...topics], timeout: adminTimeout })
          // librdkafka reports a partition the group never committed on with a
          // negative offset (RD_KAFKA_OFFSET_INVALID, -1001).
          return result.flatMap(({ topic, partitions }) => partitions.map(({ partition, offset }) => ({
            topic,
            partition,
            offset: OFFSET_PATTERN.test(offset) ? offset : null
          })))
        } catch (error) {
          throw wrap(error, 'fetchOffsets failed')
        }
      }
    }
  }
  return adapter
}

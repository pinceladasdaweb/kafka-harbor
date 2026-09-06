/**
 * ClientAdapter over @confluentinc/kafka-javascript, through its promisified
 * KafkaJS-compatible API. The client is a peer dependency loaded on
 * `connect()`, never at import time: an application that only uses the core
 * (or another adapter) never touches the native binding.
 */
import {
  AdapterError,
  ConfigError,
  type BrokerConfig,
  type ClientAdapter,
  type ConsumeOptions,
  type ConsumerHandle,
  type RawMessage,
  type RawRecord,
  type TopicPartition,
  type TopicPartitionOffset,
  type TopicSpec
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
  /** Timeout for Admin API calls, ms. Default: 30000. */
  adminTimeoutMs?: number
  /** The client's own log level (KafkaJS.logLevel: 0 nothing, 1 error, 2 warn, 3 info, 4 debug). Default: 1. */
  logLevel?: 0 | 1 | 2 | 3 | 4
}

/** librdkafka codes surfaced through KafkaJSError.code. */
const ERR__ASSIGN_PARTITIONS = -175
const ERR__REVOKE_PARTITIONS = -174
const ERR_TOPIC_ALREADY_EXISTS = 36

/**
 * Failures that will not go away by trying again: bad arguments, oversized
 * records, authorization. Everything else the client reports (transport,
 * timeouts, leadership changes) is transient and worth the producer's retry
 * ladder. The client's own `retriable` flag is NOT consulted: it is only
 * meaningful for the transactional producer and defaults to false, which
 * would silently disable every retry.
 */
const DEFINITIVE_CODES = new Set([
  -186, // ERR__INVALID_ARG
  10, // ERR_MSG_SIZE_TOO_LARGE
  17, // ERR_INVALID_TOPIC_EXCEPTION
  18, // ERR_RECORD_LIST_TOO_LARGE
  29, // ERR_TOPIC_AUTHORIZATION_FAILED
  30, // ERR_GROUP_AUTHORIZATION_FAILED
  31, // ERR_CLUSTER_AUTHORIZATION_FAILED
  40, // ERR_INVALID_CONFIG
  58, // ERR_SASL_AUTHENTICATION_FAILED
  87 // ERR_INVALID_RECORD
])

/**
 * librdkafka properties the adapter owns because the core's offset policy
 * depends on them; a passthrough that flipped one would turn the consumer
 * into at-most-once without anyone noticing.
 */
const RESERVED_CONSUMER_KEYS = ['enable.auto.commit', 'enable.auto.offset.store', 'auto.offset.reset']

interface RebalanceAssignmentFns {
  assign: (assignment: KafkaJS.TopicPartition[]) => void
  unassign: (assignment: KafkaJS.TopicPartition[]) => void
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

const wrap = (error: unknown, what: string): AdapterError => {
  const code = errorCode(error)
  return new AdapterError(`${what}: ${error instanceof Error ? error.message : String(error)}`, {
    cause: error,
    retryable: code === undefined || !DEFINITIVE_CODES.has(code)
  })
}

/**
 * The client's logger contract, pointed at the adapter's error callback:
 * fetch-loop failures the client would only print become `error` events.
 */
const errorForwardingLogger = (onError: (error: unknown) => void, level: number): KafkaJS.Logger => {
  const logger: KafkaJS.Logger = {
    info: () => {},
    warn: () => {},
    debug: () => {},
    error: (message, extra) => {
      if (level >= 1) onError(new AdapterError(`client: ${message}`, { cause: extra }))
    },
    namespace: () => logger,
    setLogLevel: () => {}
  }
  return logger
}

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

export function confluentAdapter (options: ConfluentAdapterOptions = {}): ClientAdapter {
  for (const source of [options.global, options.consumer]) {
    for (const key of RESERVED_CONSUMER_KEYS) {
      if (source !== undefined && key in source) {
        throw new ConfigError(`confluentAdapter: "${key}" is managed by the adapter (the core commits after the handler; fromBeginning drives the reset policy) and cannot be overridden`)
      }
    }
  }
  let kafka: KafkaJS.Kafka | undefined
  let producer: KafkaJS.Producer | undefined
  let admin: KafkaJS.Admin | undefined
  const consumers = new Set<KafkaJS.Consumer>()
  const adminTimeout = options.adminTimeoutMs ?? 30_000

  const loadClient = async (): Promise<ConfluentClientModule> => {
    try {
      if (typeof options.client === 'function') return await options.client()
      if (options.client !== undefined) return options.client
      return await import('@confluentinc/kafka-javascript') as unknown as ConfluentClientModule
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

  const adapter: ClientAdapter = {
    name: 'confluent',

    async connect (config: BrokerConfig) {
      if (kafka !== undefined) return
      const { KafkaJS: client } = await loadClient()
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
      try {
        await newProducer.connect()
        await newAdmin.connect()
      } catch (error) {
        await Promise.allSettled([newProducer.disconnect(), newAdmin.disconnect()])
        throw wrap(error, 'connect failed')
      }
      kafka = instance
      producer = newProducer
      admin = newAdmin
    },

    async disconnect () {
      const pending: Array<Promise<unknown>> = []
      for (const consumer of consumers) pending.push(consumer.disconnect())
      consumers.clear()
      if (producer !== undefined) pending.push(producer.disconnect())
      if (admin !== undefined) pending.push(admin.disconnect())
      kafka = undefined
      producer = undefined
      admin = undefined
      const results = await Promise.allSettled(pending)
      const failed = results.find((result): result is PromiseRejectedResult => result.status === 'rejected')
      if (failed !== undefined) throw wrap(failed.reason, 'disconnect failed')
    },

    async produce (records: readonly RawRecord[]) {
      const { producer: current } = requireConnected()
      let metadata: KafkaJS.RecordMetadata[]
      try {
        metadata = await current.sendBatch({ topicMessages: groupByTopic(records) })
      } catch (error) {
        throw wrap(error, 'produce failed')
      }
      // The client resolves per record; an errorCode other than 0 is a
      // record the broker did not take, and the batch is not acknowledged.
      const rejected = metadata.filter((entry) => entry.errorCode !== 0)
      if (rejected.length > 0) {
        const detail = rejected.map((entry) => `${entry.topicName}[${entry.partition}] code ${entry.errorCode}`).join(', ')
        throw new AdapterError(`produce not acknowledged for ${rejected.length} record(s): ${detail}`)
      }
    },

    async consume (consumeOptions: ConsumeOptions): Promise<ConsumerHandle> {
      const { kafka: current } = requireConnected()
      const rebalanceCb = async (error: { code?: number }, assignment: KafkaJS.TopicPartition[], fns: RebalanceAssignmentFns): Promise<void> => {
        if (error.code === ERR__REVOKE_PARTITIONS) {
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
          eachMessage: async (payload) => {
            await consumeOptions.eachMessage(toRaw(payload))
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
      return handle
    },

    admin: {
      async createTopics (specs: readonly TopicSpec[]) {
        const { admin: current } = requireConnected()
        const topics: KafkaJS.ITopicConfig[] = specs.map((spec) => ({
          topic: spec.topic,
          numPartitions: spec.partitions ?? 1,
          replicationFactor: spec.replicationFactor ?? 1,
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
        const wanted = new Set(specs.map((spec) => spec.topic))
        for (;;) {
          let visible: string[]
          try {
            visible = await current.listTopics({ timeout: adminTimeout })
          } catch (error) {
            throw wrap(error, 'listTopics failed')
          }
          if ([...wanted].every((topic) => visible.includes(topic))) return
          if (Date.now() >= deadline) {
            const missing = [...wanted].filter((topic) => !visible.includes(topic))
            throw new AdapterError(`createTopics: ${missing.join(', ')} not visible in metadata after ${adminTimeout}ms`)
          }
          await new Promise((resolve) => setTimeout(resolve, 50))
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
      }
    }
  }
  return adapter
}

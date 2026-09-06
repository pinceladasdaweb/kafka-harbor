/**
 * The contract between the core and a Kafka client. Deliberately minimal:
 * only what the protocol requires. Retry topics, DLQ, serialization, offset
 * policy and shutdown live in the core, outside the adapter; that is what
 * makes a second adapter cheap and what keeps this interface stable.
 *
 * Bytes cross this boundary as Buffer; the core owns every conversion.
 *
 * Semver note: this interface is implemented by users (a custom adapter),
 * so ADDING a required member is a breaking change for them. New
 * capabilities arrive as optional members, and the core works when they are
 * absent.
 */

export interface TopicPartition {
  readonly topic: string
  readonly partition: number
}

export interface TopicPartitionOffset extends TopicPartition {
  /**
   * The offset to commit: the NEXT offset to read, i.e. `message.offset + 1`
   * as a decimal string. Kafka's commit semantics, spelled out once here so
   * no adapter has to guess.
   */
  readonly offset: string
}

/** Wire headers: the adapter delivers them as the client exposes them. */
export type RawHeaders = Record<string, Buffer | string | Array<Buffer | string> | undefined>

/** A message as the wire carries it. */
export interface RawMessage {
  readonly topic: string
  readonly partition: number
  readonly offset: string
  readonly key: Buffer | null
  readonly value: Buffer | null
  readonly headers: RawHeaders
  /** Epoch milliseconds, as reported by the broker. */
  readonly timestamp: number
}

/** A record to produce. Headers go out as UTF-8 strings; a null value is a tombstone. */
export interface RawRecord {
  readonly topic: string
  readonly key: Buffer | null
  readonly value: Buffer | null
  readonly headers: Record<string, string>
  readonly partition?: number
}

export interface SaslConfig {
  mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512'
  username: string
  password: string
}

export interface BrokerConfig {
  readonly clientId: string
  readonly brokers: readonly string[]
  readonly ssl?: boolean
  readonly sasl?: SaslConfig
}

export interface ConsumeOptions {
  readonly groupId: string
  readonly topics: readonly string[]
  /**
   * Called for every message, in order within a partition. The adapter must
   * not commit on its own and must not deliver the next message of the same
   * partition until the returned promise settles. Partitions may be
   * processed concurrently up to `concurrency`.
   */
  readonly eachMessage: (message: RawMessage) => Promise<void>
  /** Maximum partitions processed concurrently. Default: 1. */
  readonly concurrency?: number
  /**
   * The longest one `eachMessage` call may take, retry delay included, in
   * milliseconds. An adapter maps it to the client setting that decides how
   * long a member may go without polling (`max.poll.interval.ms`), so the
   * core's `maxProcessingTime` and the client agree on the same number. An
   * adapter that ignores it leaves the client's default in place.
   */
  readonly maxProcessingTimeMs?: number
  /**
   * Called when a rebalance is about to take these partitions away. The
   * adapter awaits it before releasing them; the core uses that time to let
   * the handlers still running on those partitions finish and commit, so the
   * next owner does not repeat their work. Bounded by the core, never longer
   * than `maxProcessingTimeMs`.
   */
  readonly onPartitionsRevoked?: (partitions: readonly TopicPartition[]) => Promise<void>
  /** Called when a fetch loop error is not attributable to a message. */
  readonly onError?: (error: unknown) => void
  /** Whether a brand-new group starts from the earliest offset. Default: false. */
  readonly fromBeginning?: boolean
}

/**
 * A running consumption: the token the core hands back to `commit`, `pause`,
 * `resume` and `stop`. Opaque to the core beyond these methods.
 */
export interface ConsumerHandle {
  /** Persists offsets. Resolves only after the broker acknowledged the commit. */
  commit: (offsets: readonly TopicPartitionOffset[]) => Promise<void>
  /**
   * Stops fetching and leaves the group. In-flight `eachMessage` promises
   * are awaited by the core before this is called.
   */
  stop: () => Promise<void>
  /**
   * Stops fetching from these partitions without leaving the group, until
   * `resume`. Optional: the core does not call either today; an application
   * holding the handle may. A pause belongs to this consumption and must not
   * survive its `stop()`.
   */
  pause?: (partitions: readonly TopicPartition[]) => void
  resume?: (partitions: readonly TopicPartition[]) => void
}

export interface TopicSpec {
  readonly topic: string
  readonly partitions?: number
  readonly replicationFactor?: number
  /** Broker-side topic configs, e.g. `{ 'retention.ms': '3600000' }`. */
  readonly config?: Record<string, string>
}

export interface AdminApi {
  /**
   * Creates the topics that do not exist yet; existing ones are not an error.
   * Resolves only once the topics are visible: a `consume` or `topicExists`
   * issued right after must find them, even though a broker acknowledges a
   * creation before every replica serves the new metadata.
   */
  createTopics: (topics: readonly TopicSpec[]) => Promise<void>
  topicExists: (topic: string) => Promise<boolean>
}

export interface ClientAdapter {
  /** A short identifier for logs and metrics, e.g. 'confluent'. */
  readonly name: string

  connect: (config: BrokerConfig) => Promise<void>
  disconnect: () => Promise<void>

  /**
   * Produces the records and resolves after the broker acknowledged every
   * one of them (acks=all). Rejects if any record was not acknowledged; the
   * core never treats a produce as done before this resolves.
   */
  produce: (records: readonly RawRecord[]) => Promise<void>

  consume: (options: ConsumeOptions) => Promise<ConsumerHandle>

  readonly admin: AdminApi
}

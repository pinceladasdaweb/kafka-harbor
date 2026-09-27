/**
 * An in-memory ClientAdapter: a single-process "broker" with topics,
 * partitions, consumer groups and committed offsets, plus a record of every
 * call made to it. It is what the unit and contract suites drive the core
 * with, and what applications use to test their handlers without Docker.
 *
 * Fidelity boundaries: no rebalancing (one member per group), no retention,
 * no transactions. Keyed messages land on the partition Kafka's default
 * partitioner would pick (murmur2); unkeyed ones round-robin.
 */
import {
  AdapterError,
  ConfigError,
  partitionForKey,
  partitionKey,
  splitPartitionKey,
  type BrokerConfig,
  type ClientAdapter,
  type CommittedOffset,
  type ConsumeOptions,
  type ConsumerHandle,
  type PartitionOffsets,
  type RawMessage,
  type RawRecord,
  type TopicPartition,
  type TopicPartitionOffset,
  type TopicSpec
} from '../index'

export { runAdapterContract } from './contract'
export type { AdapterContractSetup } from './contract'

export interface MemoryAdapterOptions {
  /** Partitions for topics created on demand. Default: 1. */
  partitions?: number
  /**
   * Whether producing to an unknown topic creates it. Default: true, like a
   * broker with `auto.create.topics.enable`. Off, it fails the way the
   * consumer's `autoCreateTopics: false` path needs it to.
   */
  autoCreateTopics?: boolean
  /** Time source for message timestamps. Default: Date.now. */
  now?: () => number
}

export interface RecordedCall {
  readonly method: 'connect' | 'disconnect' | 'produce' | 'consume' | 'commit' | 'stop' | 'pause' | 'resume' | 'createTopics' | 'topicExists' | 'fetchTopicOffsets' | 'fetchCommittedOffsets' | 'transaction' | 'transactionProduce' | 'transactionOffsets' | 'transactionCommit' | 'transactionAbort'
  readonly args: readonly unknown[]
}

export interface MemoryAdapter extends ClientAdapter {
  /** Every call the core made, in order. */
  readonly calls: readonly RecordedCall[]
  /** Forgets the recorded calls, so a long test does not keep every payload alive. */
  clearCalls: () => void
  /** Topic names known to the broker. */
  topics: () => string[]
  createTopic: (topic: string, partitions?: number) => void
  /** Every message on a topic, across partitions, in the order they were appended. */
  messages: (topic: string) => RawMessage[]
  /** The committed offset (next to read) of a group on a partition. */
  committed: (groupId: string, topic: string, partition: number) => string | undefined
  /** Makes the next produce call reject with this error, once. */
  failNextProduce: (error: unknown) => void
  /**
   * Resolves once every message currently on the topic has been delivered to
   * the group and none of its partitions is paused (a retry message the core
   * parked is still to run; the partition resumes once it did). Rejects
   * instead of waiting forever when the group's last member stopped before
   * that happened (a consumer that aborted or crashed): nothing would ever
   * deliver the rest.
   */
  whenDrained: (groupId: string, topic: string) => Promise<void>
  /** Currently paused partitions of a group. */
  paused: (groupId: string) => TopicPartition[]
  readonly connected: boolean
}

interface Topic {
  readonly name: string
  readonly partitions: RawMessage[][]
  /** Every message in the order it was appended, whatever its partition. */
  readonly appended: RawMessage[]
  roundRobin: number
}

interface Group {
  readonly committed: Map<string, string>
  readonly positions: Map<string, number>
  readonly paused: Set<string>
  /** Consumptions currently open on the group. */
  members: number
  /** Whether a member ever left: a drained wait after that has nobody to wait for. */
  stopped: boolean
}

export function memoryAdapter (options: MemoryAdapterOptions = {}): MemoryAdapter {
  const defaultPartitions = options.partitions ?? 1
  const autoCreate = options.autoCreateTopics ?? true
  const now = options.now ?? Date.now
  const topics = new Map<string, Topic>()
  const groups = new Map<string, Group>()
  const calls: RecordedCall[] = []
  const wakeups = new Set<() => void>()
  let connected = false
  let transactionalId: string | undefined
  /** The group each consumption belongs to, for the offsets a transaction commits. */
  const groupOfHandle = new WeakMap<ConsumerHandle, string>()
  let transactionOpen = false
  let nextProduceError: { error: unknown } | undefined

  const record = (method: RecordedCall['method'], ...args: unknown[]): void => {
    calls.push({ method, args })
  }
  const wake = (): void => {
    for (const resolve of [...wakeups]) resolve()
    wakeups.clear()
  }
  const nextEvent = (): Promise<void> => new Promise((resolve) => { wakeups.add(resolve) })

  const ensureTopic = (name: string, partitions = defaultPartitions): Topic => {
    let topic = topics.get(name)
    if (topic === undefined) {
      topic = { name, partitions: Array.from({ length: partitions }, () => []), appended: [], roundRobin: 0 }
      topics.set(name, topic)
      wake()
    }
    return topic
  }
  const group = (groupId: string): Group => {
    let entry = groups.get(groupId)
    if (entry === undefined) {
      entry = { committed: new Map(), positions: new Map(), paused: new Set(), members: 0, stopped: false }
      groups.set(groupId, entry)
    }
    return entry
  }

  /** Every record checked before the first append: a batch is all or nothing. */
  const validate = (records: readonly RawRecord[]): void => {
    for (const item of records) {
      if (typeof item.topic !== 'string' || item.topic === '') {
        throw new AdapterError(`invalid topic name ${JSON.stringify(item.topic)}`, { retryable: false })
      }
      if (!autoCreate && !topics.has(item.topic)) {
        throw new AdapterError(`unknown topic "${item.topic}"`, { retryable: false })
      }
      const count = topics.get(item.topic)?.partitions.length ?? defaultPartitions
      if (item.partition !== undefined && (!Number.isInteger(item.partition) || item.partition < 0 || item.partition >= count)) {
        throw new AdapterError(`partition ${item.partition} does not exist on "${item.topic}"`, { retryable: false })
      }
    }
  }
  const append = (records: readonly RawRecord[]): void => {
    for (const item of records) {
      const topic = ensureTopic(item.topic)
      const count = topic.partitions.length
      let partition: number
      if (item.partition !== undefined) {
        partition = item.partition
      } else if (item.key !== null) {
        partition = partitionForKey(item.key, count)
      } else {
        partition = topic.roundRobin++ % count
      }
      const log = topic.partitions[partition] as RawMessage[]
      const message: RawMessage = {
        topic: item.topic,
        partition,
        offset: String(log.length),
        key: item.key,
        value: item.value,
        headers: { ...item.headers },
        timestamp: now()
      }
      log.push(message)
      topic.appended.push(message)
    }
    wake()
  }

  const adapter: MemoryAdapter = {
    name: 'memory',
    calls,
    clearCalls () {
      calls.length = 0
    },
    get connected () {
      return connected
    },
    topics: () => [...topics.keys()],
    createTopic (name, partitions) {
      ensureTopic(name, partitions)
    },
    messages: (name) => [...(topics.get(name)?.appended ?? [])],
    committed: (groupId, topic, partition) => groups.get(groupId)?.committed.get(partitionKey(topic, partition)),
    failNextProduce (error) {
      nextProduceError = { error }
    },
    paused: (groupId) => [...(groups.get(groupId)?.paused ?? [])].map((entry) => {
      const { topic, partition } = splitPartitionKey(entry)
      return { topic, partition }
    }),
    async whenDrained (groupId, topicName) {
      for (;;) {
        const topic = topics.get(topicName)
        const entry = groups.get(groupId)
        // A paused partition holds a message the core parked and will run
        // later; the topic is drained once that ran too, which is when the
        // core resumes the partition.
        const drained = topic !== undefined && entry !== undefined &&
          topic.partitions.every((messages, partition) => (entry.positions.get(partitionKey(topicName, partition)) ?? 0) >= messages.length && !entry.paused.has(partitionKey(topicName, partition)))
        if (drained) return
        if (entry !== undefined && entry.members === 0 && entry.stopped) {
          throw new Error(`whenDrained("${groupId}", "${topicName}"): the group has no member left; its last consumption stopped before the topic was drained`)
        }
        await nextEvent()
      }
    },

    async connect (config: BrokerConfig) {
      record('connect', config)
      connected = true
      transactionalId = config.transactionalId
    },
    async disconnect () {
      record('disconnect')
      connected = false
      wake()
    },

    async produce (records: readonly RawRecord[]) {
      record('produce', records)
      if (nextProduceError !== undefined) {
        const { error } = nextProduceError
        nextProduceError = undefined
        throw error
      }
      if (!connected) throw new AdapterError('memory adapter is not connected')
      validate(records)
      append(records)
    },

    async transaction () {
      record('transaction')
      if (!connected) throw new AdapterError('memory adapter is not connected')
      if (transactionalId === undefined) throw new ConfigError('memory adapter: transactions need transactionalId on the harbor configuration')
      if (transactionOpen) throw new AdapterError('memory adapter: a transaction is already open; one at a time', { retryable: false })
      transactionOpen = true
      const buffered: RawRecord[] = []
      const offsets: Array<{ groupId: string, offset: TopicPartitionOffset }> = []
      let completed = false
      const requireOpen = (): void => {
        if (completed) throw new AdapterError('memory adapter: the transaction already ended', { retryable: false })
      }
      return {
        async produce (records: readonly RawRecord[]) {
          record('transactionProduce', records)
          requireOpen()
          if (nextProduceError !== undefined) {
            const { error } = nextProduceError
            nextProduceError = undefined
            throw error
          }
          validate(records)
          buffered.push(...records)
        },
        async sendOffsets (consumption: ConsumerHandle, sent: readonly TopicPartitionOffset[]) {
          record('transactionOffsets', sent)
          requireOpen()
          const groupId = groupOfHandle.get(consumption)
          if (groupId === undefined) throw new ConfigError('memory adapter: the consumption is not one this adapter opened')
          for (const offset of sent) offsets.push({ groupId, offset })
        },
        async commit () {
          record('transactionCommit')
          requireOpen()
          completed = true
          transactionOpen = false
          // Everything lands together: a consumer never sees part of it.
          append(buffered)
          for (const { groupId, offset } of offsets) group(groupId).committed.set(partitionKey(offset.topic, offset.partition), offset.offset)
        },
        async abort () {
          record('transactionAbort')
          requireOpen()
          completed = true
          transactionOpen = false
        }
      }
    },

    async consume (consumeOptions: ConsumeOptions): Promise<ConsumerHandle> {
      record('consume', consumeOptions)
      if (!connected) throw new AdapterError('memory adapter is not connected')
      const entry = group(consumeOptions.groupId)
      entry.members++
      const concurrency = consumeOptions.concurrency ?? 1
      let stopped = false
      let active = 0
      const waiting: Array<() => void> = []
      const acquire = async (): Promise<void> => {
        if (active < concurrency) {
          active++
          return
        }
        await new Promise<void>((resolve) => waiting.push(resolve))
        active++
      }
      const release = (): void => {
        active--
        waiting.shift()?.()
      }

      const running = new Set<Promise<void>>()
      const partitionsSeen = new Set<string>()

      const runPartition = async (topicName: string, partition: number, log: RawMessage[]): Promise<void> => {
        const slot = partitionKey(topicName, partition)
        for (;;) {
          if (stopped) return
          if (!entry.positions.has(slot)) {
            const committed = entry.committed.get(slot)
            entry.positions.set(slot, committed !== undefined ? Number(committed) : (consumeOptions.fromBeginning === true ? 0 : log.length))
          }
          const position = entry.positions.get(slot) as number
          const message = log[position]
          if (message === undefined || entry.paused.has(slot)) {
            await nextEvent()
            continue
          }
          await acquire()
          try {
            if (stopped) return
            await consumeOptions.eachMessage(message)
            entry.positions.set(slot, position + 1)
            wake()
          } catch (error) {
            consumeOptions.onError?.(error)
            // The message is redelivered, but not in a tight loop.
            await new Promise((resolve) => setImmediate(resolve))
          } finally {
            release()
          }
        }
      }

      // Partitions can appear later (a topic created after subscribe), so the
      // scheduler re-scans on every broker event.
      const schedule = (): void => {
        for (const topicName of consumeOptions.topics) {
          const topic = topics.get(topicName)
          if (topic === undefined) continue
          topic.partitions.forEach((log, partition) => {
            const slot = partitionKey(topicName, partition)
            if (partitionsSeen.has(slot)) return
            partitionsSeen.add(slot)
            const run = runPartition(topicName, partition, log).finally(() => running.delete(run))
            running.add(run)
          })
        }
      }
      const scanner = (async () => {
        for (;;) {
          if (stopped) return
          schedule()
          await nextEvent()
        }
      })()

      const handle: ConsumerHandle = {
        async commit (offsets: readonly TopicPartitionOffset[]) {
          record('commit', offsets)
          if (!connected || stopped) throw new AdapterError('commit on a stopped consumer')
          for (const offset of offsets) entry.committed.set(partitionKey(offset.topic, offset.partition), offset.offset)
        },
        async stop () {
          record('stop')
          stopped = true
          entry.members--
          entry.stopped = true
          wake()
          await scanner
          await Promise.allSettled([...running])
          // Positions and pauses belong to the member, and the member is gone.
          // Positions of a topic another member of the group still consumes
          // stay: one member per topic set is how the core joins.
          for (const topicName of consumeOptions.topics) {
            const count = topics.get(topicName)?.partitions.length ?? 0
            for (let partition = 0; partition < count; partition++) {
              entry.positions.delete(partitionKey(topicName, partition))
              entry.paused.delete(partitionKey(topicName, partition))
            }
          }
          wake()
        },
        pause (partitions: readonly TopicPartition[]) {
          record('pause', partitions)
          for (const item of partitions) entry.paused.add(partitionKey(item.topic, item.partition))
        },
        resume (partitions: readonly TopicPartition[]) {
          record('resume', partitions)
          for (const item of partitions) entry.paused.delete(partitionKey(item.topic, item.partition))
          wake()
        }
      }
      groupOfHandle.set(handle, consumeOptions.groupId)
      return handle
    },

    admin: {
      async createTopics (specs: readonly TopicSpec[]) {
        record('createTopics', specs)
        for (const spec of specs) ensureTopic(spec.topic, spec.partitions ?? defaultPartitions)
      },
      async topicExists (topic: string) {
        record('topicExists', topic)
        return topics.has(topic)
      },
      async fetchTopicOffsets (names: readonly string[]): Promise<PartitionOffsets[]> {
        record('fetchTopicOffsets', names)
        return names.flatMap((name) => {
          const topic = topics.get(name)
          if (topic === undefined) throw new AdapterError(`unknown topic "${name}"`, { retryable: false })
          return topic.partitions.map((log, partition) => ({ topic: name, partition, low: '0', high: String(log.length) }))
        })
      },
      async fetchCommittedOffsets (groupId: string, names: readonly string[]): Promise<CommittedOffset[]> {
        record('fetchCommittedOffsets', groupId, names)
        const committed = groups.get(groupId)?.committed
        return names.flatMap((name) => (topics.get(name)?.partitions ?? []).map((_log, partition) => ({
          topic: name,
          partition,
          offset: committed?.get(partitionKey(name, partition)) ?? null
        })))
      }
    }
  }
  return adapter
}

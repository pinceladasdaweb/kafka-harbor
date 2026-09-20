/**
 * The platformatic adapter against a fake client module: the translation
 * layer and the per-partition gate are exercised without a broker. The real
 * client is covered by the contract suite in tests/integration.
 */
import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { Readable } from 'node:stream'
import { describe, test } from 'node:test'

import type { GroupAssignment, Message } from '@platformatic/kafka'

import { partitionAssignerBySubscription, platformaticAdapter, type PlatformaticClientModule } from '../../src/adapters/platformatic/index'
import { AdapterError, ERROR_CODES, type ConsumerHandle, type RawMessage } from '../../src/index'

type ClientMessage = Message<Buffer, Buffer, Buffer, Buffer>

interface Leaders { [topic: string]: number[] }
interface OffsetsAnswer { name: string, partitions: Array<{ partitionIndex: number, offset: bigint, timestamp: bigint, leaderEpoch: number }> }

interface Behavior {
  connectError?: unknown
  sendError?: unknown
  /** One entry per `consume()` call, shifted in order; undefined lets the call succeed. */
  consumeErrors?: unknown[]
  commitError?: unknown
  closeError?: unknown
  producerCloseError?: unknown
  createTopicsError?: unknown
  listTopicsError?: unknown
  topics?: string[] | (() => string[])
  metadata?: Leaders
  metadataError?: unknown
  offsets?: (timestamp: bigint) => OffsetsAnswer[]
  offsetsError?: unknown
  committed?: Array<{ groupId: string, topics: Array<{ name: string, partitions: Array<{ partitionIndex: number, committedOffset: bigint }> }> }>
}

class FakeStream extends Readable {
  private ended = false
  constructor () {
    super({ objectMode: true, read () {} })
  }

  end (): this {
    if (!this.ended && !this.destroyed) {
      this.ended = true
      this.push(null)
    }
    return this
  }

  async close (): Promise<void> {
    this.end()
  }
}

const fakeClient = (behavior: Behavior = {}) => {
  const calls = {
    producerOptions: undefined as unknown,
    adminOptions: undefined as unknown,
    consumers: [] as FakeConsumer[],
    consumeOptions: [] as unknown[],
    sent: [] as unknown[],
    committed: [] as unknown[],
    closed: [] as unknown[],
    created: [] as unknown[],
    listTopicsCalls: 0,
    metadataCalls: [] as unknown[],
    offsetQueries: [] as unknown[]
  }
  const consumeErrors = [...(behavior.consumeErrors ?? [])]

  class FakeConsumer extends EventEmitter {
    assignments: GroupAssignment[] | null = null
    readonly streams: FakeStream[] = []
    constructor (readonly options: unknown) {
      super()
      calls.consumers.push(this)
    }

    async consume (options: unknown): Promise<FakeStream> {
      calls.consumeOptions.push(options)
      const failure = consumeErrors.shift()
      if (failure !== undefined) throw failure
      const stream = new FakeStream()
      this.streams.push(stream)
      return stream
    }

    async commit (options: unknown): Promise<void> {
      calls.committed.push(options)
      if (behavior.commitError !== undefined) throw behavior.commitError
    }

    async close (force: boolean): Promise<void> {
      calls.closed.push(force)
      if (behavior.closeError !== undefined) throw behavior.closeError
      for (const stream of this.streams) stream.end()
    }
  }

  class FakeProducer {
    constructor (options: unknown) {
      calls.producerOptions = options
    }

    async connectToBrokers (): Promise<void> {
      if (behavior.connectError !== undefined) throw behavior.connectError
    }

    async close (): Promise<void> {
      calls.closed.push('producer')
      if (behavior.producerCloseError !== undefined) throw behavior.producerCloseError
    }

    async send (options: unknown): Promise<unknown> {
      calls.sent.push(options)
      if (behavior.sendError !== undefined) throw behavior.sendError
      return { offsets: [] }
    }
  }

  class FakeAdmin {
    constructor (options: unknown) {
      calls.adminOptions = options
    }

    async connectToBrokers (): Promise<void> {}

    async close (): Promise<void> {
      calls.closed.push('admin')
    }

    async listTopics (): Promise<string[]> {
      calls.listTopicsCalls++
      if (behavior.listTopicsError !== undefined) throw behavior.listTopicsError
      return (typeof behavior.topics === 'function' ? behavior.topics() : behavior.topics) ?? []
    }

    async createTopics (options: unknown): Promise<unknown[]> {
      calls.created.push(options)
      if (behavior.createTopicsError !== undefined) throw behavior.createTopicsError
      return []
    }

    async metadata (options: unknown): Promise<unknown> {
      calls.metadataCalls.push(options)
      if (behavior.metadataError !== undefined) throw behavior.metadataError
      return {
        topics: new Map(Object.entries(behavior.metadata ?? {}).map(([name, leaders]) => [name, { partitions: leaders.map((leader) => ({ leader })) }]))
      }
    }

    async listOffsets (options: { topics: Array<{ partitions: Array<{ timestamp: bigint }> }> }): Promise<OffsetsAnswer[]> {
      calls.offsetQueries.push(options)
      if (behavior.offsetsError !== undefined) throw behavior.offsetsError
      return behavior.offsets?.(options.topics[0]!.partitions[0]!.timestamp) ?? []
    }

    async listConsumerGroupOffsets (options: unknown): Promise<unknown> {
      calls.offsetQueries.push(options)
      if (behavior.offsetsError !== undefined) throw behavior.offsetsError
      return behavior.committed ?? []
    }
  }

  const module = { Producer: FakeProducer, Consumer: FakeConsumer, Admin: FakeAdmin } as unknown as PlatformaticClientModule
  return { module, calls }
}

const broker = { clientId: 'app', brokers: ['b:9092'] }

const message = (fields: {
  topic?: string
  partition?: number
  offset: number
  key?: Buffer
  value?: Buffer
  headers?: Array<[string, string]>
  timestamp?: bigint
  leaderEpoch?: number
}): ClientMessage => ({
  topic: fields.topic ?? 't',
  partition: fields.partition ?? 0,
  offset: BigInt(fields.offset),
  key: fields.key,
  value: fields.value,
  headers: new Map(),
  headerEntries: (fields.headers ?? []).map(([name, value]) => [Buffer.from(name), Buffer.from(value)]),
  timestamp: fields.timestamp ?? 1_700_000_000_000n,
  leaderEpoch: fields.leaderEpoch ?? 0,
  metadata: {},
  commit: async () => {},
  toJSON: () => ({})
}) as unknown as ClientMessage

const until = async (condition: () => boolean, what: string): Promise<void> => {
  const deadline = Date.now() + 5_000
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}
const settle = async (): Promise<void> => { await new Promise((resolve) => setTimeout(resolve, 25)) }

const protocolError = (apiId: string, canRetry: boolean): Error => Object.assign(new Error(apiId), { code: 'PLT_KFK_PROTOCOL', apiId, canRetry })
const networkError = (): Error => Object.assign(new Error('connection refused'), { code: 'PLT_KFK_NETWORK', canRetry: true })
const multiple = (errors: unknown[], cause?: unknown): AggregateError => Object.assign(new AggregateError(errors as Error[], 'several things failed'), { code: 'PLT_KFK_MULTIPLE', ...(cause !== undefined && { cause }) })

const connected = async (behavior: Behavior = {}, options: Parameters<typeof platformaticAdapter>[0] = {}) => {
  const { module, calls } = fakeClient(behavior)
  const adapter = platformaticAdapter({ client: module, ...options })
  await adapter.connect(broker)
  return { adapter, calls }
}

const consuming = async (behavior: Behavior = {}, options: Parameters<typeof platformaticAdapter>[0] = {}, consume: { concurrency?: number, onError?: (error: unknown) => void, hold?: (message: RawMessage) => Promise<void>, topics?: string[] } = {}) => {
  const { adapter, calls } = await connected(behavior, options)
  const received: RawMessage[] = []
  const errors: unknown[] = []
  const handle = await adapter.consume({
    groupId: 'g',
    topics: consume.topics ?? ['t'],
    ...(consume.concurrency !== undefined && { concurrency: consume.concurrency }),
    onError: consume.onError ?? ((error) => { errors.push(error) }),
    eachMessage: async (raw) => {
      await consume.hold?.(raw)
      received.push(raw)
    }
  })
  const consumer = calls.consumers[0]!
  return { adapter, calls, handle, received, errors, consumer, stream: () => consumer.streams[consumer.streams.length - 1]! }
}

describe('platformaticAdapter', () => {
  test('rejects a passthrough of the options the adapter owns', () => {
    for (const key of ['autocommit', 'deserializers', 'registry', 'beforeDeserialization', 'groupId']) {
      assert.throws(() => platformaticAdapter({ consumer: { [key]: true } as never }), (error: unknown) => {
        assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
        assert.match((error as Error).message, new RegExp(`"${key}" is managed by the adapter`))
        return true
      })
      assert.throws(() => platformaticAdapter({ global: { [key]: 1 } as never }), { code: ERROR_CODES.CONFIG_INVALID })
    }
    for (const key of ['acks', 'idempotent', 'serializers', 'registry', 'beforeSerialization', 'transactionalId']) {
      assert.throws(() => platformaticAdapter({ producer: { [key]: 1 } as never }), { code: ERROR_CODES.CONFIG_INVALID })
      assert.throws(() => platformaticAdapter({ global: { [key]: 1 } as never }), { code: ERROR_CODES.CONFIG_INVALID })
    }
    for (const key of ['clientId', 'bootstrapBrokers']) {
      assert.throws(() => platformaticAdapter({ admin: { [key]: 'x' } as never }), (error: unknown) => {
        assert.match((error as Error).message, new RegExp(`"${key}" comes from the harbor configuration`))
        return true
      })
    }
    assert.doesNotThrow(() => platformaticAdapter({ consumer: { sessionTimeout: 10_000 }, producer: { compression: 'gzip' }, global: { timeout: 1000 } }))
    for (const [name, value] of [['bufferedMessages', 0], ['reconnectDelayMs', -1], ['adminTimeoutMs', 1.5]] as const) {
      assert.throws(() => platformaticAdapter({ [name]: value }), (error: unknown) => {
        assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
        assert.match((error as Error).message, new RegExp(`${name} must be a positive integer`))
        return true
      })
    }
  })

  test('connect maps the harbor config, pins acks, idempotence and bounded retries, and lets the passthrough tune the rest', async () => {
    const { module, calls } = fakeClient()
    const adapter = platformaticAdapter({ client: module, global: { timeout: 1000 }, producer: { retries: 7, compression: 'gzip' }, admin: { retries: 1 } })
    await adapter.connect({ ...broker, ssl: true, sasl: { mechanism: 'scram-sha-256', username: 'u', password: 'p' } })
    assert.deepEqual(calls.producerOptions, {
      clientId: 'app',
      bootstrapBrokers: ['b:9092'],
      tls: {},
      sasl: { mechanism: 'SCRAM-SHA-256', username: 'u', password: 'p' },
      timeout: 1000,
      retries: 7,
      compression: 'gzip',
      acks: -1,
      idempotent: true
    })
    assert.deepEqual(calls.adminOptions, {
      clientId: 'app',
      bootstrapBrokers: ['b:9092'],
      tls: {},
      sasl: { mechanism: 'SCRAM-SHA-256', username: 'u', password: 'p' },
      timeout: 1000,
      retries: 1
    })
    // A second connect is a no-op.
    await adapter.connect(broker)
    assert.equal((calls.producerOptions as { clientId: string }).clientId, 'app')
    await adapter.disconnect()
  })

  test('the producer retries a bounded number of times by default, and plain brokers get neither tls nor sasl', async () => {
    const { calls, adapter } = await connected()
    assert.deepEqual(calls.producerOptions, { clientId: 'app', bootstrapBrokers: ['b:9092'], retries: 3, acks: -1, idempotent: true })
    await adapter.disconnect()
  })

  test('a failed connect closes what it opened and wraps the error; a client that cannot be loaded is a definitive adapter error', async () => {
    const { module, calls } = fakeClient({ connectError: networkError() })
    const adapter = platformaticAdapter({ client: module })
    await assert.rejects(adapter.connect(broker), (error: unknown) => {
      assert.ok(error instanceof AdapterError)
      assert.match(error.message, /connect failed: connection refused/)
      assert.equal(error.retryable, true)
      return true
    })
    assert.deepEqual(calls.closed.sort(), ['admin', 'producer'])
    await assert.rejects(adapter.produce([]), { code: ERROR_CODES.ADAPTER, message: /not connected/ })

    const missing = platformaticAdapter({ client: async () => { throw new Error('Cannot find module') } })
    await assert.rejects(missing.connect(broker), (error: unknown) => {
      assert.ok(error instanceof AdapterError)
      assert.equal(error.retryable, false)
      assert.match(error.message, /@platformatic\/kafka could not be loaded/)
      assert.match((error.cause as Error).message, /Cannot find module/)
      return true
    })
  })

  test('produce maps records: bytes for key and value, headers as byte pairs, an explicit partition, and a tombstone stays null', async () => {
    const { adapter, calls } = await connected()
    await adapter.produce([
      { topic: 'a', key: Buffer.from('k'), value: Buffer.from('v'), headers: { 'x-a': 'one', 'x-b': 'two' } },
      { topic: 'a', key: null, value: null, headers: {}, partition: 3 }
    ])
    const [sent] = calls.sent as [{ messages: Array<{ topic: string, key?: Buffer, value?: Buffer, headers: Map<Buffer, Buffer>, partition?: number }> }]
    assert.equal(sent.messages.length, 2)
    const [first, second] = sent.messages as [typeof sent.messages[number], typeof sent.messages[number]]
    assert.equal(first.topic, 'a')
    assert.deepEqual(first.key, Buffer.from('k'))
    assert.deepEqual(first.value, Buffer.from('v'))
    assert.deepEqual([...first.headers].map(([name, value]) => [name.toString(), value.toString()]), [['x-a', 'one'], ['x-b', 'two']])
    assert.equal('partition' in first, false)
    assert.equal('key' in second, false)
    assert.equal('value' in second, false)
    assert.equal(second.partition, 3)
    await adapter.disconnect()
  })

  test('produce failures are classified: an answer Kafka marks as not retriable, an authentication or user error is definitive; the rest is transient', async () => {
    const cases: Array<[unknown, boolean, RegExp]> = [
      [protocolError('MESSAGE_TOO_LARGE', false), false, /produce failed: MESSAGE_TOO_LARGE/],
      [protocolError('NOT_LEADER_OR_FOLLOWER', true), true, /NOT_LEADER_OR_FOLLOWER/],
      [networkError(), true, /connection refused/],
      [multiple([networkError(), multiple([protocolError('NOT_LEADER_OR_FOLLOWER', true)])]), true, /several things failed/],
      [multiple([networkError()], Object.assign(new Error('bad credentials'), { code: 'PLT_KFK_AUTHENTICATION' })), false, /several things failed/],
      [Object.assign(new Error('Failed to serialize a message.'), { code: 'PLT_KFK_USER' }), false, /serialize/],
      [Object.assign(new Error('zstd is not available'), { code: 'PLT_KFK_UNSUPPORTED_COMPRESSION', canRetry: false }), false, /zstd/],
      [Object.assign(new Error('timed out'), { code: 'PLT_KFK_TIMEOUT', canRetry: false }), true, /timed out/],
      [new Error('plain'), true, /plain/],
      ['a string', true, /a string/]
    ]
    for (const [failure, retryable, pattern] of cases) {
      const { adapter } = await connected({ sendError: failure })
      await assert.rejects(adapter.produce([{ topic: 'a', key: null, value: Buffer.from('v'), headers: {} }]), (error: unknown) => {
        assert.ok(error instanceof AdapterError)
        assert.equal(error.retryable, retryable, `retryable for ${String(failure)}`)
        assert.match(error.message, pattern)
        assert.equal(error.cause, failure)
        return true
      })
      await adapter.disconnect()
    }
  })

  test('an error whose causes form a cycle is still classified', async () => {
    const loop = Object.assign(new Error('loop'), { code: 'PLT_KFK_NETWORK' }) as Error & { cause?: unknown }
    loop.cause = loop
    const { adapter } = await connected({ sendError: loop })
    await assert.rejects(adapter.produce([{ topic: 'a', key: null, value: null, headers: {} }]), (error: unknown) => {
      assert.equal((error as AdapterError).retryable, true)
      return true
    })
    await adapter.disconnect()
  })
})

describe('platformaticAdapter: consume', () => {
  test('opens one client consumer per call that never commits on its own, starting from the committed offset with fromBeginning as the fallback', async () => {
    const { module, calls } = fakeClient()
    const adapter = platformaticAdapter({ client: module, global: { timeout: 1000 }, consumer: { sessionTimeout: 10_000 } })
    await adapter.connect({ ...broker, ssl: true })
    await adapter.consume({ groupId: 'g', topics: ['a'], fromBeginning: true, eachMessage: async () => {} })
    await adapter.consume({ groupId: 'g', topics: ['a-retry-1'], eachMessage: async () => {} })
    assert.equal(calls.consumers.length, 2)
    assert.deepEqual(calls.consumers[0]!.options, { clientId: 'app', bootstrapBrokers: ['b:9092'], tls: {}, timeout: 1000, sessionTimeout: 10_000, groupId: 'g', autocommit: false })
    assert.deepEqual(calls.consumeOptions, [
      { topics: ['a'], autocommit: false, mode: 'committed', fallbackMode: 'earliest', partitionAssigner: partitionAssignerBySubscription },
      { topics: ['a-retry-1'], autocommit: false, mode: 'committed', fallbackMode: 'latest', partitionAssigner: partitionAssignerBySubscription }
    ])
    await adapter.disconnect()
    assert.deepEqual(calls.closed, [true, true, 'producer', 'admin'])

    // A passthrough assigner takes the place of the adapter's.
    const own = () => []
    const custom = await connected({}, { consumer: { partitionAssigner: own } })
    await custom.adapter.consume({ groupId: 'g', topics: ['a'], eachMessage: async () => {} })
    assert.equal((custom.calls.consumeOptions[0] as { partitionAssigner: unknown }).partitionAssigner, own)
    await custom.adapter.disconnect()
  })

  test('partitions are assigned round-robin among the members subscribed to each topic, not among every member of the group', () => {
    const members = new Map([
      ['b', { memberId: 'b', version: 1, topics: ['orders', 'orders-retry-1'] }],
      ['a', { memberId: 'a', version: 1, topics: ['orders'] }],
      ['c', { memberId: 'c', version: 1 }]
    ])
    const metadata = { topics: new Map([['orders', { partitionsCount: 4 }], ['orders-retry-1', { partitionsCount: 2 }], ['orders-dlq', { partitionsCount: 1 }]]) }
    // The client hands the leader's own topics only ('a' leads here); the other members' topics are placed as well.
    const assigned = partitionAssignerBySubscription('a', members as never, new Set(['orders', 'ghost']), metadata as never)
    const shape = assigned.map(({ memberId, assignments }) => [memberId, [...assignments.values()].map(({ topic, partitions }) => `${topic}:${partitions.join(',')}`)])
    assert.deepEqual(shape, [
      ['a', ['orders:0,3']],
      ['b', ['orders:1', 'orders-retry-1:0']],
      // A member whose subscription could not be read is offered every subscribed topic, the way the client would.
      ['c', ['orders:2', 'orders-retry-1:1']]
    ])
  })

  test('a consume that fails closes the consumer it opened and wraps the error', async () => {
    const { adapter, calls } = await connected({ consumeErrors: [protocolError('GROUP_AUTHORIZATION_FAILED', false)] })
    await assert.rejects(adapter.consume({ groupId: 'g', topics: ['a'], eachMessage: async () => {} }), (error: unknown) => {
      assert.ok(error instanceof AdapterError)
      assert.match(error.message, /consume failed: GROUP_AUTHORIZATION_FAILED/)
      assert.equal(error.retryable, false)
      return true
    })
    assert.deepEqual(calls.closed, [true])
    await adapter.disconnect()
  })

  test('delivers messages of a partition in order, one at a time, in the raw shape', async () => {
    let inFlight = 0
    let overlap = false
    const { handle, received, stream } = await consuming({}, {}, {
      hold: async () => {
        inFlight++
        if (inFlight > 1) overlap = true
        await new Promise((resolve) => setTimeout(resolve, 10))
        inFlight--
      }
    })
    stream().push(message({ offset: 0, key: Buffer.from('k'), value: Buffer.from('v'), headers: [['x-a', 'one'], ['x-b', 'two'], ['x-b', 'three'], ['x-b', 'four']], timestamp: 1_234n, leaderEpoch: 2 }))
    stream().push(message({ offset: 1 }))
    stream().push(message({ offset: 2, timestamp: 0n }))
    await until(() => received.length === 3, 'three deliveries')
    assert.equal(overlap, false)
    assert.deepEqual(received.map((raw) => raw.offset), ['0', '1', '2'])
    const [first, second, third] = received as [RawMessage, RawMessage, RawMessage]
    assert.equal(first.topic, 't')
    assert.equal(first.partition, 0)
    assert.deepEqual(first.key, Buffer.from('k'))
    assert.deepEqual(first.value, Buffer.from('v'))
    assert.deepEqual(first.headers, { 'x-a': Buffer.from('one'), 'x-b': [Buffer.from('two'), Buffer.from('three'), Buffer.from('four')] })
    assert.equal(first.timestamp, 1_234)
    assert.equal(second.key, null)
    assert.equal(second.value, null)
    assert.deepEqual(second.headers, {})
    assert.ok(third.timestamp > 1_000_000_000_000, 'a record without a timestamp is stamped now')
    await handle.stop()
  })

  test('partitions run concurrently up to the concurrency option, each in order', async () => {
    let inFlight = 0
    let maxInFlight = 0
    const { handle, received, stream } = await consuming({}, {}, {
      concurrency: 2,
      hold: async () => {
        inFlight++
        maxInFlight = Math.max(maxInFlight, inFlight)
        await new Promise((resolve) => setTimeout(resolve, 20))
        inFlight--
      }
    })
    for (const partition of [0, 1, 2]) {
      for (const offset of [0, 1]) stream().push(message({ partition, offset }))
    }
    await until(() => received.length === 6, 'six deliveries')
    assert.equal(maxInFlight, 2)
    for (const partition of [0, 1, 2]) {
      assert.deepEqual(received.filter((raw) => raw.partition === partition).map((raw) => raw.offset), ['0', '1'])
    }
    await handle.stop()
  })

  test('a worker slot takes one message and moves on: a partition with a long queue does not starve the others', async () => {
    const { handle, received, stream } = await consuming({}, {}, { hold: async () => { await new Promise((resolve) => setTimeout(resolve, 2)) } })
    for (const offset of [0, 1, 2, 3]) stream().push(message({ partition: 0, offset }))
    stream().push(message({ partition: 1, offset: 0 }))
    stream().push(message({ partition: 2, offset: 0 }))
    await until(() => received.length === 6, 'six deliveries')
    assert.deepEqual(received.map((raw) => `${raw.partition}@${raw.offset}`), ['0@0', '1@0', '2@0', '0@1', '0@2', '0@3'])
    await handle.stop()
  })

  test('a partition that comes back after being lost is read from a new stream, never from the old buffer', async () => {
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const { handle, received, stream, consumer, errors } = await consuming({}, {}, { hold: async (raw) => { if (raw.offset === '44') await gate } })
    consumer.assignments = [{ topic: 't', partitions: [0] }]
    consumer.emit('consumer:group:join', { groupId: 'g', memberId: 'm' })
    const first = stream()
    first.push(message({ offset: 44 }))
    first.push(message({ offset: 45 }))
    first.push(message({ offset: 46 }))
    await until(() => first.readableLength === 0, 'taken in')
    // Lost while offset 44 is in flight: 45 and 46 are dropped.
    consumer.assignments = []
    consumer.emit('consumer:group:join', { groupId: 'g', memberId: 'm' })
    // Meanwhile the old stream still holds what it fetched before the loss.
    first.push(message({ offset: 50 }))
    first.push(message({ offset: 51 }))
    // Back again: the old stream is closed and a new one opened, so 50 and 51 never surface.
    consumer.assignments = [{ topic: 't', partitions: [0] }]
    consumer.emit('consumer:group:join', { groupId: 'g', memberId: 'm' })
    await until(() => consumer.streams.length === 2, 'a fresh stream')
    assert.equal(first.readableEnded || first.destroyed || first.closed, true)
    release()
    await until(() => received.length === 1, 'the in-flight delivery')
    // The new stream starts from what the group committed: 45 comes again and is delivered.
    stream().push(message({ offset: 45 }))
    stream().push(message({ offset: 46 }))
    await until(() => received.length === 3, 'the refetched messages')
    await settle()
    assert.deepEqual(received.map((raw) => raw.offset), ['44', '45', '46'])
    assert.equal(errors.length, 0, 'a rotation is not a failure')
    // A join that changes nothing leaves the stream alone.
    consumer.emit('consumer:group:join', { groupId: 'g', memberId: 'm' })
    await settle()
    assert.equal(consumer.streams.length, 2)
    await handle.stop()
  })

  test('stops reading the stream once bufferedMessages are waiting and reads on as they drain', async () => {
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const { handle, received, stream } = await consuming({}, { bufferedMessages: 1 }, {
      hold: async (raw) => { if (raw.offset === '0') await gate }
    })
    for (const offset of [0, 1, 2, 3]) stream().push(message({ offset }))
    await settle()
    assert.equal(received.length, 0)
    // One in flight, one queued, one in the adapter's hands waiting for room: the last stays in the client's stream.
    assert.equal(stream().readableLength, 1)
    release()
    await until(() => received.length === 4, 'four deliveries')
    assert.deepEqual(received.map((raw) => raw.offset), ['0', '1', '2', '3'])
    await handle.stop()
  })

  test('a message fetched again after a rebalance is delivered once per consumption, and a lower offset is skipped', async () => {
    const { handle, received, stream } = await consuming()
    stream().push(message({ offset: 5 }))
    stream().push(message({ offset: 6 }))
    stream().push(message({ offset: 6 }))
    stream().push(message({ offset: 5 }))
    stream().push(message({ offset: 7 }))
    await until(() => received.length === 3, 'three deliveries')
    await settle()
    assert.deepEqual(received.map((raw) => raw.offset), ['5', '6', '7'])
    await handle.stop()
  })

  test('messages of a partition this member no longer owns are dropped: as they arrive, at their turn, and when the group is joined again', async () => {
    const releases = new Map<string, () => void>()
    const gates = new Map<string, Promise<void>>()
    for (const offset of ['0', '10']) gates.set(offset, new Promise<void>((resolve) => { releases.set(offset, resolve) }))
    const { handle, received, stream, consumer } = await consuming({}, {}, {
      hold: async (raw) => { await gates.get(raw.offset) }
    })
    consumer.assignments = [{ topic: 't', partitions: [0] }]
    // Partition 1 is someone else's: dropped as it arrives.
    stream().push(message({ partition: 1, offset: 0 }))
    stream().push(message({ partition: 0, offset: 0 }))
    stream().push(message({ partition: 0, offset: 1 }))
    stream().push(message({ partition: 0, offset: 2 }))
    await until(() => stream().readableLength === 0, 'the stream drained into the adapter')
    await settle()
    assert.equal(received.length, 0)
    // The rebalance completes while offset 0 is in flight: what was queued
    // for the lost partition goes when the client announces the new
    // generation, and the message in flight finishes.
    consumer.assignments = []
    consumer.emit('consumer:group:join', { groupId: 'g', memberId: 'm' })
    releases.get('0')!()
    await until(() => received.length === 1, 'the in-flight delivery')
    await settle()
    assert.deepEqual(received.map((raw) => `${raw.partition}@${raw.offset}`), ['0@0'])

    // The partition comes back: its messages are fresh again, whatever offsets were seen before.
    consumer.assignments = [{ topic: 't', partitions: [0] }]
    stream().push(message({ partition: 0, offset: 1 }))
    await until(() => received.length === 2, 'the redelivered message')

    // Without an announcement, a queued message is checked at its turn.
    stream().push(message({ partition: 0, offset: 10 }))
    stream().push(message({ partition: 0, offset: 11 }))
    await until(() => stream().readableLength === 0, 'both taken in')
    await settle()
    consumer.assignments = []
    releases.get('10')!()
    await until(() => received.length === 3, 'the in-flight delivery of offset 10')
    await settle()
    assert.equal(received.length, 3, 'offset 11 was skipped at its turn')
    // A queue already emptied is left alone by the announcement.
    consumer.emit('consumer:group:join', { groupId: 'g', memberId: 'm' })
    await handle.stop()
  })

  test('pause holds the partition and resume restarts it', async () => {
    const { handle, received, stream } = await consuming()
    stream().push(message({ offset: 0 }))
    await until(() => received.length === 1, 'first delivery')
    handle.pause!([{ topic: 't', partition: 0 }])
    stream().push(message({ offset: 1 }))
    stream().push(message({ partition: 1, offset: 0 }))
    await until(() => received.length === 2, 'the other partition keeps flowing')
    await settle()
    assert.equal(received.length, 2)
    handle.resume!([{ topic: 't', partition: 0 }])
    await until(() => received.length === 3, 'delivery after resume')
    assert.equal(received[2]?.offset, '1')
    // Pausing while a delivery is in flight holds the next one.
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const held = await consuming({}, {}, { hold: async (raw) => { if (raw.offset === '0') await gate } })
    held.stream().push(message({ offset: 0 }))
    held.stream().push(message({ offset: 1 }))
    await settle()
    held.handle.pause!([{ topic: 't', partition: 0 }])
    release()
    await until(() => held.received.length === 1, 'the in-flight delivery')
    await settle()
    assert.equal(held.received.length, 1)
    held.handle.resume!([{ topic: 't', partition: 0 }])
    await until(() => held.received.length === 2, 'after resume')
    await handle.stop()
    await held.handle.stop()
  })

  test('commit sends bigint offsets with the leader epoch of the last delivered record, -1 when none, and wraps failures', async () => {
    const { handle, received, stream, calls } = await consuming()
    stream().push(message({ offset: 4, leaderEpoch: 7 }))
    await until(() => received.length === 1, 'delivery')
    await handle.commit([{ topic: 't', partition: 0, offset: '5' }, { topic: 't', partition: 1, offset: '2' }])
    assert.deepEqual(calls.committed, [{
      offsets: [
        { topic: 't', partition: 0, offset: 5n, leaderEpoch: 7 },
        { topic: 't', partition: 1, offset: 2n, leaderEpoch: -1 }
      ]
    }])
    await handle.stop()

    const failing = await consuming({ commitError: protocolError('REBALANCE_IN_PROGRESS', true) })
    await assert.rejects(failing.handle.commit([{ topic: 't', partition: 0, offset: '1' }]), (error: unknown) => {
      assert.ok(error instanceof AdapterError)
      assert.match(error.message, /commit failed: REBALANCE_IN_PROGRESS/)
      assert.equal(error.retryable, true)
      return true
    })
    await failing.handle.stop()
  })

  test('stop closes the consumer with force and ends the read loop; a close failure still ends the loop and is wrapped', async () => {
    const { handle, calls, stream } = await consuming()
    await handle.stop()
    assert.deepEqual(calls.closed, [true])
    assert.equal(stream().readableEnded || stream().destroyed, true)

    const failing = await consuming({ closeError: new Error('cannot leave') })
    await assert.rejects(failing.handle.stop(), (error: unknown) => {
      assert.ok(error instanceof AdapterError)
      assert.match(error.message, /consumer disconnect failed: cannot leave/)
      return true
    })
    assert.equal(failing.stream().destroyed, true)
  })

  test('a stream the client gives up on is reported and replaced after the reconnect delay, and so is one that ends on its own', async () => {
    const { handle, received, errors, stream, consumer, calls } = await consuming({ consumeErrors: [undefined, undefined, new Error('rejoin refused')] }, { reconnectDelayMs: 5 })
    const first = stream()
    first.destroy(new Error('fetch gave up'))
    await until(() => consumer.streams.length === 2, 'a second stream')
    assert.equal(errors.length, 1)
    assert.ok(errors[0] instanceof AdapterError)
    assert.match((errors[0] as Error).message, /consume failed: fetch gave up/)
    stream().push(message({ offset: 0 }))
    await until(() => received.length === 1, 'delivery on the new stream')

    // A stream that ends without the adapter asking is reported the same way.
    stream().end()
    await until(() => errors.length === 2, 'the end reported')
    assert.match((errors[1] as Error).message, /the client closed the message stream/)
    // The next consume fails once, is reported, and is tried again.
    await until(() => consumer.streams.length === 3, 'a third stream')
    assert.match((errors[2] as Error).message, /consume failed: rejoin refused/)
    assert.equal(calls.consumeOptions.length, 4)
    await handle.stop()
  })

  test('stop during the reconnect wait ends the wait at once and opens no new stream', async () => {
    const { handle, errors, stream, calls } = await consuming({}, { reconnectDelayMs: 60_000 })
    stream().destroy(new Error('fetch gave up'))
    await until(() => errors.length === 1, 'the failure reported')
    const startedAt = Date.now()
    await handle.stop()
    assert.ok(Date.now() - startedAt < 1_000, 'stop did not sit out the reconnect delay')
    assert.equal(calls.consumeOptions.length, 1)
  })

  test('a client error drops the stream so the next one joins the group again; heartbeat errors are reported; nothing after stop', async () => {
    const { handle, errors, consumer, stream, received } = await consuming({}, { reconnectDelayMs: 5 })
    consumer.emit('consumer:heartbeat:error', { groupId: 'g', error: networkError() })
    assert.equal(errors.length, 1)
    assert.match((errors[0] as Error).message, /heartbeat failed: connection refused/)
    consumer.emit('error', new Error('rejoin gave up'))
    await until(() => consumer.streams.length === 2, 'a new stream after the client error')
    assert.match((errors[1] as Error).message, /consume failed: rejoin gave up/)
    stream().push(message({ offset: 0 }))
    await until(() => received.length === 1, 'delivery on the new stream')
    await handle.stop()
    consumer.emit('error', new Error('late'))
    consumer.emit('error', 'not even an error')
    assert.equal(errors.length, 2)

    const { adapter, calls } = await connected()
    const quiet = await adapter.consume({ groupId: 'g', topics: ['t'], eachMessage: async () => {} })
    assert.doesNotThrow(() => calls.consumers[0]!.emit('error', new Error('unheard')))
    await quiet.stop()
    await adapter.disconnect()
  })

  test('a rejected eachMessage does not stop the partition', async () => {
    const { adapter, calls } = await connected()
    const seen: string[] = []
    const handle = await adapter.consume({
      groupId: 'g',
      topics: ['t'],
      eachMessage: async (raw) => {
        seen.push(raw.offset)
        if (raw.offset === '0') throw new Error('handler blew up')
      }
    })
    const stream = calls.consumers[0]!.streams[0]!
    stream.push(message({ offset: 0 }))
    stream.push(message({ offset: 1 }))
    await until(() => seen.length === 2, 'both deliveries')
    await handle.stop()
    await adapter.disconnect()
  })
})

describe('platformaticAdapter: admin', () => {
  test('createTopics maps each spec in a call of its own, treats an existing topic as success and waits until the topics are visible', async () => {
    let looks = 0
    const { adapter, calls } = await connected({ topics: () => (++looks >= 3 ? ['a', 'b'] : ['a']) })
    await adapter.admin.createTopics([
      { topic: 'a', partitions: 3, replicationFactor: 2, config: { 'retention.ms': '1000' } },
      { topic: 'b' }
    ])
    assert.deepEqual(calls.created, [
      { topics: [{ topic: 'a', partitions: 3, replicas: 2 }], configs: [{ name: 'retention.ms', value: '1000' }] },
      { topics: [{ topic: 'b', partitions: 1, replicas: 1 }] }
    ])
    assert.equal(calls.listTopicsCalls, 3)
    await adapter.disconnect()

    const existing = await connected({ topics: ['a'], createTopicsError: multiple([multiple([protocolError('TOPIC_ALREADY_EXISTS', false)])]) })
    await existing.adapter.admin.createTopics([{ topic: 'a' }])
    await existing.adapter.disconnect()

    const refused = await connected({ createTopicsError: multiple([protocolError('TOPIC_ALREADY_EXISTS', false), protocolError('INVALID_PARTITIONS', false)]) })
    await assert.rejects(refused.adapter.admin.createTopics([{ topic: 'a' }]), (error: unknown) => {
      assert.ok(error instanceof AdapterError)
      assert.match(error.message, /createTopics failed/)
      assert.equal(error.retryable, false)
      return true
    })
    await refused.adapter.disconnect()

    const transport = await connected({ createTopicsError: networkError() })
    await assert.rejects(transport.adapter.admin.createTopics([{ topic: 'a' }]), { code: ERROR_CODES.ADAPTER, retryable: true })
    await transport.adapter.disconnect()
  })

  test('createTopics gives up at the admin timeout when a topic never shows up, and wraps a metadata failure', async () => {
    const { adapter } = await connected({ topics: [] }, { adminTimeoutMs: 120 })
    await assert.rejects(adapter.admin.createTopics([{ topic: 'ghost' }, { topic: 'other' }]), {
      code: ERROR_CODES.ADAPTER,
      message: /createTopics: ghost, other not visible in metadata after 120ms/
    })
    await adapter.disconnect()

    const failing = await connected({ listTopicsError: new Error('metadata down') })
    await assert.rejects(failing.adapter.admin.createTopics([{ topic: 'a' }]), { message: /listTopics failed: metadata down/ })
    await assert.rejects(failing.adapter.admin.topicExists('a'), { message: /listTopics failed: metadata down/ })
    await failing.adapter.disconnect()
  })

  test('topicExists tells the truth', async () => {
    const { adapter } = await connected({ topics: ['a'] })
    assert.equal(await adapter.admin.topicExists('a'), true)
    assert.equal(await adapter.admin.topicExists('b'), false)
    await adapter.disconnect()
  })

  test('fetchTopicOffsets asks the watermarks of the partitions with a leader, one call per watermark, and leaves out what the broker did not answer', async () => {
    const answer = (timestamp: bigint): OffsetsAnswer[] => [
      { name: 'a', partitions: [{ partitionIndex: 0, offset: timestamp === -1n ? 10n : 2n, timestamp, leaderEpoch: 0 }, { partitionIndex: 2, offset: timestamp === -1n ? 5n : 0n, timestamp, leaderEpoch: 0 }] },
      { name: 'b', partitions: timestamp === -1n ? [{ partitionIndex: 0, offset: 1n, timestamp, leaderEpoch: 0 }] : [] }
    ]
    const { adapter, calls } = await connected({ metadata: { a: [1, -1, 1], b: [1], c: [-1] }, offsets: answer })
    const offsets = await adapter.admin.fetchTopicOffsets!(['a', 'b', 'c'])
    assert.deepEqual(offsets, [
      { topic: 'a', partition: 0, low: '2', high: '10' },
      { topic: 'a', partition: 2, low: '0', high: '5' }
    ])
    assert.deepEqual(calls.metadataCalls, [{ topics: ['a', 'b', 'c'], forceUpdate: true }])
    assert.equal(calls.offsetQueries.length, 2)
    const [highQuery, lowQuery] = calls.offsetQueries as [{ topics: unknown[] }, { topics: unknown[] }]
    assert.deepEqual(highQuery.topics, [
      { name: 'a', partitions: [{ partitionIndex: 0, timestamp: -1n }, { partitionIndex: 2, timestamp: -1n }] },
      { name: 'b', partitions: [{ partitionIndex: 0, timestamp: -1n }] }
    ])
    assert.deepEqual(lowQuery.topics[1], { name: 'b', partitions: [{ partitionIndex: 0, timestamp: -2n }] })
    // No partition has a leader yet: nothing to ask, nothing reported.
    assert.deepEqual(await adapter.admin.fetchTopicOffsets!(['c']), [])
    assert.equal(calls.offsetQueries.length, 2)
    await adapter.disconnect()

    const failing = await connected({ metadata: { a: [1] }, offsetsError: networkError() })
    await assert.rejects(failing.adapter.admin.fetchTopicOffsets!(['a']), { message: /listOffsets failed: connection refused/ })
    await assert.rejects(failing.adapter.admin.fetchCommittedOffsets!('g', ['a']), { message: /listConsumerGroupOffsets failed: connection refused/ })
    await failing.adapter.disconnect()

    // The client reports a topic it cannot find as a user error: definitive.
    const unknown = await connected({ metadataError: Object.assign(new Error('Unknown topic a.'), { code: 'PLT_KFK_USER' }) })
    await assert.rejects(unknown.adapter.admin.fetchTopicOffsets!(['a']), { message: /metadata failed: Unknown topic a\./, retryable: false })
    await unknown.adapter.disconnect()

    // A leader that moved between the metadata and the request: nothing reported this time, no failure either.
    const moving = await connected({ metadata: { a: [1] }, offsetsError: multiple([Object.assign(protocolError('NOT_LEADER_OR_FOLLOWER', true), { hasStaleMetadata: true })]) })
    assert.deepEqual(await moving.adapter.admin.fetchTopicOffsets!(['a']), [])
    await moving.adapter.disconnect()
  })

  test('fetchCommittedOffsets asks the partitions of each topic and reads a negative committed offset as null', async () => {
    const { adapter, calls } = await connected({
      metadata: { a: [1, 1], b: [1] },
      committed: [{ groupId: 'g', topics: [{ name: 'a', partitions: [{ partitionIndex: 0, committedOffset: 7n }, { partitionIndex: 1, committedOffset: -1n }] }, { name: 'b', partitions: [{ partitionIndex: 0, committedOffset: 0n }] }] }]
    })
    assert.deepEqual(await adapter.admin.fetchCommittedOffsets!('g', ['a', 'b']), [
      { topic: 'a', partition: 0, offset: '7' },
      { topic: 'a', partition: 1, offset: null },
      { topic: 'b', partition: 0, offset: '0' }
    ])
    assert.deepEqual(calls.offsetQueries, [{ groups: [{ groupId: 'g', topics: [{ name: 'a', partitionIndexes: [0, 1] }, { name: 'b', partitionIndexes: [0] }] }] }])
    await adapter.disconnect()
  })

  test('disconnect stops every consumption and closes producer and admin; a failure is wrapped; nothing works before connect', async () => {
    const { adapter, calls } = await connected()
    const handle: ConsumerHandle = await adapter.consume({ groupId: 'g', topics: ['t'], eachMessage: async () => {} })
    await adapter.disconnect()
    assert.deepEqual(calls.closed, [true, 'producer', 'admin'])
    await assert.rejects(adapter.produce([]), { message: /not connected/ })
    await assert.rejects(adapter.consume({ groupId: 'g', topics: ['t'], eachMessage: async () => {} }), { message: /not connected/ })
    await assert.rejects(adapter.admin.createTopics([{ topic: 'a' }]), { message: /not connected/ })
    await assert.rejects(adapter.admin.topicExists('a'), { message: /not connected/ })
    await assert.rejects(adapter.admin.fetchTopicOffsets!(['a']), { message: /not connected/ })
    await assert.rejects(adapter.admin.fetchCommittedOffsets!('g', ['a']), { message: /not connected/ })
    // Stopping a consumption the disconnect already stopped is harmless.
    await handle.stop()

    const failing = await connected({ producerCloseError: new Error('socket gone') })
    await assert.rejects(failing.adapter.disconnect(), (error: unknown) => {
      assert.ok(error instanceof AdapterError)
      assert.match(error.message, /disconnect failed: socket gone/)
      return true
    })
  })
})

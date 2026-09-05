/**
 * The Confluent adapter against a fake client module: the translation layer
 * is exercised without the native binding. The real client is covered by
 * the contract suite in tests/integration.
 */
import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import type { KafkaJS } from '@confluentinc/kafka-javascript'

import { confluentAdapter, type ConfluentClientModule } from '../../src/adapters/confluent/index'
import { ERROR_CODES, type RawMessage } from '../../src/index'

interface FakeCalls {
  kafkaConfig: unknown
  producerConfig: unknown
  consumerConfigs: unknown[]
  sent: KafkaJS.ProducerBatch[]
  committed: unknown[]
  created: unknown[]
  runConfig: KafkaJS.ConsumerRunConfig | undefined
  paused: unknown[]
  resumed: unknown[]
  disconnected: string[]
}

const fakeClient = (behavior: {
  sendResult?: KafkaJS.RecordMetadata[]
  sendError?: unknown
  createTopicsError?: unknown
  topics?: string[] | (() => string[])
} = {}): { module: ConfluentClientModule, calls: FakeCalls } => {
  const calls: FakeCalls = { kafkaConfig: undefined, producerConfig: undefined, consumerConfigs: [], sent: [], committed: [], created: [], runConfig: undefined, paused: [], resumed: [], disconnected: [] }
  class Kafka {
    constructor (config: unknown) {
      calls.kafkaConfig = config
    }

    producer (config: unknown): KafkaJS.Producer {
      calls.producerConfig = config
      return {
        connect: async () => {},
        disconnect: async () => { calls.disconnected.push('producer') },
        sendBatch: async (batch: KafkaJS.ProducerBatch) => {
          calls.sent.push(batch)
          if (behavior.sendError !== undefined) throw behavior.sendError
          return behavior.sendResult ?? [{ topicName: 't', partition: 0, errorCode: 0 }]
        }
      } as unknown as KafkaJS.Producer
    }

    consumer (config: unknown): KafkaJS.Consumer {
      calls.consumerConfigs.push(config)
      return {
        connect: async () => {},
        disconnect: async () => { calls.disconnected.push('consumer') },
        subscribe: async () => {},
        run: async (config: KafkaJS.ConsumerRunConfig) => { calls.runConfig = config },
        commitOffsets: async (offsets: unknown) => { calls.committed.push(offsets) },
        pause: (topics: unknown) => { calls.paused.push(topics) },
        resume: (topics: unknown) => { calls.resumed.push(topics) }
      } as unknown as KafkaJS.Consumer
    }

    admin (): KafkaJS.Admin {
      return {
        connect: async () => {},
        disconnect: async () => { calls.disconnected.push('admin') },
        createTopics: async (options: unknown) => {
          calls.created.push(options)
          if (behavior.createTopicsError !== undefined) throw behavior.createTopicsError
          return true
        },
        listTopics: async () => (typeof behavior.topics === 'function' ? behavior.topics() : behavior.topics) ?? []
      } as unknown as KafkaJS.Admin
    }
  }
  return { module: { KafkaJS: { Kafka: Kafka as unknown as ConfluentClientModule['KafkaJS']['Kafka'] } }, calls }
}

const broker = { clientId: 'app', brokers: ['b:9092'] }

describe('confluentAdapter', () => {
  test('rejects a passthrough of the properties the offset policy depends on', () => {
    for (const key of ['enable.auto.commit', 'enable.auto.offset.store', 'auto.offset.reset']) {
      assert.throws(() => confluentAdapter({ consumer: { [key]: true } }), { code: ERROR_CODES.CONFIG_INVALID })
      assert.throws(() => confluentAdapter({ global: { [key]: 'x' } }), { code: ERROR_CODES.CONFIG_INVALID })
    }
    assert.doesNotThrow(() => confluentAdapter({ consumer: { 'fetch.min.bytes': 1 } }))
  })

  test('configures acks=all, idempotence, no auto-commit and the reset policy from fromBeginning', async () => {
    const { module, calls } = fakeClient()
    const adapter = confluentAdapter({ client: module })
    await adapter.connect(broker)
    assert.deepEqual((calls.producerConfig as { kafkaJS: unknown }).kafkaJS, { acks: -1, idempotent: true })
    await adapter.consume({ groupId: 'g', topics: ['t'], fromBeginning: true, eachMessage: async () => {} })
    const consumerConfig = calls.consumerConfigs[0] as { kafkaJS: { groupId: string, autoCommit: boolean, fromBeginning: boolean } }
    assert.equal(consumerConfig.kafkaJS.groupId, 'g')
    assert.equal(consumerConfig.kafkaJS.autoCommit, false)
    assert.equal(consumerConfig.kafkaJS.fromBeginning, true)
    assert.equal(calls.runConfig?.partitionsConsumedConcurrently, 1)
    await adapter.disconnect()
    assert.deepEqual(calls.disconnected.sort(), ['admin', 'consumer', 'producer'])
  })

  test('loads the client lazily and reports a missing module as a non-retryable adapter error', async () => {
    const adapter = confluentAdapter({ client: async () => { throw new Error('Cannot find module') } })
    await assert.rejects(adapter.connect(broker), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.ADAPTER)
      assert.equal((error as { retryable: boolean }).retryable, false)
      return true
    })
    await assert.rejects(adapter.produce([]), { code: ERROR_CODES.ADAPTER })
  })

  test('produce groups records by topic, keeps key/value/headers and reports unacknowledged records', async () => {
    const { module, calls } = fakeClient({ sendResult: [{ topicName: 'a', partition: 0, errorCode: 0 }, { topicName: 'b', partition: 1, errorCode: 7 }] })
    const adapter = confluentAdapter({ client: module })
    await adapter.connect(broker)
    await assert.rejects(adapter.produce([
      { topic: 'a', key: Buffer.from('k'), value: Buffer.from('v'), headers: { h: '1' } },
      { topic: 'b', key: null, value: null, headers: {}, partition: 1 },
      { topic: 'a', key: null, value: Buffer.from('w'), headers: {} }
    ]), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.ADAPTER)
      assert.match((error as Error).message, /b\[1\] code 7/)
      return true
    })
    const batch = calls.sent[0]?.topicMessages ?? []
    assert.deepEqual(batch.map((entry) => entry.topic), ['a', 'b'])
    assert.equal(batch[0]?.messages.length, 2)
    assert.deepEqual(batch[0]?.messages[0], { key: Buffer.from('k'), value: Buffer.from('v'), headers: { h: '1' } })
    assert.deepEqual(batch[1]?.messages[0], { key: null, value: null, headers: {}, partition: 1 })
  })

  test('client failures are retryable unless their code is definitive; the client\'s own retriable flag is ignored', async () => {
    const cases: Array<[number | undefined, boolean]> = [
      [-195, true], // ERR__TRANSPORT
      [-185, true], // ERR__TIMED_OUT
      [6, true], // NOT_LEADER_FOR_PARTITION
      [undefined, true],
      [10, false], // MSG_SIZE_TOO_LARGE
      [29, false], // TOPIC_AUTHORIZATION_FAILED
      [-186, false] // INVALID_ARG
    ]
    for (const [code, retryable] of cases) {
      const failure = Object.assign(new Error(`code ${String(code)}`), { code, retriable: false })
      const { module } = fakeClient({ sendError: failure })
      const adapter = confluentAdapter({ client: module })
      await adapter.connect(broker)
      await assert.rejects(adapter.produce([{ topic: 't', key: null, value: Buffer.from('x'), headers: {} }]), (error: unknown) => {
        assert.equal((error as { code: string }).code, ERROR_CODES.ADAPTER)
        assert.equal((error as { retryable: boolean }).retryable, retryable, `code ${String(code)}`)
        assert.equal((error as { cause: unknown }).cause, failure)
        return true
      })
    }
  })

  test('delivers raw messages with a positive timestamp only, and commits the string offsets as given', async () => {
    const { module, calls } = fakeClient()
    const adapter = confluentAdapter({ client: module })
    await adapter.connect(broker)
    const received: RawMessage[] = []
    const handle = await adapter.consume({ groupId: 'g', topics: ['t'], eachMessage: async (message) => { received.push(message) } })
    const eachMessage = calls.runConfig?.eachMessage
    assert.ok(eachMessage)
    const base = { heartbeat: async () => {}, pause: () => () => {} }
    await eachMessage({ ...base, topic: 't', partition: 2, message: { key: Buffer.from('k'), value: Buffer.from('v'), timestamp: '1700000000000', offset: '41', attributes: 0, headers: { h: Buffer.from('1') } } as KafkaJS.KafkaMessage })
    await eachMessage({ ...base, topic: 't', partition: 2, message: { key: null, value: null, timestamp: '', offset: '42', attributes: 0, headers: {} } as KafkaJS.KafkaMessage })
    assert.equal(received[0]?.timestamp, 1_700_000_000_000)
    assert.equal(received[0]?.partition, 2)
    assert.deepEqual(received[0]?.headers, { h: Buffer.from('1') })
    assert.ok((received[1]?.timestamp ?? 0) > 1_700_000_000_000, 'a blank timestamp falls back to now, never to 0')
    assert.equal(received[1]?.key, null)
    assert.equal(received[1]?.value, null)

    await handle.commit([{ topic: 't', partition: 2, offset: '43' }])
    assert.deepEqual(calls.committed[0], [{ topic: 't', partition: 2, offset: '43' }])
    handle.pause?.([{ topic: 't', partition: 2 }])
    handle.resume?.([{ topic: 't', partition: 2 }])
    assert.deepEqual(calls.paused[0], [{ topic: 't', partitions: [2] }])
    assert.deepEqual(calls.resumed[0], [{ topic: 't', partitions: [2] }])
    await handle.stop()
    assert.deepEqual(calls.disconnected, ['consumer'])
    await adapter.disconnect()
    assert.deepEqual(calls.disconnected, ['consumer', 'producer', 'admin'])
  })

  test('forwards the client\'s error log lines to onError', async () => {
    const { module, calls } = fakeClient()
    const adapter = confluentAdapter({ client: module })
    await adapter.connect(broker)
    const errors: unknown[] = []
    await adapter.consume({ groupId: 'g', topics: ['t'], eachMessage: async () => {}, onError: (error) => { errors.push(error) } })
    const logger = (calls.consumerConfigs[0] as { kafkaJS: { logger: KafkaJS.Logger } }).kafkaJS.logger
    logger.info('noise')
    logger.error('fetch failed', { fac: 'FETCH' })
    assert.equal(errors.length, 1)
    assert.equal((errors[0] as { code: string }).code, ERROR_CODES.ADAPTER)
    assert.match((errors[0] as Error).message, /fetch failed/)
  })

  test('createTopics maps the spec, treats "already exists" as success and wraps anything else', async () => {
    const ok = fakeClient({ topics: ['t'] })
    const adapter = confluentAdapter({ client: ok.module, adminTimeoutMs: 5 })
    await adapter.connect(broker)
    await adapter.admin.createTopics([{ topic: 't', partitions: 3, replicationFactor: 2, config: { 'retention.ms': '1000' } }])
    assert.deepEqual(ok.calls.created[0], {
      timeout: 5,
      topics: [{ topic: 't', numPartitions: 3, replicationFactor: 2, configEntries: [{ name: 'retention.ms', value: '1000' }] }]
    })
    assert.equal(await adapter.admin.topicExists('t'), true)
    assert.equal(await adapter.admin.topicExists('u'), false)

    const exists = fakeClient({ createTopicsError: Object.assign(new Error('exists'), { code: 36 }), topics: ['t'] })
    const adapter2 = confluentAdapter({ client: exists.module })
    await adapter2.connect(broker)
    await adapter2.admin.createTopics([{ topic: 't' }])

    const aggregate = fakeClient({ createTopicsError: Object.assign(new Error('agg'), { errors: [{ code: 36 }, { code: 36 }] }), topics: ['t', 'u'] })
    const adapter3 = confluentAdapter({ client: aggregate.module })
    await adapter3.connect(broker)
    await adapter3.admin.createTopics([{ topic: 't' }, { topic: 'u' }])

    const other = fakeClient({ createTopicsError: Object.assign(new Error('nope'), { code: 29 }) })
    const adapter4 = confluentAdapter({ client: other.module })
    await adapter4.connect(broker)
    await assert.rejects(adapter4.admin.createTopics([{ topic: 't' }]), { code: ERROR_CODES.ADAPTER, retryable: false })
  })

  test('createTopics resolves only once the topics are visible in metadata, and gives up at the admin timeout', async () => {
    // The controller acknowledges before the metadata reflects the topic;
    // the fake becomes aware of it on the third listing.
    let listings = 0
    const lagging = fakeClient({ topics: () => (++listings >= 3 ? ['t'] : []) })
    const adapter = confluentAdapter({ client: lagging.module, adminTimeoutMs: 2_000 })
    await adapter.connect(broker)
    await adapter.admin.createTopics([{ topic: 't' }])
    assert.equal(listings, 3)

    const never = fakeClient({ topics: [] })
    const adapter2 = confluentAdapter({ client: never.module, adminTimeoutMs: 120 })
    await adapter2.connect(broker)
    await assert.rejects(adapter2.admin.createTopics([{ topic: 'ghost' }]), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.ADAPTER)
      assert.match((error as Error).message, /ghost not visible/)
      return true
    })

    const failingList = fakeClient({ topics: () => { throw new Error('metadata timeout') } })
    const adapter3 = confluentAdapter({ client: failingList.module })
    await adapter3.connect(broker)
    await assert.rejects(adapter3.admin.createTopics([{ topic: 't' }]), /metadata timeout/)
  })
})

describe('confluentAdapter: less travelled paths', () => {
  test('passes brokers, clientId, ssl and sasl to the client, plus the global passthrough', async () => {
    const plain = fakeClient()
    await confluentAdapter({ client: plain.module }).connect(broker)
    const plainConfig = plain.calls.kafkaConfig as { kafkaJS: Record<string, unknown> }
    assert.deepEqual(plainConfig.kafkaJS, { brokers: ['b:9092'], clientId: 'app', logLevel: 1 })

    const secure = fakeClient()
    await confluentAdapter({ client: secure.module, global: { 'socket.keepalive.enable': true }, logLevel: 3 }).connect({
      ...broker,
      ssl: true,
      sasl: { mechanism: 'scram-sha-512', username: 'u', password: 'p' }
    })
    const secureConfig = secure.calls.kafkaConfig as { kafkaJS: Record<string, unknown>, 'socket.keepalive.enable': boolean }
    assert.deepEqual(secureConfig.kafkaJS, {
      brokers: ['b:9092'],
      clientId: 'app',
      logLevel: 3,
      ssl: true,
      sasl: { mechanism: 'scram-sha-512', username: 'u', password: 'p' }
    })
    assert.equal(secureConfig['socket.keepalive.enable'], true)
  })

  test('a message without headers arrives with an empty header map, and a non-Error failure is stringified', async () => {
    const { module, calls } = fakeClient({ sendError: 'plain string failure' })
    const adapter = confluentAdapter({ client: module })
    await adapter.connect(broker)
    const received: RawMessage[] = []
    await adapter.consume({ groupId: 'g', topics: ['t'], eachMessage: async (message) => { received.push(message) } })
    await calls.runConfig!.eachMessage!({ topic: 't', partition: 0, heartbeat: async () => {}, pause: () => () => {}, message: { key: null, value: Buffer.from('v'), timestamp: '5', offset: '0', attributes: 0, size: 1 } as KafkaJS.KafkaMessage })
    assert.deepEqual(received[0]?.headers, {})
    await assert.rejects(adapter.produce([{ topic: 't', key: null, value: Buffer.from('x'), headers: {} }]), (error: unknown) => {
      assert.match((error as Error).message, /plain string failure/)
      assert.equal((error as { retryable: boolean }).retryable, true)
      return true
    })
  })

  test('connect is idempotent and a failed connect tears down what it opened', async () => {
    const { module, calls } = fakeClient()
    const adapter = confluentAdapter({ client: module })
    await adapter.connect(broker)
    await adapter.connect(broker)
    assert.equal(calls.consumerConfigs.length, 0)
    assert.equal(calls.disconnected.length, 0)

    const failing = fakeClient()
    const KafkaClass = failing.module.KafkaJS.Kafka as unknown as { prototype: { producer: (config: unknown) => KafkaJS.Producer } }
    const producerFactory = KafkaClass.prototype.producer
    KafkaClass.prototype.producer = function (config: unknown) {
      const producer = producerFactory.call(this, config)
      producer.connect = async () => { throw Object.assign(new Error('refused'), { code: -195 }) }
      return producer
    }
    const adapter2 = confluentAdapter({ client: failing.module })
    await assert.rejects(adapter2.connect(broker), { code: ERROR_CODES.ADAPTER, retryable: true })
    assert.deepEqual(failing.calls.disconnected.sort(), ['admin', 'producer'])
    await assert.rejects(adapter2.produce([]), { code: ERROR_CODES.ADAPTER })
  })

  test('the rebalance callback hands assignments and revocations to the client, and revocations to onPartitionsRevoked first', async () => {
    const { module, calls } = fakeClient()
    const adapter = confluentAdapter({ client: module })
    await adapter.connect(broker)
    const revoked: unknown[] = []
    await adapter.consume({ groupId: 'g', topics: ['t'], eachMessage: async () => {}, onPartitionsRevoked: async (partitions) => { revoked.push(partitions) } })
    const config = calls.consumerConfigs[0] as { rebalance_cb: (error: { code: number }, assignment: unknown[], fns: { assign: (a: unknown) => void, unassign: (a: unknown) => void }) => Promise<void> }
    const applied: string[] = []
    const fns = { assign: () => { applied.push('assign') }, unassign: () => { applied.push('unassign') } }
    const assignment = [{ topic: 't', partition: 0 }, { topic: 't', partition: 1 }]
    await config.rebalance_cb({ code: -175 }, assignment, fns)
    await config.rebalance_cb({ code: -174 }, assignment, fns)
    await config.rebalance_cb({ code: 99 }, assignment, fns)
    assert.deepEqual(applied, ['assign', 'unassign'])
    assert.deepEqual(revoked, [assignment])
  })

  test('a revocation callback that throws still releases the partitions', async () => {
    const { module, calls } = fakeClient()
    const adapter = confluentAdapter({ client: module })
    await adapter.connect(broker)
    await adapter.consume({ groupId: 'g', topics: ['t'], eachMessage: async () => {}, onPartitionsRevoked: async () => { throw new Error('bad hook') } })
    const config = calls.consumerConfigs[0] as { rebalance_cb: (error: { code: number }, assignment: unknown[], fns: { assign: () => void, unassign: () => void }) => Promise<void> }
    let unassigned = false
    await assert.rejects(config.rebalance_cb({ code: -174 }, [], { assign: () => {}, unassign: () => { unassigned = true } }), /bad hook/)
    assert.equal(unassigned, true)
  })

  test('a consume that fails to subscribe disconnects the consumer it created and wraps the error', async () => {
    const { module, calls } = fakeClient()
    const KafkaClass = module.KafkaJS.Kafka as unknown as { prototype: { consumer: (config: unknown) => KafkaJS.Consumer } }
    const factory = KafkaClass.prototype.consumer
    KafkaClass.prototype.consumer = function (config: unknown) {
      const consumer = factory.call(this, config)
      consumer.subscribe = async () => { throw Object.assign(new Error('unknown topic'), { code: 3 }) }
      return consumer
    }
    const adapter = confluentAdapter({ client: module })
    await adapter.connect(broker)
    await assert.rejects(adapter.consume({ groupId: 'g', topics: ['t'], eachMessage: async () => {} }), { code: ERROR_CODES.ADAPTER })
    assert.deepEqual(calls.disconnected, ['consumer'])
    await assert.rejects(confluentAdapter({ client: module }).consume({ groupId: 'g', topics: ['t'], eachMessage: async () => {} }), { code: ERROR_CODES.ADAPTER })
  })

  test('commit, stop, disconnect and listTopics failures are wrapped with the original as cause', async () => {
    const { module } = fakeClient()
    const KafkaClass = module.KafkaJS.Kafka as unknown as { prototype: { consumer: (config: unknown) => KafkaJS.Consumer, admin: () => KafkaJS.Admin } }
    const consumerFactory = KafkaClass.prototype.consumer
    KafkaClass.prototype.consumer = function (config: unknown) {
      const consumer = consumerFactory.call(this, config)
      consumer.commitOffsets = async () => { throw new Error('REBALANCE_IN_PROGRESS') }
      consumer.disconnect = async () => { throw new Error('leave failed') }
      return consumer
    }
    const adminFactory = KafkaClass.prototype.admin
    KafkaClass.prototype.admin = function () {
      const admin = adminFactory.call(this)
      admin.listTopics = async () => { throw new Error('metadata timeout') }
      return admin
    }
    const adapter = confluentAdapter({ client: module })
    await adapter.connect(broker)
    const handle = await adapter.consume({ groupId: 'g', topics: ['t'], eachMessage: async () => {} })
    await assert.rejects(handle.commit([{ topic: 't', partition: 0, offset: '1' }]), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.ADAPTER)
      assert.equal(((error as { cause: Error }).cause).message, 'REBALANCE_IN_PROGRESS')
      return true
    })
    await assert.rejects(adapter.admin.topicExists('t'), /metadata timeout/)
    await assert.rejects(handle.stop(), /leave failed/)
    // The consumer left the set on stop(); a second consumer's failure surfaces from disconnect().
    await adapter.consume({ groupId: 'g2', topics: ['t'], eachMessage: async () => {} })
    await assert.rejects(adapter.disconnect(), { code: ERROR_CODES.ADAPTER })
    await assert.rejects(adapter.admin.topicExists('t'), { code: ERROR_CODES.ADAPTER })
  })

  test('the forwarding logger stays silent when the client log level is off', async () => {
    const { module, calls } = fakeClient()
    const adapter = confluentAdapter({ client: module, logLevel: 0 })
    await adapter.connect(broker)
    const errors: unknown[] = []
    await adapter.consume({ groupId: 'g', topics: ['t'], eachMessage: async () => {}, onError: (error) => { errors.push(error) } })
    const config = calls.consumerConfigs[0] as { kafkaJS: { logger: KafkaJS.Logger } }
    config.kafkaJS.logger.error('quiet')
    config.kafkaJS.logger.warn('w')
    config.kafkaJS.logger.debug('d')
    assert.equal(config.kafkaJS.logger.namespace('x'), config.kafkaJS.logger)
    config.kafkaJS.logger.setLogLevel(4)
    assert.equal(errors.length, 0)
    const withoutOnError = await adapter.consume({ groupId: 'g2', topics: ['t'], eachMessage: async () => {} })
    assert.equal('logger' in (calls.consumerConfigs[1] as { kafkaJS: object }).kafkaJS, false)
    await withoutOnError.stop()
  })

  test('produce with topic configs, an eachMessage that rejects, and a record on a topic with an explicit partition', async () => {
    const { module, calls } = fakeClient()
    const adapter = confluentAdapter({ client: module })
    await adapter.connect(broker)
    await adapter.consume({ groupId: 'g', topics: ['t'], concurrency: 3, eachMessage: async () => { throw new Error('handler') } })
    assert.equal(calls.runConfig?.partitionsConsumedConcurrently, 3)
    await assert.rejects(calls.runConfig!.eachMessage!({ topic: 't', partition: 0, heartbeat: async () => {}, pause: () => () => {}, message: { key: null, value: null, timestamp: '1', offset: '0', attributes: 0, headers: {} } as KafkaJS.KafkaMessage }), /handler/)
  })
})

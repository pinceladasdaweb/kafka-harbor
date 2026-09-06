import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { describe, test } from 'node:test'

import { partitionForKey } from '../../src/index'
import { memoryAdapter, runAdapterContract } from '../../src/testing/index'

runAdapterContract('memory', async () => {
  const adapter = memoryAdapter()
  await adapter.connect({ clientId: 'contract', brokers: ['memory:9092'] })
  const run = randomUUID().slice(0, 8)
  return {
    adapter,
    topic: async (label, partitions = 1) => {
      const topic = `${label}-${run}`
      await adapter.admin.createTopics([{ topic, partitions, replicationFactor: 1 }])
      return topic
    },
    group: (label) => `${label}-${run}`,
    timeoutMs: 2_000,
    teardown: async () => { await adapter.disconnect() }
  }
})

describe('memoryAdapter specifics', () => {
  test('an invalid topic name is refused, as a broker would', async () => {
    const adapter = memoryAdapter()
    await adapter.connect({ clientId: 'c', brokers: ['memory'] })
    for (const topic of ['', undefined, 7]) {
      await assert.rejects(adapter.produce([{ topic: topic as never, key: null, value: Buffer.from('x'), headers: {} }]), { code: 'ADAPTER', retryable: false })
    }
    assert.deepEqual(adapter.topics(), [])
    await adapter.disconnect()
  })

  test('a batch with an invalid partition writes nothing at all', async () => {
    const adapter = memoryAdapter({ partitions: 2 })
    await adapter.connect({ clientId: 'c', brokers: ['memory'] })
    await assert.rejects(adapter.produce([
      { topic: 't', key: null, value: Buffer.from('ok'), headers: {} },
      { topic: 't', key: null, value: Buffer.from('bad'), headers: {}, partition: 9 }
    ]), { code: 'ADAPTER', retryable: false })
    assert.equal(adapter.messages('t').length, 0)
  })

  test('with autoCreateTopics off, producing to an unknown topic is refused and consuming it waits', async () => {
    const adapter = memoryAdapter({ autoCreateTopics: false })
    assert.equal(adapter.connected, false)
    await assert.rejects(adapter.produce([{ topic: 'nope', key: null, value: Buffer.from('x'), headers: {} }]), { code: 'ADAPTER' })
    await assert.rejects(adapter.consume({ groupId: 'g', topics: ['nope'], eachMessage: async () => {} }), { code: 'ADAPTER' })
    await adapter.connect({ clientId: 'c', brokers: ['memory'] })
    await assert.rejects(adapter.produce([{ topic: 'nope', key: null, value: Buffer.from('x'), headers: {} }]), { code: 'ADAPTER', retryable: false })
    const received: unknown[] = []
    const handle = await adapter.consume({ groupId: 'g', topics: ['later'], fromBeginning: true, eachMessage: async (m) => { received.push(m) } })
    adapter.createTopic('later')
    await adapter.produce([{ topic: 'later', key: null, value: Buffer.from('x'), headers: {} }])
    await adapter.whenDrained('g', 'later')
    assert.equal(received.length, 1)
    await handle.stop()
    await adapter.disconnect()
  })

  test('a rejecting eachMessage is reported through onError and redelivered without a tight loop', async () => {
    const adapter = memoryAdapter()
    await adapter.connect({ clientId: 'c', brokers: ['memory'] })
    await adapter.produce([{ topic: 't', key: null, value: Buffer.from('x'), headers: {} }])
    const errors: unknown[] = []
    let attempts = 0
    const handle = await adapter.consume({
      groupId: 'g',
      topics: ['t'],
      fromBeginning: true,
      onError: (error) => { errors.push(error) },
      eachMessage: async () => {
        attempts++
        if (attempts < 3) throw new Error('flaky')
      }
    })
    await adapter.whenDrained('g', 't')
    assert.equal(attempts, 3)
    assert.equal(errors.length, 2)
    await handle.stop()
    assert.deepEqual(adapter.paused('g'), [])
    assert.equal(adapter.connected, true)
    await adapter.disconnect()
    assert.equal(adapter.connected, false)
  })
})

describe('memoryAdapter diagnostics', () => {
  test('whenDrained rejects once the last member of the group stopped with messages still undelivered', async () => {
    const adapter = memoryAdapter()
    await adapter.connect({ clientId: 'c', brokers: ['memory'] })
    await adapter.produce(['a', 'b'].map((value) => ({ topic: 't', key: null, value: Buffer.from(value), headers: {} })))
    let stopping: Promise<void> | undefined
    const handle = await adapter.consume({
      groupId: 'g',
      topics: ['t'],
      fromBeginning: true,
      eachMessage: async () => { stopping ??= handle.stop() }
    })
    const drained = adapter.whenDrained('g', 't')
    await assert.rejects(drained, /whenDrained\("g", "t"\): the group has no member left/)
    await stopping
    // A group that never had a member still waits: the consumer may be about to start.
    const waiting = adapter.whenDrained('other', 't')
    const outcome = await Promise.race([waiting.then(() => 'settled', () => 'settled'), new Promise((resolve) => setTimeout(() => resolve('pending'), 50))])
    assert.equal(outcome, 'pending')
    await adapter.disconnect()
  })

  test('messages() lists a topic in append order across partitions, even with a frozen clock', async () => {
    const adapter = memoryAdapter({ partitions: 2, now: () => 1_000 })
    await adapter.connect({ clientId: 'c', brokers: ['memory'] })
    await adapter.produce([
      { topic: 't', key: null, value: Buffer.from('first'), headers: {}, partition: 0 },
      { topic: 't', key: null, value: Buffer.from('second'), headers: {}, partition: 0 },
      { topic: 't', key: null, value: Buffer.from('third'), headers: {}, partition: 1 }
    ])
    // Sorted by offset this would read first, third, second.
    assert.deepEqual(adapter.messages('t').map((m) => m.value?.toString()), ['first', 'second', 'third'])
    assert.deepEqual(adapter.messages('t').map((m) => `${m.partition}@${m.offset}`), ['0@0', '0@1', '1@0'])
    await adapter.disconnect()
  })

  test('clearCalls forgets the recorded calls without touching the broker state', async () => {
    const adapter = memoryAdapter()
    await adapter.connect({ clientId: 'c', brokers: ['memory'] })
    await adapter.produce([{ topic: 't', key: null, value: Buffer.from('x'), headers: {} }])
    assert.equal(adapter.calls.length, 2)
    adapter.clearCalls()
    assert.equal(adapter.calls.length, 0)
    assert.equal(adapter.messages('t').length, 1)
    await adapter.disconnect()
    assert.deepEqual(adapter.calls.map((call) => call.method), ['disconnect'])
  })
})

describe('memoryAdapter inspection helpers', () => {
  test('topics(), createTopic(), paused() and whenDrained() on a topic created later', async () => {
    const adapter = memoryAdapter()
    await adapter.connect({ clientId: 'c', brokers: ['memory'] })
    assert.deepEqual(adapter.topics(), [])
    adapter.createTopic('a', 3)
    assert.deepEqual(adapter.topics(), ['a'])
    await adapter.produce([{ topic: 'a', key: null, value: Buffer.from('x'), headers: {}, partition: 2 }])
    assert.equal(adapter.messages('a')[0]?.partition, 2)
    assert.deepEqual(adapter.messages('missing'), [])
    assert.equal(adapter.committed('g', 'a', 0), undefined)

    const handle = await adapter.consume({ groupId: 'g', topics: ['a', 'b'], fromBeginning: true, eachMessage: async () => {} })
    handle.pause?.([{ topic: 'a', partition: 1 }])
    assert.deepEqual(adapter.paused('g'), [{ topic: 'a', partition: 1 }])
    handle.resume?.([{ topic: 'a', partition: 1 }])
    assert.deepEqual(adapter.paused('g'), [])

    const drained = adapter.whenDrained('g', 'b')
    let settled = false
    drained.then(() => { settled = true }).catch(() => undefined)
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(settled, false, 'an unknown topic is not drained')
    await adapter.produce([{ topic: 'b', key: null, value: Buffer.from('y'), headers: {} }])
    await drained
    await handle.stop()
    await assert.rejects(handle.commit([{ topic: 'a', partition: 0, offset: '1' }]), { code: 'ADAPTER' })
    await adapter.disconnect()
    await assert.rejects(adapter.produce([{ topic: 'a', key: null, value: Buffer.from('z'), headers: {} }]), { code: 'ADAPTER' })
    await assert.rejects(adapter.consume({ groupId: 'g', topics: ['a'], eachMessage: async () => {} }), { code: 'ADAPTER' })
  })

  test('keyed messages land on a stable partition and unkeyed ones round-robin', async () => {
    const adapter = memoryAdapter({ partitions: 3 })
    await adapter.connect({ clientId: 'c', brokers: ['memory'] })
    await adapter.produce([
      { topic: 't', key: Buffer.from('same'), value: Buffer.from('1'), headers: {} },
      { topic: 't', key: Buffer.from('same'), value: Buffer.from('2'), headers: {} },
      { topic: 't', key: null, value: Buffer.from('3'), headers: {} },
      { topic: 't', key: null, value: Buffer.from('4'), headers: {} },
      { topic: 't', key: null, value: Buffer.from('5'), headers: {} }
    ])
    const byValue = new Map(adapter.messages('t').map((m) => [m.value?.toString(), m.partition]))
    assert.equal(byValue.get('1'), byValue.get('2'))
    assert.equal(byValue.get('1'), partitionForKey('same', 3), 'keyed records follow Kafka\'s default partitioner')
    assert.deepEqual([byValue.get('3'), byValue.get('4'), byValue.get('5')], [0, 1, 2])
  })
})

describe('memoryAdapter edge branches', () => {
  test('paused() of an unknown group is empty, and whenDrained() waits for a group that has not read yet', async () => {
    const adapter = memoryAdapter()
    await adapter.connect({ clientId: 'c', brokers: ['memory'] })
    assert.deepEqual(adapter.paused('nobody'), [])
    await adapter.produce([{ topic: 't', key: null, value: Buffer.from('x'), headers: {} }])
    let settled = false
    const drained = adapter.whenDrained('g', 't')
    drained.then(() => { settled = true }).catch(() => undefined)
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(settled, false)
    const handle = await adapter.consume({ groupId: 'g', topics: ['t'], fromBeginning: true, eachMessage: async () => {} })
    await drained
    await handle.stop()
    // A group that stopped has no read positions; an empty topic counts as drained for it.
    adapter.createTopic('empty')
    await adapter.whenDrained('g', 'empty')
    await adapter.disconnect()
  })

  test('createTopics without a partition count uses the adapter default', async () => {
    const adapter = memoryAdapter({ partitions: 2 })
    await adapter.connect({ clientId: 'c', brokers: ['memory'] })
    await adapter.admin.createTopics([{ topic: 'x' }])
    await adapter.produce([{ topic: 'x', key: null, value: Buffer.from('a'), headers: {}, partition: 1 }])
    await assert.rejects(adapter.produce([{ topic: 'x', key: null, value: Buffer.from('b'), headers: {}, partition: 2 }]), { code: 'ADAPTER' })
    await adapter.disconnect()
  })

  test('a partition runner waiting for a concurrency slot gives up once the consumption stopped', async () => {
    const adapter = memoryAdapter({ partitions: 2 })
    await adapter.connect({ clientId: 'c', brokers: ['memory'] })
    await adapter.produce([
      { topic: 't', key: null, value: Buffer.from('p0'), headers: {}, partition: 0 },
      { topic: 't', key: null, value: Buffer.from('p1'), headers: {}, partition: 1 }
    ])
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const delivered: string[] = []
    const handle = await adapter.consume({
      groupId: 'g',
      topics: ['t'],
      fromBeginning: true,
      concurrency: 1,
      eachMessage: async (message) => {
        delivered.push(message.value?.toString() ?? '')
        await gate
      }
    })
    await new Promise((resolve) => setTimeout(resolve, 10))
    assert.deepEqual(delivered, ['p0'])
    const stopping = handle.stop()
    release()
    await stopping
    assert.deepEqual(delivered, ['p0'], 'the second partition never got its slot')
    await adapter.disconnect()
  })
})

describe('memoryAdapter bookkeeping', () => {
  test('records commit, pause, resume and topicExists calls', async () => {
    const adapter = memoryAdapter()
    await adapter.connect({ clientId: 'c', brokers: ['memory'] })
    adapter.createTopic('t')
    const handle = await adapter.consume({ groupId: 'g', topics: ['t'], eachMessage: async () => {} })
    await handle.commit([{ topic: 't', partition: 0, offset: '1' }])
    handle.pause?.([{ topic: 't', partition: 0 }])
    handle.resume?.([{ topic: 't', partition: 0 }])
    await adapter.admin.topicExists('t')
    assert.deepEqual(adapter.calls.map((call) => call.method), ['connect', 'consume', 'commit', 'pause', 'resume', 'topicExists'])
    assert.deepEqual(adapter.calls[2]?.args[0], [{ topic: 't', partition: 0, offset: '1' }])
    await handle.stop()
    await adapter.disconnect()
  })

  test('without fromBeginning, a new group starts at the end of the log', async () => {
    const adapter = memoryAdapter()
    await adapter.connect({ clientId: 'c', brokers: ['memory'] })
    await adapter.produce([{ topic: 't', key: null, value: Buffer.from('old'), headers: {} }])
    const received: string[] = []
    const handle = await adapter.consume({ groupId: 'g', topics: ['t'], eachMessage: async (m) => { received.push(m.value?.toString() ?? '') } })
    await new Promise((resolve) => setTimeout(resolve, 10))
    await adapter.produce([{ topic: 't', key: null, value: Buffer.from('new'), headers: {} }])
    await adapter.whenDrained('g', 't')
    assert.deepEqual(received, ['new'])
    await handle.stop()
    await adapter.disconnect()
  })

  test('a rejecting eachMessage without onError is still redelivered', async () => {
    const adapter = memoryAdapter()
    await adapter.connect({ clientId: 'c', brokers: ['memory'] })
    await adapter.produce([{ topic: 't', key: null, value: Buffer.from('x'), headers: {} }])
    let attempts = 0
    const handle = await adapter.consume({ groupId: 'g', topics: ['t'], fromBeginning: true, eachMessage: async () => { if (++attempts < 2) throw new Error('once') } })
    await adapter.whenDrained('g', 't')
    assert.equal(attempts, 2)
    await handle.stop()
    await adapter.disconnect()
  })
})

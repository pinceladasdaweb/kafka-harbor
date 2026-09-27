import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { ERROR_CODES } from '../../src/index'
import { captureEvents, harness, withoutPause } from '../helpers/harness'
import { settle, until } from '../helpers/manual-clock'

describe('retry delays with a partition that pauses', () => {
  test('a retry message not due yet parks: the partition is paused, the delivery settles, nothing is committed, and it runs when due', async () => {
    const h = harness()
    const processed = captureEvents(h.harbor, 'messageProcessed')
    const retried = captureEvents(h.harbor, 'messageRetried')
    let attempts = 0
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '30s' }] } })
    consumer.subscribe('orders', () => { if (attempts++ === 0) throw new Error('once') })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await until(() => retried.length === 1)
    // The retry copy arrived before it was due: parked, not sleeping in the delivery.
    await until(() => h.adapter.paused('g').length === 1)
    assert.deepEqual(h.adapter.paused('g'), [{ topic: 'orders-retry-1', partition: 0 }])
    assert.deepEqual(h.clock.sleeps, [30_000])
    assert.equal(h.adapter.committed('g', 'orders-retry-1', 0), undefined)
    assert.equal(processed.length, 0)

    h.clock.advance(30_000)
    await until(() => processed.length === 1)
    assert.equal(processed[0]?.topic, 'orders-retry-1')
    assert.equal(h.adapter.committed('g', 'orders-retry-1', 0), '1')
    assert.deepEqual(h.adapter.paused('g'), [], 'resumed once the message was done')
    await h.harbor.shutdown()
  })

  test('messages behind a parked one wait their turn on the paused partition and park in order', async () => {
    const h = harness()
    const processed = captureEvents(h.harbor, 'messageProcessed')
    const retried = captureEvents(h.harbor, 'messageRetried')
    const seen: unknown[] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '10s' }] } })
    consumer.subscribe('orders', (message) => {
      if (message.topic === 'orders') throw new Error('first time')
      seen.push(message.value)
    })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 'a' })
    await until(() => retried.length === 1)
    h.clock.advance(1_000)
    await h.harbor.producer().send('orders', { value: 'b' })
    await until(() => retried.length === 2)
    await until(() => h.adapter.paused('g').length === 1)
    // Only the head is parked; the second stays on the paused partition.
    assert.deepEqual(h.clock.sleeps, [10_000])
    h.clock.advance(9_000)
    await until(() => processed.length === 1)
    assert.deepEqual(seen, ['a'])
    // Resumed, the second is delivered, due one second later than the first, and parks for what is left.
    await until(() => h.clock.sleeps.length === 2)
    assert.equal(h.clock.sleeps[1], 1_000)
    h.clock.advance(1_000)
    await until(() => processed.length === 2)
    assert.deepEqual(seen, ['a', 'b'])
    assert.equal(h.adapter.committed('g', 'orders-retry-1', 0), '2')
    assert.deepEqual(h.adapter.paused('g'), [])
    assert.equal(h.logs.filter((entry) => entry.level === 'warn').length, 0, 'the second parked in its own right, on a partition no longer paused')
    await h.harbor.shutdown()
  })

  test('a stop while parked cancels the wait and leaves the message uncommitted', async () => {
    const h = harness()
    const retried = captureEvents(h.harbor, 'messageRetried')
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '1m' }] } })
    consumer.subscribe('orders', () => { throw new Error('no') })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await until(() => retried.length === 1)
    await until(() => h.clock.waiting === 1)
    await h.harbor.shutdown()
    assert.equal(h.clock.waiting, 0, 'the timer was cancelled')
    assert.equal(h.adapter.committed('g', 'orders-retry-1', 0), undefined)
    assert.equal(h.adapter.messages('orders-retry-1').length, 1, 'still there for the next member')
    assert.deepEqual(h.adapter.paused('g'), [], 'a pause does not outlive the consumption')
  })

  test('a partition taken away while parked drops the message, and the pause with it', async () => {
    const h = harness({}, { partitions: 2 })
    const retried = captureEvents(h.harbor, 'messageRetried')
    const processed = captureEvents(h.harbor, 'messageProcessed')
    let revoke: ((partitions: Array<{ topic: string, partition: number }>) => Promise<void>) | undefined
    const original = h.adapter.consume
    h.adapter.consume = async (options) => {
      if (options.topics.includes('orders-retry-1')) revoke = options.onPartitionsRevoked as typeof revoke
      return await original(options)
    }
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '1m' }] } })
    consumer.subscribe('orders', (message) => { if (message.topic === 'orders') throw new Error('no') })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1, partition: 0 })
    await until(() => retried.length === 1)
    await until(() => h.adapter.paused('g').length === 1)
    const partition = h.adapter.paused('g')[0]?.partition as number
    await (revoke as NonNullable<typeof revoke>)([{ topic: 'orders-retry-1', partition }])
    assert.equal(h.clock.waiting, 0, 'the timer was cancelled')
    assert.deepEqual(h.adapter.paused('g'), [], 'resumed before the partition was released')
    h.clock.advance(60_000)
    await settle()
    assert.equal(processed.length, 0, 'the next owner processes it')
    assert.equal(h.adapter.committed('g', 'orders-retry-1', partition), undefined)
    await h.harbor.shutdown()
  })

  test('without pause/resume the delivery waits inside the adapter, bounded by maxProcessingTime as before', async () => {
    const h = harness()
    withoutPause(h)
    const processed = captureEvents(h.harbor, 'messageProcessed')
    const retried = captureEvents(h.harbor, 'messageRetried')
    let attempts = 0
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '30s' }] } })
    consumer.subscribe('orders', () => { if (attempts++ === 0) throw new Error('once') })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await until(() => retried.length === 1)
    await until(() => h.clock.waiting === 1)
    assert.deepEqual(h.adapter.paused('g'), [])
    assert.equal(h.adapter.calls.filter((call) => call.method === 'pause').length, 0)
    h.clock.advance(30_000)
    await until(() => processed.length === 1)
    assert.equal(h.adapter.committed('g', 'orders-retry-1', 0), '1')
    assert.equal(h.logs.filter((entry) => entry.level === 'warn').length, 0, 'nothing was attempted on a handle without pause')
    await h.harbor.shutdown()
  })

  test('a delay longer than maxProcessingTime is fine with an adapter that pauses, and refused at start() without one', async () => {
    const h = harness()
    const processed = captureEvents(h.harbor, 'messageProcessed')
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, maxProcessingTime: '1m', retry: { levels: [{ delay: '1h' }] } })
    let attempts = 0
    consumer.subscribe('orders', () => { if (attempts++ === 0) throw new Error('once') })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await until(() => h.clock.sleeps.includes(3_600_000))
    h.clock.advance(3_600_000)
    await until(() => processed.length === 1)
    await h.harbor.shutdown()

    const plain = harness()
    withoutPause(plain)
    const stuck = plain.harbor.consumer({ groupId: 'g', autoCreateTopics: true, maxProcessingTime: '1m', retry: { levels: [{ delay: '1h' }] } })
    stuck.subscribe('orders', () => {})
    await assert.rejects(stuck.start(), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
      assert.match((error as Error).message, /3600000ms exceeds maxProcessingTime \(60000ms\) and adapter "memory" has no pause\/resume/)
      return true
    })
    assert.equal(stuck.status, 'stopped')
    assert.equal(plain.adapter.calls.filter((call) => call.method === 'stop').length, plain.adapter.calls.filter((call) => call.method === 'consume').length, 'every consumption opened was closed')
    // A delay equal to the bound fits in the delivery; a handle missing only resume cannot park either.
    const equal = plain.harbor.consumer({ groupId: 'e', autoCreateTopics: true, maxProcessingTime: '1m', retry: { levels: [{ delay: '1m' }] } })
    equal.subscribe('orders', () => {})
    await equal.start()
    await plain.harbor.shutdown()

    const halfway = harness()
    const original = halfway.adapter.consume
    halfway.adapter.consume = async (options) => {
      const { resume, ...handle } = await original(options)
      return handle
    }
    const lopsided = halfway.harbor.consumer({ groupId: 'g', autoCreateTopics: true, maxProcessingTime: '1m', retry: { levels: [{ delay: '1h' }] } })
    lopsided.subscribe('orders', () => {})
    await assert.rejects(lopsided.start(), { code: ERROR_CODES.CONFIG_INVALID })
    await halfway.harbor.shutdown()
  })

  test('a refused start with deliveries already in flight settles instead of hanging, and leaves them uncommitted', async () => {
    const plain = harness()
    withoutPause(plain)
    plain.adapter.createTopic('orders')
    await plain.harbor.connect()
    await plain.adapter.produce([{ topic: 'orders', key: null, value: Buffer.from('1'), headers: {} }])
    const stuck = plain.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, maxProcessingTime: '1m', retry: { levels: [{ delay: '1h' }] } })
    const seen: unknown[] = []
    stuck.subscribe('orders', (message) => { seen.push(message.value) })
    await assert.rejects(stuck.start(), { code: ERROR_CODES.CONFIG_INVALID })
    assert.deepEqual(seen, [])
    assert.equal(plain.adapter.committed('g', 'orders', 0), undefined)
    assert.equal(plain.clock.waiting, 0)
    await plain.harbor.shutdown()
  })

  test('a pause the client refuses, or a delivery that comes through a pause, makes the delivery wait itself', async () => {
    const refusing = harness()
    const original = refusing.adapter.consume
    refusing.adapter.consume = async (options) => {
      const handle = await original(options)
      return { ...handle, pause: () => { throw new Error('not assigned') } }
    }
    const processed = captureEvents(refusing.harbor, 'messageProcessed')
    let attempts = 0
    const consumer = refusing.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '2s' }] } })
    consumer.subscribe('orders', () => { if (attempts++ === 0) throw new Error('once') })
    await consumer.start()
    await refusing.harbor.producer().send('orders', { value: 1 })
    await until(() => refusing.clock.waiting === 1)
    assert.ok(refusing.logs.some((entry) => entry.level === 'warn' && entry.message.includes('pausing orders-retry-1[0] failed, waiting in the delivery instead: not assigned')))
    refusing.clock.advance(2_000)
    await until(() => processed.length === 1)
    await refusing.harbor.shutdown()

    // An adapter whose pause does nothing keeps delivering: the second message waits in its delivery, after the first.
    const leaky = harness()
    const leakyOriginal = leaky.adapter.consume
    leaky.adapter.consume = async (options) => ({ ...(await leakyOriginal(options)), pause: () => {} })
    const seen: unknown[] = []
    const leakyConsumer = leaky.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '2s' }] } })
    leakyConsumer.subscribe('orders', (message) => {
      if (message.topic === 'orders') throw new Error('first time')
      seen.push(message.value)
    })
    await leakyConsumer.start()
    await leaky.harbor.producer().sendBatch('orders', [{ value: 'a' }, { value: 'b' }])
    await until(() => leaky.logs.some((entry) => entry.level === 'warn' && entry.message.includes('delivered orders-retry-1[0]@1 on a paused partition')))
    assert.equal(leaky.clock.waiting, 1, 'the second waits behind the parked one, not on a timer of its own')
    leaky.clock.advance(2_000)
    await until(() => seen.length === 2)
    assert.deepEqual(seen, ['a', 'b'])
    assert.equal(leaky.adapter.committed('g', 'orders-retry-1', 0), '2')
    await leaky.harbor.shutdown()
  })

  test('parked messages coming back take at most concurrency slots at a time', async () => {
    const h = harness({}, { partitions: 3 })
    const processed = captureEvents(h.harbor, 'messageProcessed')
    let inFlight = 0
    let maxInFlight = 0
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, concurrency: 1, retry: { levels: [{ delay: '5s' }] }, topicDefaults: { partitions: 3 } })
    consumer.subscribe('orders', async (message) => {
      if (message.topic === 'orders') throw new Error('first time')
      inFlight++
      maxInFlight = Math.max(maxInFlight, inFlight)
      await new Promise((resolve) => setTimeout(resolve, 10))
      inFlight--
    })
    await consumer.start()
    await h.harbor.producer().sendBatch('orders', [0, 1, 2].map((partition) => ({ value: partition, partition })))
    await until(() => h.adapter.paused('g').length === 3)
    h.clock.advance(5_000)
    await until(() => processed.length === 3)
    assert.equal(maxInFlight, 1)
    assert.deepEqual(h.adapter.paused('g'), [])
    await h.harbor.shutdown()
  })

  test('a resume that fails is logged, and the message still went through', async () => {
    const h = harness()
    const processed = captureEvents(h.harbor, 'messageProcessed')
    const original = h.adapter.consume
    h.adapter.consume = async (options) => {
      const handle = await original(options)
      return { ...handle, resume: () => { throw new Error('client gone') } }
    }
    let attempts = 0
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '1s' }] } })
    consumer.subscribe('orders', () => { if (attempts++ === 0) throw new Error('once') })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await until(() => h.clock.waiting === 1)
    h.clock.advance(1_000)
    await until(() => processed.length === 1)
    assert.ok(h.logs.some((entry) => entry.level === 'warn' && entry.message.includes('resuming orders-retry-1[0] failed: client gone')))
    await h.harbor.shutdown()
  })

  test('a parked message of a batch subscription joins its batch when due', async () => {
    const h = harness()
    const batches = captureEvents(h.harbor, 'batchProcessed')
    const seen: unknown[][] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '5s' }] } })
    consumer.subscribeBatch('orders', (messages, ctx) => {
      if (ctx.topic === 'orders') throw new Error('first time')
      seen.push(messages.map((message) => message.value))
    }, { size: 2, maxWait: '1s' })
    await consumer.start()
    await h.harbor.producer().sendBatch('orders', [{ value: 1 }, { value: 2 }])
    await until(() => h.adapter.paused('g').length === 1)
    h.clock.advance(5_000)
    await until(() => batches.filter((event) => event.topic === 'orders-retry-1' && event.outcome === 'processed').length === 1)
    assert.deepEqual(seen, [[1, 2]])
    assert.equal(h.adapter.committed('g', 'orders-retry-1', 0), '2')
    await h.harbor.shutdown()
  })
})

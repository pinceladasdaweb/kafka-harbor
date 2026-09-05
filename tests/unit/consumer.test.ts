import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { ERROR_CODES, type ConsumerEvents, type HarborEvents, type Message } from '../../src/index'
import { harness, json, text, type Harness } from '../helpers/harness'
import { settle, until } from '../helpers/manual-clock'

type Events = { [K in keyof HarborEvents]: Array<HarborEvents[K]> }

const capture = (h: Harness): Events => {
  const events: Events = {
    connected: [],
    disconnected: [],
    error: [],
    messageProcessed: [],
    messageRetried: [],
    messageDeadLettered: [],
    messageFailed: [],
    consumerStopped: []
  }
  for (const name of Object.keys(events) as Array<keyof HarborEvents>) {
    h.harbor.on(name, (payload) => { (events[name] as unknown[]).push(payload) })
  }
  return events
}

const produced = async (h: Harness, topic: string, values: unknown[], key?: string): Promise<void> => {
  await h.harbor.producer().sendBatch(topic, values.map((value) => ({ value, ...(key !== undefined && { key }) })))
}

describe('Consumer: success path', () => {
  test('delivers a deserialized Message with context, then commits the next offset', async () => {
    const h = harness()
    const events = capture(h)
    const seen: Array<{ message: Message<{ id: number }>, attempt: number, correlationId: string | undefined }> = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe<{ id: number }>('orders', (message, ctx) => {
      seen.push({ message, attempt: ctx.attempt, correlationId: ctx.correlationId })
    })
    await produced(h, 'orders', [{ id: 1 }, { id: 2 }], 'k')
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')

    assert.equal(seen.length, 2)
    const first = seen[0]!
    assert.equal(first.message.topic, 'orders')
    assert.equal(first.message.partition, 0)
    assert.equal(first.message.offset, '0')
    assert.equal(first.message.key, 'k')
    assert.deepEqual(first.message.value, { id: 1 })
    assert.equal(first.message.headers['x-correlation-id'], 'corr-fixed')
    assert.equal(first.message.timestamp.getTime(), h.clock.now())
    assert.equal(first.message.retry, undefined)
    assert.equal(first.attempt, 1)
    assert.equal(first.correlationId, 'corr-fixed')
    assert.equal(h.adapter.committed('g', 'orders', 0), '2')
    assert.equal(events.messageProcessed.length, 2)
    assert.equal(events.messageProcessed[0]?.offset, '0')
    assert.equal(events.error.length, 0)
    await h.harbor.shutdown()
  })

  test('a null value reaches the handler as null without deserializing', async () => {
    const h = harness()
    const values: unknown[] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', (message) => { values.push(message.value) })
    await h.harbor.connect()
    await h.adapter.produce([{ topic: 'orders', key: null, value: null as unknown as Buffer, headers: {} }])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    assert.deepEqual(values, [null])
    await h.harbor.shutdown()
  })

  test('processes partitions concurrently while preserving order within each', async () => {
    const h = harness({}, { partitions: 2 })
    const order: string[] = []
    let inFlight = 0
    let maxInFlight = 0
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, concurrency: 2 })
    consumer.subscribe<string>('orders', async (message) => {
      inFlight++
      maxInFlight = Math.max(maxInFlight, inFlight)
      await new Promise((resolve) => setTimeout(resolve, 5))
      order.push(`${message.partition}:${message.value}`)
      inFlight--
    })
    const producer = h.harbor.producer<string>()
    await producer.sendBatch('orders', ['a', 'b', 'c'].map((value) => ({ value, partition: 0 })))
    await producer.sendBatch('orders', ['x', 'y', 'z'].map((value) => ({ value, partition: 1 })))
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(maxInFlight, 2)
    assert.deepEqual(order.filter((entry) => entry.startsWith('0:')), ['0:a', '0:b', '0:c'])
    assert.deepEqual(order.filter((entry) => entry.startsWith('1:')), ['1:x', '1:y', '1:z'])
    await h.harbor.shutdown()
  })
})

describe('Consumer: retry topics and DLQ', () => {
  const failing = (h: Harness, options: { levels: number[], retryIf?: (error: unknown) => boolean, dlq?: boolean, failUntil?: number }) => {
    const events = capture(h)
    const attempts: Array<{ topic: string, attempt: number, retry: Message['retry'] }> = []
    const consumer = h.harbor.consumer({
      groupId: 'g',
      fromBeginning: true,
      autoCreateTopics: true,
      retry: { levels: options.levels.map((delay) => ({ delay })), ...(options.retryIf !== undefined && { retryIf: options.retryIf }) },
      ...(options.dlq === false && { dlq: { enabled: false } })
    })
    consumer.subscribe('orders', (message, ctx) => {
      attempts.push({ topic: message.topic, attempt: ctx.attempt, retry: message.retry })
      if (attempts.length <= (options.failUntil ?? Number.POSITIVE_INFINITY)) throw new Error(`fail #${attempts.length}`)
    })
    return { events, attempts, consumer }
  }

  test('a failure forwards the original bytes to retry-1 with tracking headers and commits the source', async () => {
    const h = harness()
    const { events, attempts, consumer } = failing(h, { levels: [5_000] })
    await produced(h, 'orders', [{ id: 7 }], 'k7')
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')

    assert.equal(attempts.length, 1)
    assert.equal(h.adapter.committed('g', 'orders', 0), '1')
    const [retried] = h.adapter.messages('orders-retry-1')
    assert.ok(retried)
    assert.equal(text(retried.key), 'k7')
    assert.deepEqual(json(retried.value), { id: 7 })
    assert.equal(retried.headers['x-retry-count'], '1')
    assert.equal(retried.headers['x-original-topic'], 'orders')
    assert.equal(retried.headers['x-first-failure-at'], new Date(h.clock.now()).toISOString())
    assert.equal(retried.headers['x-last-error'], 'fail #1')
    assert.equal(retried.headers['x-correlation-id'], 'corr-fixed')
    assert.equal(retried.headers['x-producer'], 'test-app')
    assert.equal(events.messageRetried.length, 1)
    assert.equal(events.messageRetried[0]?.retryTopic, 'orders-retry-1')
    assert.equal(events.messageRetried[0]?.level, 1)
    assert.equal(events.messageFailed[0]?.outcome, 'retry')
    // The retry consumer is now waiting for the level's delay.
    await until(() => h.clock.waiting === 1)
    assert.deepEqual(h.clock.sleeps, [5_000])
    await h.harbor.shutdown()
  })

  test('walks the whole ladder, waiting each delay, and dead-letters after the last level', async () => {
    const h = harness()
    const { events, attempts, consumer } = failing(h, { levels: [5_000, 60_000] })
    await produced(h, 'orders', [{ id: 1 }])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(attempts.length, 1)

    // Level 1: nothing happens until 5s elapsed on the clock.
    await until(() => h.clock.waiting === 1)
    assert.equal(attempts.length, 1)
    h.clock.advance(4_999)
    await settle()
    assert.equal(attempts.length, 1)
    h.clock.advance(1)
    await h.adapter.whenDrained('g', 'orders-retry-1')
    assert.equal(attempts.length, 2)
    assert.equal(attempts[1]?.topic, 'orders-retry-1')
    assert.equal(attempts[1]?.attempt, 2)
    assert.equal(attempts[1]?.retry?.count, 1)
    assert.equal(attempts[1]?.retry?.originalTopic, 'orders')
    assert.equal(attempts[1]?.retry?.lastError, 'fail #1')
    assert.equal(h.adapter.committed('g', 'orders-retry-1', 0), '1')

    // Level 2: 60s.
    await until(() => h.clock.waiting === 1)
    h.clock.advance(60_000)
    await h.adapter.whenDrained('g', 'orders-retry-2')
    assert.equal(attempts.length, 3)
    assert.equal(attempts[2]?.retry?.count, 2)
    assert.equal(h.adapter.committed('g', 'orders-retry-2', 0), '1')

    // No level left: DLQ, with the original payload and the full trail.
    const [dead] = h.adapter.messages('orders-dlq')
    assert.ok(dead)
    assert.deepEqual(json(dead.value), { id: 1 })
    assert.equal(dead.headers['x-retry-count'], '3')
    assert.equal(dead.headers['x-original-topic'], 'orders')
    assert.equal(dead.headers['x-last-error'], 'fail #3')
    assert.equal(dead.headers['x-dead-lettered-at'], new Date(h.clock.now()).toISOString())
    assert.equal(events.messageDeadLettered.length, 1)
    assert.equal(events.messageDeadLettered[0]?.attempts, 3)
    assert.equal(events.messageDeadLettered[0]?.dlqTopic, 'orders-dlq')
    assert.equal(events.messageRetried.length, 2)
    assert.deepEqual(events.messageFailed.map((e) => e.outcome), ['retry', 'retry', 'dead-letter'])
    assert.equal(events.error.length, 0)
    assert.equal(consumer.status, 'running')
    await h.harbor.shutdown()
  })

  test('a message that succeeds on a retry topic is committed and goes no further', async () => {
    const h = harness()
    const { events, attempts, consumer } = failing(h, { levels: [1_000], failUntil: 1 })
    await produced(h, 'orders', [{ id: 1 }])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    await until(() => h.clock.waiting === 1)
    h.clock.advance(1_000)
    await h.adapter.whenDrained('g', 'orders-retry-1')
    assert.equal(attempts.length, 2)
    assert.equal(h.adapter.messages('orders-dlq').length, 0)
    assert.equal(events.messageProcessed.length, 1)
    assert.equal(events.messageProcessed[0]?.topic, 'orders-retry-1')
    await h.harbor.shutdown()
  })

  test('retryIf false sends the message straight to the DLQ', async () => {
    const h = harness()
    const { events, consumer } = failing(h, { levels: [1_000], retryIf: () => false })
    await produced(h, 'orders', [{ id: 1 }])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(h.adapter.messages('orders-retry-1').length, 0)
    assert.equal(h.adapter.messages('orders-dlq').length, 1)
    assert.equal(h.adapter.messages('orders-dlq')[0]?.headers['x-retry-count'], '1')
    assert.equal(events.messageDeadLettered[0]?.attempts, 1)
    assert.equal(h.adapter.committed('g', 'orders', 0), '1')
    await h.harbor.shutdown()
  })

  test('an error carrying retryable: false is not retried by default', async () => {
    const h = harness()
    const events = capture(h)
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 1_000 }] } })
    consumer.subscribe('orders', () => {
      throw Object.assign(new Error('business rule'), { retryable: false })
    })
    await produced(h, 'orders', [1])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(h.adapter.messages('orders-retry-1').length, 0)
    assert.equal(events.messageDeadLettered.length, 1)
    await h.harbor.shutdown()
  })

  test('a payload that does not deserialize is dead-lettered, never retried', async () => {
    const h = harness()
    const events = capture(h)
    let calls = 0
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 1_000 }] } })
    consumer.subscribe('orders', () => { calls++ })
    await h.harbor.connect()
    await h.adapter.produce([{ topic: 'orders', key: null, value: Buffer.from('{broken'), headers: {} }])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(calls, 0)
    assert.equal(h.adapter.messages('orders-retry-1').length, 0)
    const [dead] = h.adapter.messages('orders-dlq')
    assert.equal(text(dead?.value ?? null), '{broken')
    assert.match(String(dead?.headers['x-last-error']), /not valid JSON/)
    assert.equal(events.messageDeadLettered.length, 1)
    assert.ok(events.messageFailed[0]?.error !== undefined)
    await h.harbor.shutdown()
  })

  test('with no level and no DLQ, a failure stops the consumer without committing', async () => {
    const h = harness()
    const { events, consumer } = failing(h, { levels: [], dlq: false })
    await produced(h, 'orders', [1, 2])
    await consumer.start()
    await until(() => consumer.status === 'stopped')
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    assert.equal(events.messageFailed[0]?.outcome, 'crash')
    assert.equal(events.error.length, 1)
    assert.equal(events.error[0]?.scope, 'consumer')
    assert.equal(events.consumerStopped[0]?.reason, 'crash')
    assert.ok(h.logs.some((entry) => entry.level === 'error' && entry.message.includes('orders[0]@0')))
    await h.harbor.shutdown()
  })

  test('a retry produce that is not acknowledged leaves the offset uncommitted and stops the consumer', async () => {
    const h = harness()
    const { events, consumer } = failing(h, { levels: [1_000] })
    await produced(h, 'orders', [1])
    h.adapter.failNextProduce(Object.assign(new Error('broker refused'), { retryable: false }))
    await consumer.start()
    await until(() => consumer.status === 'stopped')
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    assert.equal(h.adapter.messages('orders-retry-1').length, 0)
    assert.equal(events.messageRetried.length, 0)
    assert.equal(events.error.length, 1)
    assert.equal(events.consumerStopped[0]?.reason, 'crash')
    await h.harbor.shutdown()
  })

  test('the messageDeadLettered event fires after the DLQ produce, so a failed produce never reports a rescue', async () => {
    const h = harness()
    const { events, consumer } = failing(h, { levels: [] })
    await produced(h, 'orders', [1])
    h.adapter.failNextProduce(Object.assign(new Error('nope'), { retryable: false }))
    await consumer.start()
    await until(() => consumer.status === 'stopped')
    assert.equal(events.messageDeadLettered.length, 0)
    assert.equal(h.adapter.messages('orders-dlq').length, 0)
    await h.harbor.shutdown()
  })

  test('custom naming is used for the retry ladder and the DLQ', async () => {
    const h = harness()
    const events = capture(h)
    const consumer = h.harbor.consumer({
      groupId: 'g',
      fromBeginning: true,
      autoCreateTopics: true,
      retry: { levels: [{ delay: 0 }], topicNaming: (topic, level) => `${topic}.retry.${level}` },
      dlq: { topicNaming: (topic) => `${topic}.dead` }
    })
    consumer.subscribe('orders', () => { throw new Error('x') })
    await produced(h, 'orders', [1])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    await h.adapter.whenDrained('g', 'orders.retry.1')
    assert.equal(h.adapter.messages('orders.retry.1').length, 1)
    assert.equal(h.adapter.messages('orders.dead').length, 1)
    assert.equal(events.messageDeadLettered[0]?.dlqTopic, 'orders.dead')
    await h.harbor.shutdown()
  })

  test('a corrupt retry-count header is ignored: the level of the topic drives the ladder', async () => {
    const h = harness()
    const { attempts, consumer } = failing(h, { levels: [0, 0] })
    await h.harbor.connect()
    // Someone produced straight into retry-1 with garbage tracking headers.
    await h.adapter.produce([{ topic: 'orders-retry-1', key: null, value: Buffer.from('1'), headers: { 'x-retry-count': 'lots', 'x-original-topic': 'orders' } }])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders-retry-1')
    await h.adapter.whenDrained('g', 'orders-retry-2')
    assert.equal(attempts[0]?.retry, undefined)
    assert.equal(attempts[0]?.attempt, 2)
    assert.equal(h.adapter.messages('orders-retry-2')[0]?.headers['x-retry-count'], '1')
    assert.equal(h.adapter.messages('orders-dlq').length, 1)
    await h.harbor.shutdown()
  })
})

describe('Consumer: abort and shutdown', () => {
  test('harbor.abort() stops the consumer without committing', async () => {
    const h = harness()
    const events = capture(h)
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    consumer.subscribe('orders', () => {
      throw h.harbor.abort(new Error('database schema missing'))
    })
    await produced(h, 'orders', [1])
    await consumer.start()
    await until(() => consumer.status === 'stopped')
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    assert.equal(h.adapter.messages('orders-retry-1').length, 0)
    assert.equal(h.adapter.messages('orders-dlq').length, 0)
    assert.equal(events.messageFailed[0]?.outcome, 'abort')
    assert.equal((events.error[0]?.error as { code: string }).code, ERROR_CODES.ABORT_PROCESSING)
    assert.equal(events.consumerStopped[0]?.reason, 'abort')
    await h.harbor.shutdown()
  })

  test('shutdown waits for the in-flight handler, lets it commit, then leaves and disconnects', async () => {
    const h = harness()
    const events = capture(h)
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', async () => { await gate })
    await produced(h, 'orders', [1, 2])
    await consumer.start()
    await until(() => h.adapter.calls.some((call) => call.method === 'consume'))
    await settle()

    const closing = h.harbor.shutdown(30_000)
    await settle()
    assert.equal(consumer.status, 'stopping')
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    release()
    await closing
    // The first message committed on its way out; the second was never
    // processed and stays for the next member.
    assert.equal(h.adapter.committed('g', 'orders', 0), '1')
    assert.equal(consumer.status, 'stopped')
    assert.equal(h.harbor.status, 'closed')
    assert.equal(h.adapter.connected, false)
    assert.deepEqual(h.adapter.calls.slice(-2).map((call) => call.method), ['stop', 'disconnect'])
    assert.equal(events.consumerStopped[0]?.reason, 'shutdown')
    assert.equal(events.disconnected.length, 1)
  })

  test('a handler that outlives the timeout is abandoned: its signal aborts, nothing is committed, the error is explicit', async () => {
    const h = harness()
    let signal: AbortSignal | undefined
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', async (_message, ctx) => {
      signal = ctx.signal
      await gate
      throw new Error('gave up because of the abort')
    })
    await produced(h, 'orders', [1])
    await consumer.start()
    await until(() => signal !== undefined)

    const closing = h.harbor.shutdown(1_000)
    await until(() => h.clock.waiting === 1)
    h.clock.advance(1_000)
    await assert.rejects(closing, (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.SHUTDOWN_TIMEOUT)
      assert.equal((error as { inFlight: number }).inFlight, 1)
      return true
    })
    assert.equal(signal?.aborted, true)
    assert.equal(h.harbor.status, 'closed')
    assert.equal(h.adapter.connected, false)
    release()
    await settle()
    // The late failure is not a verdict on the message: no retry, no DLQ.
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    assert.equal(h.adapter.messages('orders-dlq').length, 0)
    assert.ok(h.logs.some((entry) => entry.level === 'warn' && entry.message.includes('1 handler(s)')))
  })

  test('a retry wait is cut short by shutdown and the message stays uncommitted', async () => {
    const h = harness()
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 60_000 }] } })
    consumer.subscribe('orders', () => { throw new Error('x') })
    await produced(h, 'orders', [1])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    await until(() => h.clock.waiting === 1)
    await h.harbor.shutdown(0)
    assert.equal(h.adapter.committed('g', 'orders-retry-1', 0), undefined)
    assert.equal(h.adapter.messages('orders-dlq').length, 0)
  })
})

describe('Consumer: configuration and topics', () => {
  test('validates groupId, concurrency, handler, duplicate topics and start/subscribe order', async () => {
    const h = harness()
    assert.throws(() => h.harbor.consumer({ groupId: '' }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => h.harbor.consumer({ groupId: 'g', concurrency: 0 }), { code: ERROR_CODES.CONFIG_INVALID })
    const consumer = h.harbor.consumer({ groupId: 'g', autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    assert.throws(() => consumer.subscribe('', () => {}), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => consumer.subscribe('orders', 'nope' as unknown as () => void), { code: ERROR_CODES.CONFIG_INVALID })
    await assert.rejects(consumer.start(), { code: ERROR_CODES.CONFIG_INVALID })
    consumer.subscribe('orders', () => {})
    assert.throws(() => consumer.subscribe('orders', () => {}), { code: ERROR_CODES.CONFIG_INVALID })
    // A topic claimed by another subscription's ladder is refused.
    assert.throws(() => consumer.subscribe('orders-retry-1', () => {}), { code: ERROR_CODES.CONFIG_INVALID })
    await consumer.start()
    assert.throws(() => consumer.subscribe('other', () => {}), { code: ERROR_CODES.CONFIG_INVALID })
    await assert.rejects(consumer.start(), { code: ERROR_CODES.CONFIG_INVALID })
    await h.harbor.shutdown()
  })

  test('a retry delay above maxProcessingTime is rejected at construction', () => {
    const h = harness()
    assert.throws(
      () => h.harbor.consumer({ groupId: 'g', maxProcessingTime: '1m', retry: { levels: [{ delay: '2m' }] } }),
      (error: unknown) => {
        assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
        assert.match((error as Error).message, /retry\.levels\[0\]\.delay/)
        return true
      }
    )
  })

  test('autoCreateTopics creates the ladder and the DLQ through the Admin API with the topic defaults', async () => {
    const h = harness()
    const consumer = h.harbor.consumer({
      groupId: 'g',
      autoCreateTopics: true,
      retry: { levels: [{ delay: 0 }, { delay: 0 }] },
      topicDefaults: { partitions: 3, replicationFactor: 2 }
    })
    consumer.subscribe('orders', () => {})
    await consumer.start()
    const call = h.adapter.calls.find((entry) => entry.method === 'createTopics')
    assert.deepEqual(call?.args[0], [
      { topic: 'orders-retry-1', partitions: 3, replicationFactor: 2 },
      { topic: 'orders-retry-2', partitions: 3, replicationFactor: 2 },
      { topic: 'orders-dlq', partitions: 3, replicationFactor: 2 }
    ])
    assert.deepEqual(h.adapter.topics().sort(), ['orders-dlq', 'orders-retry-1', 'orders-retry-2'])
    await h.harbor.shutdown()
  })

  test('without autoCreateTopics, a missing retry or DLQ topic fails start() naming the topic', async () => {
    const h = harness()
    const consumer = h.harbor.consumer({ groupId: 'g', retry: { levels: [{ delay: 0 }] } })
    consumer.subscribe('orders', () => {})
    h.adapter.createTopic('orders-retry-1')
    await assert.rejects(consumer.start(), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.TOPIC_MISSING)
      assert.equal((error as { topic: string }).topic, 'orders-dlq')
      return true
    })
    assert.equal(consumer.status, 'stopped')
    assert.equal(h.adapter.calls.filter((call) => call.method === 'consume').length, 0)
    await h.harbor.shutdown()
  })

  test('the consumer subscribes to the original topic and its whole ladder, never the DLQ', async () => {
    const h = harness()
    const consumer = h.harbor.consumer({ groupId: 'g', autoCreateTopics: true, retry: { levels: [{ delay: 0 }, { delay: 0 }] } })
    consumer.subscribe('orders', () => {}).subscribe('payments', () => {})
    await consumer.start()
    const call = h.adapter.calls.find((entry) => entry.method === 'consume')
    const options = call?.args[0] as { topics: string[], groupId: string, concurrency: number, fromBeginning: boolean }
    assert.deepEqual(options.topics, ['orders', 'orders-retry-1', 'orders-retry-2', 'payments', 'payments-retry-1', 'payments-retry-2'])
    assert.equal(options.groupId, 'g')
    assert.equal(options.concurrency, 1)
    assert.equal(options.fromBeginning, false)
    await h.harbor.shutdown()
  })

  test('a per-topic serializer overrides the consumer and harbor ones', async () => {
    const h = harness()
    const values: unknown[] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe<string>('orders', (message) => { values.push(message.value) }, {
      serializer: { serialize: (value) => Buffer.from(value), deserialize: (bytes) => `raw:${bytes.toString()}` }
    })
    await produced(h, 'orders', ['x'])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    assert.deepEqual(values, ['raw:"x"'])
    await h.harbor.shutdown()
  })

  test('start() after shutdown is refused', async () => {
    const h = harness()
    const consumer = h.harbor.consumer({ groupId: 'g' })
    consumer.subscribe('orders', () => {})
    await h.harbor.shutdown()
    await assert.rejects(consumer.start(), { code: ERROR_CODES.CLOSED })
  })

  test('event listeners that throw are reported to the logger and do not break processing', async () => {
    const h = harness()
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    h.harbor.on('messageProcessed', () => { throw new Error('listener bug') })
    await produced(h, 'orders', [1])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(h.adapter.committed('g', 'orders', 0), '1')
    assert.ok(h.logs.some((entry) => entry.message.includes('listener threw')))
    await h.harbor.shutdown()
  })

  test('the events carry the consumer-level payloads as typed', async () => {
    const h = harness()
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    const processed: Array<ConsumerEvents['messageProcessed']> = []
    h.harbor.on('messageProcessed', (payload) => { processed.push(payload) })
    await produced(h, 'orders', [1])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(processed[0]?.groupId, 'g')
    assert.equal(processed[0]?.durationMs, 0)
    await h.harbor.shutdown()
  })
})

describe('Consumer: review regressions', () => {
  test('a stop that begins while start() is joining the group closes the membership and start() rejects as closed', async () => {
    // Regression: stop() during 'starting' used to return early with no
    // handle, and start() then flipped the consumer back to 'running' on a
    // harbor that was already closed: a ghost member committing offsets.
    const h = harness()
    const events = capture(h)
    let releaseConsume!: () => void
    const gate = new Promise<void>((resolve) => { releaseConsume = resolve })
    const original = h.adapter.consume
    h.adapter.consume = async (options) => {
      await gate
      return await original(options)
    }
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    await produced(h, 'orders', [1])
    const starting = consumer.start()
    await settle()
    assert.equal(consumer.status, 'starting')
    const closing = h.harbor.shutdown()
    await settle()
    releaseConsume()
    await assert.rejects(starting, { code: ERROR_CODES.CLOSED })
    await closing
    assert.equal(consumer.status, 'stopped')
    assert.equal(h.harbor.status, 'closed')
    assert.deepEqual(h.adapter.calls.filter((call) => call.method === 'stop').length, 1)
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    assert.equal(events.consumerStopped.length, 1)
  })

  test('a tombstone that fails keeps its null value through the retry topic and the DLQ', async () => {
    // Regression: forward() replaced a null value with an empty buffer, which
    // the JSON serializer then refused, skipping the ladder.
    const h = harness()
    const values: unknown[] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    consumer.subscribe('orders', (message) => {
      values.push(message.value)
      throw new Error('cannot handle a tombstone')
    })
    await h.harbor.connect()
    await h.adapter.produce([{ topic: 'orders', key: Buffer.from('k'), value: null, headers: {} }])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    await h.adapter.whenDrained('g', 'orders-retry-1')
    assert.deepEqual(values, [null, null])
    assert.equal(h.adapter.messages('orders-retry-1')[0]?.value, null)
    assert.equal(h.adapter.messages('orders-dlq')[0]?.value, null)
    assert.equal(h.adapter.messages('orders-dlq')[0]?.headers['x-retry-count'], '2')
    await h.harbor.shutdown()
  })

  test('a shutdown whose handlers finish in time leaves no deadline timer behind', async () => {
    // Regression: the 30s deadline sleep kept running after a clean shutdown,
    // holding the process open for the whole grace period.
    const h = harness()
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', async () => { await gate })
    await produced(h, 'orders', [1])
    await consumer.start()
    await settle()
    const closing = h.harbor.shutdown(30_000)
    await until(() => h.clock.waiting === 1)
    release()
    await closing
    assert.equal(h.clock.waiting, 0)
    assert.equal(h.adapter.committed('g', 'orders', 0), '1')
  })

  test('a failed commit is reported and the consumer carries on; the message is simply not committed', async () => {
    const h = harness()
    const events = capture(h)
    const original = h.adapter.consume
    let failures = 1
    h.adapter.consume = async (options) => {
      const handle = await original(options)
      const commit = handle.commit
      handle.commit = async (offsets) => {
        if (failures-- > 0) throw new Error('REBALANCE_IN_PROGRESS')
        await commit(offsets)
      }
      return handle
    }
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    let handled = 0
    consumer.subscribe('orders', () => { handled++ })
    await produced(h, 'orders', [1, 2])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(handled, 2)
    assert.equal(consumer.status, 'running')
    // The first message's commit failed: reported, not processed-as-committed, and the second commit still happened.
    assert.equal(events.error.length, 1)
    assert.equal(events.error[0]?.topic, 'orders')
    assert.equal(events.messageProcessed.length, 1)
    assert.equal(events.messageProcessed[0]?.offset, '1')
    assert.equal(h.adapter.committed('g', 'orders', 0), '2')
    assert.ok(h.logs.some((entry) => entry.level === 'warn' && entry.message.includes('commit failed')))
    await h.harbor.shutdown()
  })
})

describe('Consumer: adapter-level failures', () => {
  test('errors the adapter reports outside a message are emitted with scope adapter', async () => {
    const h = harness()
    const events = capture(h)
    const consumer = h.harbor.consumer({ groupId: 'g', autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    await consumer.start()
    const options = h.adapter.calls.find((call) => call.method === 'consume')?.args[0] as { onError: (error: unknown) => void }
    options.onError(new Error('fetch loop'))
    assert.equal(events.error.length, 1)
    assert.equal(events.error[0]?.scope, 'adapter')
    assert.equal(events.error[0]?.groupId, 'g')
    assert.equal(consumer.status, 'running')
    await h.harbor.shutdown()
  })

  test('a message on a topic the consumer never subscribed to stops it without a commit', async () => {
    const h = harness()
    const events = capture(h)
    const consumer = h.harbor.consumer({ groupId: 'g', autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    await consumer.start()
    const options = h.adapter.calls.find((call) => call.method === 'consume')?.args[0] as { eachMessage: (raw: unknown) => Promise<void> }
    await options.eachMessage({ topic: 'stray', partition: 0, offset: '0', key: null, value: Buffer.from('1'), headers: {}, timestamp: h.clock.now() })
    await until(() => consumer.status === 'stopped')
    assert.equal(events.error.length, 1)
    assert.equal((events.error[0]?.error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
    assert.equal(events.consumerStopped[0]?.reason, 'crash')
    assert.equal(h.adapter.calls.filter((call) => call.method === 'commit').length, 0)
    await h.harbor.shutdown()
  })

  test('a stop that fails while the pipeline is crashing is reported once more, not swallowed', async () => {
    const h = harness()
    const events = capture(h)
    const original = h.adapter.consume
    h.adapter.consume = async (options) => {
      const handle = await original(options)
      const stop = handle.stop
      handle.stop = async () => { await stop(); throw new Error('leave failed') }
      return handle
    }
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, dlq: { enabled: false } })
    consumer.subscribe('orders', () => { throw new Error('x') })
    await produced(h, 'orders', [1])
    await consumer.start()
    await until(() => consumer.status === 'stopped' && events.error.length === 2)
    assert.deepEqual(events.error.map((e) => (e.error as Error).message), ['x', 'leave failed'])
    await h.harbor.shutdown().catch(() => undefined)
  })
})

describe('Consumer: remaining branches', () => {
  test('two partitions crashing at once stop the consumer a single time', async () => {
    const h = harness({}, { partitions: 2 })
    const events = capture(h)
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, concurrency: 2, dlq: { enabled: false } })
    let started = 0
    consumer.subscribe('orders', async () => {
      started++
      await gate
      throw new Error('both fail')
    })
    const producer = h.harbor.producer()
    await producer.send('orders', { value: 1, partition: 0 })
    await producer.send('orders', { value: 2, partition: 1 })
    await consumer.start()
    await until(() => started === 2)
    release()
    await until(() => consumer.status === 'stopped')
    await settle()
    assert.equal(events.consumerStopped.length, 1)
    assert.equal(h.adapter.calls.filter((call) => call.method === 'stop').length, 1)
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    assert.equal(h.adapter.committed('g', 'orders', 1), undefined)
    await h.harbor.shutdown()
  })

  test('a commit that fails after a retry or DLQ produce is reported and the outcome event is withheld', async () => {
    const h = harness()
    const events = capture(h)
    const original = h.adapter.consume
    h.adapter.consume = async (options) => {
      const handle = await original(options)
      handle.commit = async () => { throw new Error('coordinator gone') }
      return handle
    }
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    consumer.subscribe('orders', () => { throw new Error('x') })
    await produced(h, 'orders', [1])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    await h.adapter.whenDrained('g', 'orders-retry-1')
    assert.equal(h.adapter.messages('orders-retry-1').length, 1)
    assert.equal(h.adapter.messages('orders-dlq').length, 1)
    assert.equal(events.messageRetried.length, 0)
    assert.equal(events.messageDeadLettered.length, 0)
    assert.equal(events.error.length, 2)
    assert.equal(consumer.status, 'running')
    await h.harbor.shutdown()
  })
})

describe('Consumer: mutation follow-ups', () => {
  test('non-string groupId and topic are rejected', () => {
    const h = harness()
    assert.throws(() => h.harbor.consumer({ groupId: 42 as never }), { code: ERROR_CODES.CONFIG_INVALID })
    const consumer = h.harbor.consumer({ groupId: 'g' })
    assert.throws(() => consumer.subscribe(42 as never, () => {}), { code: ERROR_CODES.CONFIG_INVALID })
  })

  test('stop() is single-flight and an idle consumer stops silently', async () => {
    const h = harness()
    const events = capture(h)
    const idle = h.harbor.consumer({ groupId: 'idle' })
    idle.subscribe('orders', () => {})
    const running = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    running.subscribe('orders', () => {})
    await running.start()
    await Promise.all([running.stop(), running.stop()])
    await running.stop()
    assert.equal(h.adapter.calls.filter((call) => call.method === 'stop').length, 1)
    await h.harbor.shutdown()
    assert.equal(idle.status, 'stopped')
    assert.deepEqual(events.consumerStopped.map((e) => e.groupId), ['g'])
  })

  test('auto-creation covers only the topics the plan needs: no DLQ when disabled, no call when nothing is derived', async () => {
    const h = harness()
    const noDlq = h.harbor.consumer({ groupId: 'a', autoCreateTopics: true, dlq: { enabled: false }, retry: { levels: [{ delay: 0 }] } })
    noDlq.subscribe('orders', () => {})
    await noDlq.start()
    assert.deepEqual((h.adapter.calls.find((call) => call.method === 'createTopics')?.args[0] as Array<{ topic: string }>).map((spec) => spec.topic), ['orders-retry-1'])
    const bare = h.harbor.consumer({ groupId: 'b', autoCreateTopics: true, dlq: { enabled: false } })
    bare.subscribe('payments', () => {})
    await bare.start()
    assert.equal(h.adapter.calls.filter((call) => call.method === 'createTopics').length, 1)
    await h.harbor.shutdown()
  })

  test('a delivery arriving while the consumer stops is neither handled nor committed', async () => {
    const h = harness()
    let handled = 0
    const consumer = h.harbor.consumer({ groupId: 'g', autoCreateTopics: true })
    consumer.subscribe('orders', () => { handled++ })
    await consumer.start()
    const options = h.adapter.calls.find((call) => call.method === 'consume')?.args[0] as { eachMessage: (raw: unknown) => Promise<void> }
    const stopping = consumer.stop()
    await options.eachMessage({ topic: 'orders', partition: 0, offset: '0', key: null, value: Buffer.from('1'), headers: {}, timestamp: h.clock.now() })
    await stopping
    assert.equal(handled, 0)
    assert.equal(h.adapter.calls.filter((call) => call.method === 'commit').length, 0)
    await h.harbor.shutdown()
  })

  test('tracking headers on the original topic are ignored: it is always a first delivery there', async () => {
    const h = harness()
    const seen: Array<{ retry: Message['retry'], attempt: number, hasRetryKey: boolean }> = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    consumer.subscribe('orders', (message, ctx) => { seen.push({ retry: message.retry, attempt: ctx.attempt, hasRetryKey: 'retry' in message }) })
    await h.harbor.connect()
    await h.adapter.produce([{ topic: 'orders', key: null, value: Buffer.from('1'), headers: { 'x-retry-count': '7', 'x-original-topic': 'orders', 'x-first-failure-at': new Date().toISOString() } }])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    assert.deepEqual(seen, [{ retry: undefined, attempt: 1, hasRetryKey: false }])
    assert.deepEqual(h.clock.sleeps, [], 'the original topic never waits')
    await h.harbor.shutdown()
  })

  test('a zero-delay retry level does not sleep at all', async () => {
    const h = harness()
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    consumer.subscribe('orders', () => { throw new Error('x') })
    await produced(h, 'orders', [1])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    await h.adapter.whenDrained('g', 'orders-retry-1')
    assert.deepEqual(h.clock.sleeps, [])
    assert.equal(h.adapter.messages('orders-dlq').length, 1)
    await h.harbor.shutdown()
  })

  test('an abort on one partition abandons the other partition\'s running handler: no commit even if it later succeeds', async () => {
    const h = harness({}, { partitions: 2 })
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    let slowSignal: AbortSignal | undefined
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, concurrency: 2 })
    consumer.subscribe<string>('orders', async (message, ctx) => {
      if (message.value === 'slow') {
        slowSignal = ctx.signal
        await gate
        return
      }
      throw h.harbor.abort(new Error('infra'))
    })
    const producer = h.harbor.producer<string>()
    await producer.send('orders', { value: 'slow', partition: 0 })
    await consumer.start()
    await until(() => slowSignal !== undefined)
    await producer.send('orders', { value: 'abort', partition: 1 })
    await until(() => consumer.status === 'stopped')
    assert.equal(slowSignal?.aborted, true)
    release()
    await settle()
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    assert.equal(h.adapter.committed('g', 'orders', 1), undefined)
    await h.harbor.shutdown()
  })

  test('every delivery removes its abort listener once settled', async () => {
    const { getEventListeners } = await import('node:events')
    const h = harness()
    let signal: AbortSignal | undefined
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', (_message, ctx) => { signal = ctx.signal })
    await produced(h, 'orders', [1, 2, 3, 4, 5])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    await settle()
    assert.ok(signal)
    assert.equal(getEventListeners(signal, 'abort').length, 0)
    await h.harbor.shutdown()
    assert.equal(signal.aborted, false, 'a clean shutdown abandons nothing, so the signal never aborts')
  })

  test('an abandoned handler that finishes while the group is still being left does not commit', async () => {
    const h = harness()
    const original = h.adapter.consume
    let releaseStop!: () => void
    const stopGate = new Promise<void>((resolve) => { releaseStop = resolve })
    h.adapter.consume = async (options) => {
      const handle = await original(options)
      const stop = handle.stop
      handle.stop = async () => { await stopGate; await stop() }
      return handle
    }
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', async () => { await gate })
    await produced(h, 'orders', [1])
    await consumer.start()
    await settle()
    const closing = h.harbor.shutdown(1_000)
    await until(() => h.clock.waiting === 1)
    h.clock.advance(1_000)
    await until(() => consumer.status === 'stopping')
    // The handler completes after abandonment while the handle is still open.
    release()
    await settle()
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    releaseStop()
    await assert.rejects(closing, { code: ERROR_CODES.SHUTDOWN_TIMEOUT })
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
  })

  test('a start() that fails while a stop is waiting leaves the consumer stopped without a handle to close', async () => {
    const h = harness()
    const events = capture(h)
    let releaseAdmin!: () => void
    const adminGate = new Promise<void>((resolve) => { releaseAdmin = resolve })
    h.adapter.admin.createTopics = async () => { await adminGate; throw new Error('admin down') }
    const consumer = h.harbor.consumer({ groupId: 'g', autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    const starting = consumer.start()
    await settle()
    const closing = h.harbor.shutdown()
    await settle()
    releaseAdmin()
    await assert.rejects(starting, /admin down/)
    await closing
    assert.equal(consumer.status, 'stopped')
    assert.equal(h.adapter.calls.filter((call) => call.method === 'stop').length, 0)
    assert.equal(events.consumerStopped.length, 0)
  })
})

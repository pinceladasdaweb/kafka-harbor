import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { Idempotency } from 'quayside'
import { MemoryStorage } from 'quayside/memory'

import { AdapterError, type HarborEvents, type IdempotencyEngine, type Instrumentation } from '../../src/index'
import { captureErrors, harness, type Harness } from '../helpers/harness'
import { until } from '../helpers/manual-clock'

interface Order { id: string, total: number }

/** One quayside engine on a shared in-memory storage, on the harness clock. */
const engineOn = (storage: MemoryStorage, h: Harness, options: Partial<ConstructorParameters<typeof Idempotency>[0]> = {}): Idempotency =>
  new Idempotency({ storage, clock: { now: () => h.clock.now(), sleep: async (ms) => { await h.clock.sleep(ms) } }, ...options })

const processed = (h: Harness): Array<HarborEvents['messageProcessed']> => {
  const events: Array<HarborEvents['messageProcessed']> = []
  h.harbor.on('messageProcessed', (event) => { events.push(event) })
  return events
}

describe('consumer idempotency through quayside', () => {
  test('a redelivery of the same delivery (group, topic, partition, offset) replays the outcome and commits without running the handler', async () => {
    const storage = new MemoryStorage()
    const runs: string[] = []
    // Two processes of the same group, one after the other, sharing the
    // storage: the second one sees every message again, as after a crash
    // between the handler and the commit.
    for (const round of ['first', 'second']) {
      const h = harness()
      const events = processed(h)
      const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, idempotency: { engine: engineOn(storage, h) } })
      consumer.subscribe<Order>('orders', (message) => { runs.push(`${round}:${message.value.id}`) })
      await consumer.start()
      await h.harbor.producer<Order>().sendBatch('orders', [{ value: { id: 'a', total: 1 } }, { value: { id: 'b', total: 2 } }])
      await h.adapter.whenDrained('g', 'orders')
      assert.deepEqual(events.map((event) => event.replayed), round === 'first' ? [false, false] : [true, true])
      assert.equal(h.adapter.committed('g', 'orders', 0), '2', 'replays are committed like any processed message')
      assert.deepEqual(h.logs.filter((log) => log.level !== 'info'), [], 'the engine ran as the work itself, not as instrumentation')
      await h.harbor.shutdown()
    }
    assert.deepEqual(runs, ['first:a', 'first:b'])
  })

  test('the default key is per group: another group sharing the storage runs its own handler', async () => {
    const storage = new MemoryStorage()
    const h = harness()
    const runs: string[] = []
    for (const groupId of ['billing', 'shipping']) {
      const consumer = h.harbor.consumer({ groupId, fromBeginning: true, autoCreateTopics: true, idempotency: { engine: engineOn(storage, h) } })
      consumer.subscribe('orders', () => { runs.push(groupId) })
      await consumer.start()
    }
    await h.harbor.producer().send('orders', { value: 1 })
    await h.adapter.whenDrained('billing', 'orders')
    await h.adapter.whenDrained('shipping', 'orders')
    assert.deepEqual(runs.sort(), ['billing', 'shipping'])
    await h.harbor.shutdown()
  })

  test('a business key collapses duplicates the producer sent, whatever their offsets', async () => {
    const h = harness()
    const events = processed(h)
    let runs = 0
    const consumer = h.harbor.consumer({
      groupId: 'g',
      fromBeginning: true,
      autoCreateTopics: true,
      idempotency: { engine: engineOn(new MemoryStorage(), h), key: (message) => `order:${(message.value as Order).id}` }
    })
    consumer.subscribe<Order>('orders', () => { runs++ })
    await consumer.start()
    await h.harbor.producer<Order>().sendBatch('orders', [{ value: { id: 'a', total: 1 } }, { value: { id: 'a', total: 1 } }, { value: { id: 'b', total: 2 } }])
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(runs, 2)
    assert.deepEqual(events.map((event) => [event.offset, event.replayed]), [['0', false], ['1', true], ['2', false]])
    await h.harbor.shutdown()
  })

  test('with the payload in the key, the same key with different content is refused; retryIf sends the refusal straight to the DLQ', async () => {
    const h = harness()
    let runs = 0
    const consumer = h.harbor.consumer({
      groupId: 'g',
      fromBeginning: true,
      autoCreateTopics: true,
      retry: { levels: [{ delay: 0 }], retryIf: (error) => (error as { code?: string }).code !== 'IDEMPOTENCY_KEY_REUSE' }
    })
    consumer.subscribe<Order>('orders', () => { runs++ }, {
      idempotency: { engine: engineOn(new MemoryStorage(), h), key: (message) => ({ key: `order:${message.value.id}`, payload: message.value }) }
    })
    await consumer.start()
    await h.harbor.producer<Order>().sendBatch('orders', [{ value: { id: 'a', total: 1 } }, { value: { id: 'a', total: 99 } }])
    await until(() => h.adapter.messages('orders-dlq').length === 1)
    assert.equal(runs, 1)
    assert.equal(h.adapter.messages('orders-retry-1').length, 0, 'a deterministic refusal skipped the ladder')
    assert.match(String(h.adapter.messages('orders-dlq')[0]?.headers['x-last-error']), /IdempotencyKeyReuseError/)
    await h.harbor.shutdown()
  })

  test('a failure is not replayed: the retry runs the handler again; a persisted failure is replayed with its retryable flag', async () => {
    const storage = new MemoryStorage()
    const h = harness()
    const attempts: number[] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] }, idempotency: { engine: engineOn(storage, h) } })
    consumer.subscribe('orders', (_message, ctx) => {
      attempts.push(ctx.attempt)
      if (ctx.attempt === 1) throw new Error('transient')
    })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await until(() => attempts.length === 2)
    await h.adapter.whenDrained('g', 'orders-retry-1')
    assert.deepEqual(attempts, [1, 2])
    await h.harbor.shutdown()

    // persistFailures: a business rejection is stored; the redelivery of the
    // same delivery gets the stored error back, retryable: false included,
    // and goes straight to the DLQ without the handler running again.
    let runs = 0
    for (const round of [1, 2]) {
      const h2 = harness()
      const failed: Array<HarborEvents['messageFailed']> = []
      h2.harbor.on('messageFailed', (event) => { failed.push(event) })
      const persisted = h2.harbor.consumer({ groupId: 'p', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] }, idempotency: { engine: engineOn(storage, h2, { persistFailures: true }) } })
      persisted.subscribe('orders', () => { runs++; throw Object.assign(new Error('bad sku'), { retryable: false, code: 'BAD_SKU' }) })
      await persisted.start()
      await h2.harbor.producer().send('orders', { value: 1 })
      // The event follows the commit, which follows the DLQ produce.
      await until(() => failed.length === 1)
      assert.equal(failed[0]?.outcome, 'dead-letter')
      assert.equal((failed[0]?.error as { code: string }).code, 'BAD_SKU', `round ${round}`)
      assert.equal(runs, 1, 'the second round replayed the stored failure')
      await h2.harbor.shutdown()
    }
  })

  test('a key function that throws fails the message like the handler would, and the handler does not run', async () => {
    const h = harness()
    let runs = 0
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, idempotency: { engine: engineOn(new MemoryStorage(), h), key: () => { throw new Error('no id') } } })
    consumer.subscribe('orders', () => { runs++ })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await until(() => h.adapter.messages('orders-dlq').length === 1)
    assert.equal(runs, 0)
    assert.equal(h.adapter.messages('orders-dlq')[0]?.headers['x-last-error'], 'no id')
    await h.harbor.shutdown()
  })

  test('an engine that fails (storage away) is a handler failure: retried, then run when the storage is back', async () => {
    const h = harness()
    const errors = captureErrors(h.harbor)
    let calls = 0
    let runs = 0
    const flaky: IdempotencyEngine = {
      async executeWithMetadata (_input, run) {
        if (calls++ === 0) throw new AdapterError('storage unavailable')
        return { value: await run(), replayed: false }
      }
    }
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] }, idempotency: { engine: flaky } })
    consumer.subscribe('orders', () => { runs++ })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await until(() => runs === 1)
    await h.adapter.whenDrained('g', 'orders-retry-1')
    assert.equal(calls, 2)
    assert.equal(h.adapter.messages('orders-dlq').length, 0)
    assert.equal(errors.length, 0)
    await h.harbor.shutdown()
  })

  test('a subscription may override the consumer engine, and its key function sees the topic\'s message type', async () => {
    const h = harness()
    const seen: string[] = []
    const engine = (name: string): IdempotencyEngine => ({
      async executeWithMetadata (input, run) {
        seen.push(`${name}:${typeof input === 'string' ? input : input.key}`)
        return { value: await run(), replayed: false }
      }
    })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, idempotency: { engine: engine('consumer') } })
    consumer.subscribe('orders', () => {})
    consumer.subscribe<Order>('payments', () => {}, { idempotency: { engine: engine('payments'), key: (message) => `pay:${message.value.id}` } })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await h.harbor.producer<Order>().send('payments', { value: { id: 'p1', total: 2 } })
    await h.adapter.whenDrained('g', 'orders')
    await h.adapter.whenDrained('g', 'payments')
    assert.deepEqual(seen.sort(), ['consumer:g:orders:0:0', 'payments:pay:p1'])
    await h.harbor.shutdown()
  })

  test('a consumer without idempotency and without instrumentation calls the handler directly and logs nothing', async () => {
    const h = harness()
    const events = processed(h)
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await h.adapter.whenDrained('g', 'orders')
    assert.deepEqual(events.map((event) => event.replayed), [false])
    assert.deepEqual(h.logs.filter((log) => log.level !== 'info'), [])
    await h.harbor.shutdown()
  })

  test('the engine runs inside the instrumentation, so a replay is a short handler span too', async () => {
    const storage = new MemoryStorage()
    const calls: string[] = []
    const instrumentation: Instrumentation = {
      async wrapHandler (message, _context, run) {
        calls.push(`wrap ${message.offset}`)
        return await run()
      }
    }
    for (const round of ['first', 'second']) {
      const h = harness({ instrumentation })
      const events = processed(h)
      const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, idempotency: { engine: engineOn(storage, h) } })
      consumer.subscribe('orders', () => { calls.push(`handler ${round}`) })
      await consumer.start()
      await h.harbor.producer().send('orders', { value: 1 })
      await h.adapter.whenDrained('g', 'orders')
      assert.equal(events[0]?.replayed, round === 'second')
      await h.harbor.shutdown()
    }
    assert.deepEqual(calls, ['wrap 0', 'handler first', 'wrap 0'])
  })
})

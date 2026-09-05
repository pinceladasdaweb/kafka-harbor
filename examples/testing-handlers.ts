/**
 * Testing handlers without a broker: the in-memory adapter from
 * `kafka-harbor/testing` runs the same core code as production, with topics,
 * partitions, consumer groups and committed offsets, so a handler test can
 * assert on outcomes (retried, dead-lettered, committed) instead of mocking
 * the client. Runs under `node --test`; `npm run examples` executes it.
 */
import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { createHarbor, type Handler, type HarborEvents } from '../src/index'
import { memoryAdapter } from '../src/testing/index'

interface Order { id: string, total: number }

/** The handler under test: rejects negative totals for good, fails transiently on a flaky dependency. */
const makeHandler = (deps: { charge: (order: Order) => Promise<void> }): Handler<Order> => async (message) => {
  if (message.value.total < 0) {
    throw Object.assign(new Error(`order ${message.value.id} has a negative total`), { retryable: false })
  }
  await deps.charge(message.value)
}

/** One harbor per test, on a fresh in-memory broker, with the events captured. */
const setup = () => {
  const adapter = memoryAdapter()
  const harbor = createHarbor({ clientId: 'orders-test', brokers: ['memory'], adapter, logger: { info () {}, warn () {}, error () {} } })
  const events: Array<{ type: keyof HarborEvents, payload: unknown }> = []
  for (const type of ['messageProcessed', 'messageRetried', 'messageDeadLettered', 'error'] as const) {
    harbor.on(type, (payload) => { events.push({ type, payload }) })
  }
  const consumer = harbor.consumer({ groupId: 'orders', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }, { delay: 0 }] } })
  return { adapter, harbor, consumer, events }
}

describe('order handler', () => {
  test('charges the order and commits', async () => {
    const { adapter, harbor, consumer, events } = setup()
    const charged: string[] = []
    consumer.subscribe('orders', makeHandler({ charge: async (order) => { charged.push(order.id) } }))
    await harbor.producer<Order>().send('orders', { key: 'o1', value: { id: 'o1', total: 10 } })
    await consumer.start()
    await adapter.whenDrained('orders', 'orders')

    assert.deepEqual(charged, ['o1'])
    assert.equal(adapter.committed('orders', 'orders', 0), '1')
    assert.deepEqual(events.map((e) => e.type), ['messageProcessed'])
    await harbor.shutdown()
  })

  test('a flaky dependency is retried through the ladder and succeeds on the second attempt', async () => {
    const { adapter, harbor, consumer, events } = setup()
    let calls = 0
    consumer.subscribe('orders', makeHandler({
      charge: async () => {
        if (calls++ === 0) throw new Error('payment gateway timeout')
      }
    }))
    await harbor.producer<Order>().send('orders', { key: 'o2', value: { id: 'o2', total: 20 } })
    await consumer.start()
    await adapter.whenDrained('orders', 'orders')
    await adapter.whenDrained('orders', 'orders-retry-1')

    assert.equal(calls, 2)
    assert.equal(adapter.messages('orders-retry-1').length, 1)
    assert.equal(adapter.messages('orders-dlq').length, 0)
    assert.deepEqual(events.map((e) => e.type), ['messageRetried', 'messageProcessed'])
    await harbor.shutdown()
  })

  test('a business rule violation goes straight to the DLQ with the reason on it', async () => {
    const { adapter, harbor, consumer, events } = setup()
    consumer.subscribe('orders', makeHandler({ charge: async () => {} }))
    await harbor.producer<Order>().send('orders', { key: 'o3', value: { id: 'o3', total: -5 } })
    await consumer.start()
    await adapter.whenDrained('orders', 'orders')

    const [dead] = adapter.messages('orders-dlq')
    assert.ok(dead)
    assert.equal(dead.headers['x-last-error'], 'order o3 has a negative total')
    assert.equal(dead.headers['x-retry-count'], '1')
    assert.equal(adapter.messages('orders-retry-1').length, 0, 'retryable: false skips the ladder')
    assert.deepEqual(events.map((e) => e.type), ['messageDeadLettered'])
    await harbor.shutdown()
  })
})

import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { ERROR_CODES, type HarborEvents } from '../../src/index'
import { harness, json, type Harness } from '../helpers/harness'
import { settle, until } from '../helpers/manual-clock'

/** Puts dead-lettered messages on `orders-dlq` the way the consumer would have. */
const deadLetter = async (h: Harness, entries: Array<{ key?: string, value: unknown, original?: string, extra?: Record<string, string> }>): Promise<void> => {
  await h.harbor.connect()
  h.adapter.createTopic('orders')
  h.adapter.createTopic('payments')
  const at = new Date(h.clock.now()).toISOString()
  await h.adapter.produce(entries.map((entry) => ({
    topic: 'orders-dlq',
    key: entry.key === undefined ? null : Buffer.from(entry.key),
    value: Buffer.from(JSON.stringify(entry.value)),
    headers: {
      'x-correlation-id': `corr-${String(entry.value)}`,
      'x-tenant': 'acme',
      ...(entry.original !== undefined && { 'x-original-topic': entry.original }),
      'x-retry-count': '3',
      'x-first-failure-at': at,
      'x-last-error': 'attempt 3 failed',
      'x-dead-lettered-at': at,
      ...entry.extra
    }
  })))
}

/** Runs a redrive on the manual clock: idle time only passes when advanced. */
const drive = async (h: Harness, options: Parameters<Harness['harbor']['redrive']>[0], pump: () => Promise<void> = async () => {}) => {
  const result = h.harbor.redrive(options)
  await pump()
  await settle(20)
  // Nothing left in flight: let the idle timer expire.
  await until(() => h.clock.waiting >= 1)
  h.clock.advance(5_000)
  await settle(20)
  return await result
}

describe('harbor.redrive', () => {
  test('re-injects each dead letter into its original topic with the trail removed, then commits', async () => {
    const h = harness()
    const events: Array<HarborEvents['messageRedriven']> = []
    h.harbor.on('messageRedriven', (payload) => { events.push(payload) })
    await deadLetter(h, [
      { key: 'o1', value: 1, original: 'orders' },
      { key: 'p1', value: 2, original: 'payments' },
      { value: 3, original: 'orders' }
    ])

    const redrivenAt = new Date(h.clock.now()).toISOString()
    const result = await drive(h, { from: 'orders-dlq' })

    assert.deepEqual(result, { from: 'orders-dlq', reprocessed: 3, skipped: 0 })
    const orders = h.adapter.messages('orders')
    assert.deepEqual(orders.map((m) => json(m.value)), [1, 3])
    assert.equal(orders[0]?.key?.toString(), 'o1')
    assert.equal(orders[1]?.key, null)
    assert.deepEqual(Object.keys(orders[0]?.headers ?? {}).sort(), [
      'x-correlation-id', 'x-produced-at', 'x-producer', 'x-redriven-at', 'x-redriven-from', 'x-tenant'
    ])
    assert.equal(orders[0]?.headers['x-correlation-id'], 'corr-1')
    assert.equal(orders[0]?.headers['x-redriven-from'], 'orders-dlq')
    assert.equal(orders[0]?.headers['x-redriven-at'], redrivenAt)
    assert.equal(orders[0]?.headers['x-producer'], 'test-app')
    assert.deepEqual(h.adapter.messages('payments').map((m) => json(m.value)), [2])
    assert.equal(h.adapter.committed('orders-dlq-redrive', 'orders-dlq', 0), '3')
    assert.equal(events.length, 3)
    assert.deepEqual(events.map((e) => e.to), ['orders', 'payments', 'orders'])
    assert.equal(events[0]?.correlationId, 'corr-1')
    assert.equal(events[0]?.groupId, 'orders-dlq-redrive')
    // The redrive left the group; the harbor is still usable.
    assert.equal(h.adapter.calls.filter((call) => call.method === 'stop').length, 1)
    assert.equal(h.harbor.status, 'connected')
    await h.harbor.shutdown()
  })

  test('an explicit destination and group override the header and the default name', async () => {
    const h = harness()
    await deadLetter(h, [{ value: 1, original: 'orders' }, { value: 2 }])
    h.adapter.createTopic('quarantine')
    const result = await drive(h, { from: 'orders-dlq', to: 'quarantine', groupId: 'ops' })
    assert.deepEqual(result, { from: 'orders-dlq', reprocessed: 2, skipped: 0 })
    assert.deepEqual(h.adapter.messages('quarantine').map((m) => json(m.value)), [1, 2])
    assert.equal(h.adapter.committed('ops', 'orders-dlq', 0), '2')
    await h.harbor.shutdown()
  })

  test('max stops after that many messages and commits only them', async () => {
    const h = harness()
    await deadLetter(h, [{ value: 1, original: 'orders' }, { value: 2, original: 'orders' }, { value: 3, original: 'orders' }])
    const result = await h.harbor.redrive({ from: 'orders-dlq', max: 2 })
    assert.deepEqual(result, { from: 'orders-dlq', reprocessed: 2, skipped: 0 })
    assert.deepEqual(h.adapter.messages('orders').map((m) => json(m.value)), [1, 2])
    assert.equal(h.adapter.committed('orders-dlq-redrive', 'orders-dlq', 0), '2')
    // A second run resumes from the committed offset and ends on idle.
    const again = await drive(h, { from: 'orders-dlq', max: 5 })
    assert.deepEqual(again, { from: 'orders-dlq', reprocessed: 1, skipped: 0 })
    assert.deepEqual(h.adapter.messages('orders').map((m) => json(m.value)), [1, 2, 3])
    assert.equal(h.adapter.committed('orders-dlq-redrive', 'orders-dlq', 0), '3')
    await h.harbor.shutdown()
  })

  test('filter skips messages (committed, not re-injected) and a payload that does not deserialize is skipped with an error event', async () => {
    const h = harness()
    const errors: unknown[] = []
    h.harbor.on('error', ({ error }) => { errors.push(error) })
    await deadLetter(h, [{ value: 1, original: 'orders' }, { value: 2, original: 'orders' }])
    await h.adapter.produce([{ topic: 'orders-dlq', key: null, value: Buffer.from('{broken'), headers: { 'x-original-topic': 'orders' } }])
    const seen: unknown[] = []
    const result = await drive(h, {
      from: 'orders-dlq',
      filter: (message) => {
        seen.push({ value: message.value, retry: message.retry?.count, topic: message.topic })
        return message.value !== 2
      }
    })
    assert.deepEqual(result, { from: 'orders-dlq', reprocessed: 1, skipped: 2 })
    assert.deepEqual(seen, [{ value: 1, retry: 3, topic: 'orders-dlq' }, { value: 2, retry: 3, topic: 'orders-dlq' }])
    assert.deepEqual(h.adapter.messages('orders').map((m) => json(m.value)), [1])
    assert.equal(h.adapter.committed('orders-dlq-redrive', 'orders-dlq', 0), '3')
    assert.equal(errors.length, 1)
    assert.equal((errors[0] as { code: string }).code, ERROR_CODES.SERIALIZATION)
    await h.harbor.shutdown()
  })

  test('a filter that throws stops the redrive with that error and commits nothing for the message', async () => {
    const h = harness()
    await deadLetter(h, [{ value: 1, original: 'orders' }])
    await assert.rejects(h.harbor.redrive({ from: 'orders-dlq', filter: () => { throw new Error('filter bug') } }), /filter bug/)
    assert.equal(h.adapter.messages('orders').length, 0)
    assert.equal(h.adapter.committed('orders-dlq-redrive', 'orders-dlq', 0), undefined)
    assert.ok(h.logs.some((entry) => entry.level === 'error' && entry.message.includes('redrive of "orders-dlq" stopped')))
    await h.harbor.shutdown()
  })

  test('a message without an original-topic header and no explicit destination fails the redrive, uncommitted', async () => {
    const h = harness()
    await deadLetter(h, [{ value: 1, original: 'orders' }, { value: 2 }])
    await assert.rejects(h.harbor.redrive({ from: 'orders-dlq' }), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
      assert.match((error as Error).message, /x-original-topic/)
      return true
    })
    // The first message went through and was committed; the second stayed.
    assert.deepEqual(h.adapter.messages('orders').map((m) => json(m.value)), [1])
    assert.equal(h.adapter.committed('orders-dlq-redrive', 'orders-dlq', 0), '1')
    await h.harbor.shutdown()
  })

  test('a re-produce that is not acknowledged stops the redrive with the offset uncommitted', async () => {
    const h = harness()
    await deadLetter(h, [{ value: 1, original: 'orders' }])
    h.adapter.failNextProduce(Object.assign(new Error('broker refused'), { retryable: false }))
    await assert.rejects(h.harbor.redrive({ from: 'orders-dlq' }), /broker refused/)
    assert.equal(h.adapter.committed('orders-dlq-redrive', 'orders-dlq', 0), undefined)
    await h.harbor.shutdown()
  })

  test('an empty DLQ returns zero after the idle timeout, honoring a duration string', async () => {
    const h = harness()
    await h.harbor.connect()
    h.adapter.createTopic('orders-dlq')
    const result = h.harbor.redrive({ from: 'orders-dlq', idleTimeout: '2s' })
    await until(() => h.clock.waiting === 1)
    assert.deepEqual(h.clock.sleeps, [2_000])
    h.clock.advance(2_000)
    assert.deepEqual(await result, { from: 'orders-dlq', reprocessed: 0, skipped: 0 })
    await h.harbor.shutdown()
  })

  test('the filter sees tombstones as null values, keys as strings, and no retry block when the headers carry none', async () => {
    const h = harness()
    await h.harbor.connect()
    h.adapter.createTopic('orders')
    await h.adapter.produce([{ topic: 'orders-dlq', key: Buffer.from('k1'), value: null, headers: { 'x-original-topic': 'orders' } }])
    const seen: unknown[] = []
    const result = await drive(h, {
      from: 'orders-dlq',
      filter: (message) => {
        seen.push({ key: message.key, value: message.value, hasRetry: 'retry' in message })
        return true
      }
    })
    assert.deepEqual(result, { from: 'orders-dlq', reprocessed: 1, skipped: 0 })
    assert.deepEqual(seen, [{ key: 'k1', value: null, hasRetry: false }])
    assert.equal(h.adapter.messages('orders')[0]?.value, null)
    await h.harbor.shutdown()
  })

  test('a delivery that lands after the run finished is ignored: nothing produced, nothing committed', async () => {
    const h = harness()
    await deadLetter(h, [{ value: 1, original: 'orders' }, { value: 2, original: 'orders' }])
    let eachMessage: ((raw: unknown) => Promise<void>) | undefined
    const original = h.adapter.consume
    h.adapter.consume = async (options) => {
      eachMessage = options.eachMessage as (raw: unknown) => Promise<void>
      return await original(options)
    }
    const result = await drive(h, { from: 'orders-dlq' })
    assert.deepEqual(result, { from: 'orders-dlq', reprocessed: 2, skipped: 0 })
    const before = h.adapter.calls.length
    await eachMessage!({ topic: 'orders-dlq', partition: 0, offset: '2', key: null, value: Buffer.from('3'), headers: { 'x-original-topic': 'orders' }, timestamp: h.clock.now() })
    assert.equal(h.adapter.calls.length, before)
    assert.deepEqual(h.adapter.messages('orders').map((m) => json(m.value)), [1, 2])
    assert.equal(h.adapter.committed('orders-dlq-redrive', 'orders-dlq', 0), '2')
    await h.harbor.shutdown()
  })

  test('activity inside the idle window keeps the run alive; the run ends only after a full quiet window', async () => {
    const h = harness()
    await deadLetter(h, [{ value: 1, original: 'orders' }])
    const result = h.harbor.redrive({ from: 'orders-dlq', idleTimeout: '4s' })
    await settle(20)
    await until(() => h.clock.waiting === 1)
    // Halfway through the window a new dead letter lands.
    h.clock.advance(2_000)
    await h.adapter.produce([{ topic: 'orders-dlq', key: null, value: Buffer.from('2'), headers: { 'x-original-topic': 'orders' } }])
    await settle(20)
    // The first wake-up sees recent activity and keeps going.
    h.clock.advance(2_000)
    await settle(20)
    await until(() => h.clock.waiting === 1)
    // Another one, then a full quiet window ends the run.
    h.clock.advance(1_000)
    await h.adapter.produce([{ topic: 'orders-dlq', key: null, value: Buffer.from('3'), headers: { 'x-original-topic': 'orders' } }])
    await settle(20)
    h.clock.advance(3_000)
    await settle(20)
    await until(() => h.clock.waiting === 1)
    h.clock.advance(4_000)
    assert.deepEqual(await result, { from: 'orders-dlq', reprocessed: 3, skipped: 0 })
    assert.deepEqual(h.clock.sleeps, [4_000, 4_000, 4_000])
    await h.harbor.shutdown()
  })

  test('fetch-loop errors reported by the adapter surface as error events with the redrive group', async () => {
    const h = harness()
    await deadLetter(h, [{ value: 1, original: 'orders' }])
    const errors: Array<HarborEvents['error']> = []
    h.harbor.on('error', (payload) => { errors.push(payload) })
    let onError: ((error: unknown) => void) | undefined
    const original = h.adapter.consume
    h.adapter.consume = async (options) => {
      onError = options.onError
      return await original(options)
    }
    await drive(h, { from: 'orders-dlq' }, async () => {
      await until(() => onError !== undefined)
      onError!(new Error('fetch loop'))
    })
    assert.equal(errors.length, 1)
    assert.equal(errors[0]?.scope, 'adapter')
    assert.equal(errors[0]?.groupId, 'orders-dlq-redrive')
    assert.equal(errors[0]?.topic, 'orders-dlq')
    await h.harbor.shutdown()
  })

  test('validates its options and refuses to run on a closed harbor', async () => {
    const h = harness()
    await assert.rejects(h.harbor.redrive({ from: '' }), { code: ERROR_CODES.CONFIG_INVALID })
    await assert.rejects(h.harbor.redrive({ from: 'd', to: '' }), { code: ERROR_CODES.CONFIG_INVALID })
    await assert.rejects(h.harbor.redrive({ from: 'd', max: 0 }), { code: ERROR_CODES.CONFIG_INVALID })
    await assert.rejects(h.harbor.redrive({ from: 'd', idleTimeout: '5' }), { code: ERROR_CODES.CONFIG_INVALID })
    await h.harbor.shutdown()
    await assert.rejects(h.harbor.redrive({ from: 'd' }), { code: ERROR_CODES.CLOSED })
  })
})

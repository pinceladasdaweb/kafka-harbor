import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { AdapterError, ERROR_CODES, type HarborEvents, type TransactionHandle } from '../../src/index'
import { captureEvents, harness, json } from '../helpers/harness'
import { until } from '../helpers/manual-clock'

const transactional = (extra: Parameters<typeof harness>[0] = {}) => harness({ transactionalId: 'shop-1', ...extra })
const methods = (h: ReturnType<typeof harness>): string[] => h.adapter.calls.map((call) => call.method).filter((method) => method.startsWith('transaction'))

describe('harbor.transaction()', () => {
  test('what the function produced lands together on commit, reported as one transaction and as transactional produces', async () => {
    const h = transactional()
    const completed = captureEvents(h.harbor, 'transactionCompleted')
    const produced = captureEvents(h.harbor, 'messageProduced')
    h.adapter.createTopic('orders')
    h.adapter.createTopic('audit')
    const result = await h.harbor.transaction(async (tx) => {
      h.clock.advance(250)
      await tx.send('orders', { key: 'a', value: { id: 'a' } })
      await tx.sendBatch('audit', [{ value: 'placed a' }, { value: 'charged a' }])
      // Nothing is visible before the commit.
      assert.equal(h.adapter.messages('orders').length, 0)
      assert.equal(h.adapter.messages('audit').length, 0)
      return 'done'
    })
    assert.equal(result, 'done')
    assert.deepEqual(h.adapter.messages('orders').map((m) => json(m.value)), [{ id: 'a' }])
    assert.deepEqual(h.adapter.messages('audit').map((m) => json(m.value)), ['placed a', 'charged a'])
    assert.equal(h.adapter.messages('orders')[0]?.headers['x-producer'], 'test-app')
    assert.deepEqual(methods(h), ['transaction', 'transactionProduce', 'transactionProduce', 'transactionCommit'])
    assert.deepEqual(completed.map((event) => [event.outcome, event.records, event.durationMs]), [['committed', 3, 250]])
    assert.deepEqual(produced.map((event) => [event.topic, event.kind, event.records]), [['orders', 'transaction', 1], ['audit', 'transaction', 2]])
    await h.harbor.shutdown()
  })

  test('a function that throws aborts the transaction: nothing lands, the error is the caller\'s, and the abort is reported', async () => {
    const h = transactional()
    const completed = captureEvents(h.harbor, 'transactionCompleted')
    h.adapter.createTopic('orders')
    await assert.rejects(h.harbor.transaction(async (tx) => {
      await tx.send('orders', { value: 1 })
      h.clock.advance(40)
      throw new Error('payment declined')
    }), /payment declined/)
    assert.equal(h.adapter.messages('orders').length, 0)
    assert.deepEqual(methods(h), ['transaction', 'transactionProduce', 'transactionAbort'])
    assert.deepEqual(completed.map((event) => [event.outcome, event.records, event.durationMs, (event.error as Error).message]), [['aborted', 1, 40, 'payment declined']])
    await h.harbor.shutdown()
  })

  test('a produce the broker refuses, or a value that does not serialize, aborts as well; an empty batch produces nothing', async () => {
    const h = transactional()
    h.adapter.createTopic('orders')
    h.adapter.failNextProduce(new AdapterError('broker down'))
    await assert.rejects(h.harbor.transaction(async (tx) => { await tx.send('orders', { value: 1 }) }), { code: ERROR_CODES.ADAPTER })
    assert.deepEqual(methods(h), ['transaction', 'transactionProduce', 'transactionAbort'], 'no retry inside a transaction')
    h.adapter.clearCalls()
    await assert.rejects(h.harbor.transaction(async (tx) => { await tx.send('orders', { value: 1n }) }), { code: ERROR_CODES.SERIALIZATION })
    assert.deepEqual(methods(h), ['transaction', 'transactionAbort'])
    h.adapter.clearCalls()
    await h.harbor.transaction(async (tx) => { await tx.sendBatch('orders', []) })
    assert.deepEqual(methods(h), ['transaction', 'transactionCommit'])
    assert.equal(h.adapter.messages('orders').length, 0)
    await h.harbor.shutdown()
  })

  test('a commit the broker refuses is the caller\'s error; the abort that follows is best effort', async () => {
    const h = transactional()
    const original = h.adapter.transaction!.bind(h.adapter)
    h.adapter.transaction = async (): Promise<TransactionHandle> => {
      const handle = await original()
      return { ...handle, commit: async () => { throw new AdapterError('fenced', { retryable: false }) }, abort: async () => { throw new Error('gone too') } }
    }
    const completed = captureEvents(h.harbor, 'transactionCompleted')
    await assert.rejects(h.harbor.transaction(async (tx) => { await tx.send('orders', { value: 1 }) }), /fenced/)
    assert.equal(completed[0]?.outcome, 'aborted')
    assert.ok(h.logs.some((entry) => entry.level === 'warn' && entry.message.includes('transaction abort failed after') && entry.message.includes('fenced') && entry.message.includes('gone too')))
    await h.harbor.shutdown()
  })

  test('transactions run one at a time, in the order they were asked for, whatever happened to the previous one', async () => {
    const h = transactional()
    h.adapter.createTopic('orders')
    const order: string[] = []
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const first = h.harbor.transaction(async (tx) => {
      order.push('first started')
      await gate
      await tx.send('orders', { value: 'first' })
      throw new Error('first fails')
    })
    const second = h.harbor.transaction(async (tx) => {
      order.push('second started')
      await tx.send('orders', { value: 'second' })
    })
    await until(() => order.length === 1)
    assert.deepEqual(order, ['first started'])
    release()
    await assert.rejects(first, /first fails/)
    await second
    assert.deepEqual(order, ['first started', 'second started'])
    assert.deepEqual(h.adapter.messages('orders').map((m) => json(m.value)), ['second'])
    await h.harbor.shutdown()
  })

  test('refused without transactionalId, without an adapter that has transactions, on a closed harbor, and without a function', async () => {
    const plain = harness()
    await assert.rejects(plain.harbor.transaction(async () => {}), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
      assert.match((error as Error).message, /transactionalId/)
      return true
    })
    await plain.harbor.shutdown()

    const unable = transactional()
    delete (unable.adapter as { transaction?: unknown }).transaction
    await assert.rejects(unable.harbor.transaction(async () => {}), { code: ERROR_CODES.CONFIG_INVALID, message: /adapter "memory" has no transactions/ })
    await unable.harbor.shutdown()
    await assert.rejects(unable.harbor.transaction(async () => {}), { code: ERROR_CODES.CLOSED })

    const h = transactional()
    await assert.rejects(h.harbor.transaction('not a function' as never), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => harness({ transactionalId: '' }), { code: ERROR_CODES.CONFIG_INVALID })
    await h.harbor.shutdown()
  })

  test('a transaction begun inside a transaction is refused instead of waiting for itself', async () => {
    const h = transactional()
    await assert.rejects(h.harbor.transaction(async () => {
      await h.harbor.transaction(async () => {})
    }), { code: ERROR_CODES.CONFIG_INVALID, message: /inside a transaction/ })
    // The outer one aborted and the runner is free again.
    await h.harbor.transaction(async () => {})
    assert.deepEqual(methods(h), ['transaction', 'transactionAbort', 'transaction', 'transactionCommit'])
    await h.harbor.shutdown()
  })

  test('a per-transaction serializer overrides the harbor\'s', async () => {
    const h = transactional()
    h.adapter.createTopic('orders')
    await h.harbor.transaction(async (tx) => { await tx.send('orders', { value: 'raw text' }) }, { serializer: { serialize: (value) => Buffer.from(`<${String(value)}>`), deserialize: (bytes) => bytes.toString() } })
    assert.equal(h.adapter.messages('orders')[0]?.value?.toString(), '<raw text>')
    await h.harbor.shutdown()
  })
})

describe('ctx.transaction() in a handler', () => {
  test('commits the consumed offset along with what the handler produced', async () => {
    const h = transactional()
    const completed = captureEvents(h.harbor, 'transactionCompleted')
    h.adapter.createTopic('shipments')
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe<{ id: string }>('orders', async (message, ctx) => {
      await ctx.transaction(async (tx) => {
        await tx.send('shipments', { key: message.value.id, value: { shipped: message.value.id } })
      })
    })
    await consumer.start()
    await h.harbor.producer<{ id: string }>().send('orders', { key: 'a', value: { id: 'a' } })
    await until(() => completed.length === 1)
    await until(() => h.adapter.committed('g', 'orders', 0) === '1')
    assert.deepEqual(h.adapter.messages('shipments').map((m) => json(m.value)), [{ shipped: 'a' }])
    assert.deepEqual(h.adapter.calls.filter((call) => call.method === 'transactionOffsets').map((call) => call.args[0]), [[{ topic: 'orders', partition: 0, offset: '1' }]])
    assert.deepEqual(methods(h), ['transaction', 'transactionProduce', 'transactionOffsets', 'transactionCommit'])
    await h.harbor.shutdown()
  })

  test('a batch handler commits the offset after its last message along with what it produced', async () => {
    const h = transactional()
    h.adapter.createTopic('shipments')
    const batches = captureEvents(h.harbor, 'batchProcessed')
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribeBatch<{ id: string }>('orders', async (messages, ctx) => {
      await ctx.transaction(async (tx) => {
        await tx.sendBatch('shipments', messages.map((message) => ({ value: { shipped: message.value.id } })))
      })
    }, { size: 2 })
    await consumer.start()
    await h.harbor.producer<{ id: string }>().sendBatch('orders', [{ value: { id: 'a' } }, { value: { id: 'b' } }])
    await until(() => batches.length === 1)
    assert.deepEqual(h.adapter.calls.filter((call) => call.method === 'transactionOffsets').map((call) => call.args[0]), [[{ topic: 'orders', partition: 0, offset: '2' }]])
    assert.equal(h.adapter.messages('shipments').length, 2)
    await h.harbor.shutdown()
  })

  test('a batch message that does not decode is dead-lettered before the handler runs, so a transaction the handler commits cannot leave it behind', async () => {
    const h = transactional()
    h.adapter.createTopic('shipments')
    const batches = captureEvents(h.harbor, 'batchProcessed')
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribeBatch<{ id: string }>('orders', async (messages, ctx) => {
      await ctx.transaction(async (tx) => {
        await tx.sendBatch('shipments', messages.map((message) => ({ value: { shipped: message.value.id } })))
      })
    }, { size: 3 })
    await consumer.start()
    await h.harbor.connect()
    await h.adapter.produce([
      { topic: 'orders', key: null, value: Buffer.from('{"id":"a"}'), headers: {} },
      { topic: 'orders', key: null, value: Buffer.from('not json'), headers: {} },
      { topic: 'orders', key: null, value: Buffer.from('{"id":"c"}'), headers: {} }
    ])
    await until(() => batches.length === 1)
    const order = h.adapter.calls.map((call) => call.method).filter((method) => method === 'produce' || method.startsWith('transaction'))
    // The batch's own produce (the source records), then the DLQ hop, then the handler's transaction.
    assert.deepEqual(order, ['produce', 'produce', 'transaction', 'transactionProduce', 'transactionOffsets', 'transactionCommit'])
    assert.equal(h.adapter.messages('orders-dlq').length, 1)
    assert.equal(h.adapter.messages('shipments').length, 2)
    assert.equal(h.adapter.committed('g', 'orders', 0), '3')
    await h.harbor.shutdown()
  })

  test('a transaction that fails inside the handler is the handler\'s failure: nothing produced, the message walks the ladder', async () => {
    const h = transactional()
    h.adapter.createTopic('shipments')
    const failed = captureEvents(h.harbor, 'messageFailed')
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '1m' }] } })
    consumer.subscribe<{ id: string }>('orders', async (message, ctx) => {
      await ctx.transaction(async (tx) => {
        await tx.send('shipments', { value: { shipped: message.value.id } })
        throw new Error('inventory refused')
      })
    })
    await consumer.start()
    await h.harbor.producer<{ id: string }>().send('orders', { value: { id: 'a' } })
    await until(() => failed.length === 1)
    assert.equal(failed[0]?.outcome, 'retry')
    assert.equal((failed[0]?.error as Error).message, 'inventory refused')
    assert.equal(h.adapter.messages('shipments').length, 0)
    assert.equal(h.adapter.messages('orders-retry-1').length, 1)
    await h.harbor.shutdown()
  })

  test('the events reach the harbor emitter like every other', async () => {
    const h = transactional()
    const events: Array<keyof HarborEvents> = []
    h.harbor.on('transactionCompleted', () => { events.push('transactionCompleted') })
    await h.harbor.transaction(async () => {})
    assert.deepEqual(events, ['transactionCompleted'])
    await h.harbor.shutdown()
  })
})

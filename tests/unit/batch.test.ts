import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { BatchFailedError, ERROR_CODES, type HarborEvents, type Instrumentation, type Message } from '../../src/index'
import { captureErrors, captureEvents, gate, harness, type Harness } from '../helpers/harness'
import { until } from '../helpers/manual-clock'

interface Order { id: string, total: number }

const events = (h: Harness): { processed: Array<HarborEvents['messageProcessed']>, failed: Array<HarborEvents['messageFailed']>, batches: Array<HarborEvents['batchProcessed']> } => ({
  processed: captureEvents(h.harbor, 'messageProcessed'),
  failed: captureEvents(h.harbor, 'messageFailed'),
  batches: captureEvents(h.harbor, 'batchProcessed')
})

const orders = (...ids: string[]): Array<{ value: Order }> => ids.map((id) => ({ value: { id, total: 1 } }))
const idsOf = (messages: Array<Message<Order>>): string[] => messages.map((message) => message.value.id)

describe('consumer.subscribeBatch()', () => {
  test('a batch runs when it fills, in offset order, and the offset after its last message is committed once', async () => {
    const h = harness()
    const { processed, batches: runs } = events(h)
    const batches: string[][] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribeBatch<Order>('orders', (messages, ctx) => {
      assert.equal(ctx.groupId, 'g')
      assert.equal(ctx.topic, 'orders')
      assert.equal(ctx.partition, 0)
      h.clock.advance(30)
      batches.push(idsOf(messages))
    }, { size: 3 })
    await consumer.start()
    h.adapter.clearCalls()
    await h.harbor.producer<Order>().sendBatch('orders', orders('a', 'b', 'c', 'd', 'e', 'f'))
    await until(() => processed.length === 6)
    assert.deepEqual(batches, [['a', 'b', 'c'], ['d', 'e', 'f']])
    assert.deepEqual(runs, [
      { topic: 'orders', partition: 0, groupId: 'g', size: 3, durationMs: 30, outcome: 'processed' },
      { topic: 'orders', partition: 0, groupId: 'g', size: 3, durationMs: 30, outcome: 'processed' }
    ])
    assert.equal(h.adapter.committed('g', 'orders', 0), '6')
    assert.equal(h.adapter.calls.filter((call) => call.method === 'commit').length, 2, 'one commit per batch')
    assert.deepEqual(processed.map((event) => [event.offset, event.durationMs, event.replayed, event.batch]), [['0', 30, false, 3], ['1', 30, false, 3], ['2', 30, false, 3], ['3', 30, false, 3], ['4', 30, false, 3], ['5', 30, false, 3]])
    assert.deepEqual(h.logs.filter((log) => log.level !== 'info'), [])
    await h.harbor.shutdown()
  })

  test('a partial batch runs once maxWait passed since its first message', async () => {
    const h = harness()
    const { processed } = events(h)
    const batches: string[][] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribeBatch<Order>('orders', (messages) => { batches.push(idsOf(messages)) }, { size: 10, maxWait: '2s' })
    await consumer.start()
    await h.harbor.producer<Order>().sendBatch('orders', orders('a', 'b'))
    await until(() => h.clock.waiting === 1)
    assert.equal(batches.length, 0, 'nothing runs before the wait')
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    h.clock.advance(1_999)
    await until(() => h.clock.waiting === 1)
    assert.equal(batches.length, 0)
    h.clock.advance(1)
    await until(() => processed.length === 2)
    assert.deepEqual(batches, [['a', 'b']])
    assert.equal(h.adapter.committed('g', 'orders', 0), '2')
    await h.harbor.shutdown()
  })

  test('one batch runs per partition at a time: a delivery arriving meanwhile waits for it, and offsets never commit out of order', async () => {
    const h = harness()
    const { processed } = events(h)
    const held = gate()
    const batches: string[][] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribeBatch<Order>('orders', async (messages) => {
      batches.push(idsOf(messages))
      if (batches.length === 1) await held.wait
    }, { size: 10, maxWait: 1_000 })
    await consumer.start()
    await h.harbor.producer<Order>().sendBatch('orders', orders('a', 'b'))
    await until(() => h.clock.waiting === 1)
    h.clock.advance(1_000)
    await until(() => batches.length === 1)
    // The first batch is running: the next delivery waits for it instead
    // of starting a second collection, so no timer is armed meanwhile.
    await h.harbor.producer<Order>().sendBatch('orders', orders('c', 'd'))
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(h.clock.waiting, 0, 'nothing collects while a batch runs')
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    held.release()
    await until(() => processed.length === 2)
    assert.equal(h.adapter.committed('g', 'orders', 0), '2')
    await until(() => h.clock.waiting === 1, 2_000)
    h.clock.advance(1_000)
    await until(() => processed.length === 4)
    assert.deepEqual(batches, [['a', 'b'], ['c', 'd']])
    assert.equal(h.adapter.committed('g', 'orders', 0), '4')
    await h.harbor.shutdown()
  })

  test('a batch formed while another one aborts or crashes is left uncommitted, so nothing is committed past the lost messages', async () => {
    for (const failure of ['abort', 'crash'] as const) {
      const h = harness()
      const held = gate()
      const batches: string[][] = []
      const stopped = captureEvents(h.harbor, 'consumerStopped')
      const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, dlq: { enabled: false } })
      consumer.subscribeBatch<Order>('orders', async (messages) => {
        batches.push(idsOf(messages))
        await held.wait
        throw failure === 'abort' ? h.harbor.abort(new Error('stop')) : new Error('no way out')
      }, { size: 2, maxWait: 1_000 })
      await consumer.start()
      await h.harbor.producer<Order>().send('orders', { value: { id: 'a', total: 1 } })
      await until(() => h.clock.waiting === 1)
      h.clock.advance(1_000)
      await until(() => batches.length === 1)
      await h.harbor.producer<Order>().sendBatch('orders', orders('b', 'c'))
      await new Promise((resolve) => setImmediate(resolve))
      held.release()
      await until(() => stopped.length === 1)
      assert.equal(stopped[0]?.reason, failure)
      assert.deepEqual(batches, [['a']], `${failure}: the second batch never ran`)
      assert.equal(h.adapter.committed('g', 'orders', 0), undefined, `${failure}: nothing committed`)
      await h.harbor.shutdown()
    }
  })

  test('a batch that throws sends every message down the ladder with that error, then to the DLQ, one commit per hop', async () => {
    const h = harness()
    const { failed, batches: runs } = events(h)
    const attempts: number[][] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '1m' }] } })
    consumer.subscribeBatch<Order>('orders', (messages) => {
      attempts.push(messages.map((message) => (message.retry?.count ?? 0) + 1))
      throw new Error('batch bug')
    }, { size: 2 })
    await consumer.start()
    await h.harbor.producer<Order>().sendBatch('orders', orders('a', 'b'))
    await until(() => failed.length === 2)
    assert.deepEqual(failed.map((event) => [event.offset, event.outcome, (event.error as Error).message]), [['0', 'retry', 'batch bug'], ['1', 'retry', 'batch bug']])
    assert.equal(h.adapter.committed('g', 'orders', 0), '2')
    assert.equal(h.adapter.messages('orders-retry-1').length, 2)
    // On the retry topic a message waits its delay before joining a batch;
    // the partition delivers the next one after that, already due.
    await until(() => h.clock.waiting === 1)
    h.clock.advance(60_000)
    await until(() => failed.length === 4)
    assert.deepEqual(attempts, [[1, 1], [2, 2]])
    assert.deepEqual(failed.slice(2).map((event) => [event.topic, event.outcome]), [['orders-retry-1', 'dead-letter'], ['orders-retry-1', 'dead-letter']])
    assert.equal(h.adapter.messages('orders-dlq').length, 2)
    assert.equal(h.adapter.committed('g', 'orders-retry-1', 0), '2')
    assert.deepEqual(h.adapter.messages('orders-dlq').map((m) => m.headers['x-original-topic']), ['orders', 'orders'])
    assert.deepEqual(runs.map((run) => [run.topic, run.outcome, run.size]), [['orders', 'retry', 2], ['orders-retry-1', 'dead-letter', 2]])
    assert.deepEqual(failed.map((event) => event.batch), [2, 2, 2, 2])
    await h.harbor.shutdown()
  })

  test('a message that does not deserialize is dead-lettered on its own and the rest of the batch runs', async () => {
    const h = harness()
    const { processed, failed, batches: runs } = events(h)
    const batches: string[][] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribeBatch<Order>('orders', (messages) => { batches.push(idsOf(messages)) }, { size: 3 })
    await consumer.start()
    await h.harbor.connect()
    await h.adapter.produce([
      { topic: 'orders', key: null, value: Buffer.from('{"id":"a","total":1}'), headers: {} },
      { topic: 'orders', key: null, value: Buffer.from('not json'), headers: {} },
      { topic: 'orders', key: null, value: Buffer.from('{"id":"c","total":1}'), headers: {} }
    ])
    await until(() => processed.length === 2 && failed.length === 1)
    assert.deepEqual(batches, [['a', 'c']])
    assert.deepEqual(runs.map((run) => run.outcome), ['processed'], 'the handler resolved; the undecodable message failed on its own')
    assert.deepEqual([failed[0]?.offset, failed[0]?.outcome], ['1', 'dead-letter'])
    assert.equal((failed[0]?.error as { code: string }).code, ERROR_CODES.SERIALIZATION)
    assert.equal(h.adapter.messages('orders-dlq').length, 1)
    assert.equal(h.adapter.committed('g', 'orders', 0), '3')
    await h.harbor.shutdown()
  })

  test('harbor.abort() from a batch stops the consumer with nothing committed', async () => {
    const h = harness()
    const { failed, batches: runs } = events(h)
    const errors = captureErrors(h.harbor)
    const stopped: string[] = []
    h.harbor.on('consumerStopped', (event) => { stopped.push(event.reason) })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribeBatch<Order>('orders', () => { throw h.harbor.abort(new Error('stop everything')) }, { size: 2 })
    await consumer.start()
    await h.harbor.producer<Order>().sendBatch('orders', orders('a', 'b'))
    await until(() => stopped.length === 1)
    assert.deepEqual(stopped, ['abort'])
    assert.deepEqual(failed.map((event) => event.outcome), ['abort', 'abort'])
    assert.deepEqual(runs.map((run) => [run.topic, run.partition, run.groupId, run.size, run.outcome]), [['orders', 0, 'g', 2, 'abort']])
    assert.equal(errors.length, 1)
    assert.deepEqual([errors[0]?.scope, errors[0]?.groupId, errors[0]?.topic, (errors[0]?.error as { code: string }).code], ['consumer', 'g', 'orders', ERROR_CODES.ABORT_PROCESSING])
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    await h.harbor.shutdown()
  })

  test('without a retry level and without a DLQ, a failing batch crashes the consumer, uncommitted', async () => {
    const h = harness()
    const { failed, batches: runs } = events(h)
    const stopped: string[] = []
    h.harbor.on('consumerStopped', (event) => { stopped.push(event.reason) })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, dlq: { enabled: false } })
    consumer.subscribeBatch<Order>('orders', () => { throw new Error('no way out') }, { size: 2 })
    await consumer.start()
    await h.harbor.producer<Order>().sendBatch('orders', orders('a', 'b'))
    await until(() => stopped.length === 1)
    assert.deepEqual(stopped, ['crash'])
    assert.deepEqual(failed.map((event) => event.outcome), ['crash'])
    assert.deepEqual(runs.map((run) => [run.topic, run.partition, run.groupId, run.size, run.outcome]), [['orders', 0, 'g', 2, 'crash']])
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    await h.harbor.shutdown()
  })

  test('shutdown discards a batch still collecting: nothing committed, everything redelivered to the next member', async () => {
    const h = harness()
    const batches: string[][] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribeBatch<Order>('orders', (messages) => { batches.push(idsOf(messages)) }, { size: 10, maxWait: '1m' })
    await consumer.start()
    await h.harbor.producer<Order>().sendBatch('orders', orders('a', 'b'))
    await until(() => h.clock.waiting === 1)
    await consumer.stop()
    assert.equal(batches.length, 0, 'the collecting batch never ran')
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    assert.equal(h.clock.waiting, 0, 'the maxWait timer is gone')

    const { processed } = events(h)
    const next = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    next.subscribeBatch<Order>('orders', (messages) => { batches.push(idsOf(messages)) }, { size: 2 })
    await next.start()
    await until(() => processed.length === 2)
    assert.deepEqual(batches, [['a', 'b']])
    await h.harbor.shutdown()
  })

  test('shutdown waits for a running batch, which commits on the way out', async () => {
    const h = harness()
    const held = gate()
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribeBatch<Order>('orders', async () => { await held.wait }, { size: 10, maxWait: 1_000 })
    await consumer.start()
    await h.harbor.producer<Order>().sendBatch('orders', orders('a', 'b'))
    await until(() => h.clock.waiting === 1)
    h.clock.advance(1_000)
    await until(() => h.clock.waiting === 0)
    let done = false
    const stopping = h.harbor.shutdown().then(() => { done = true })
    await until(() => h.clock.waiting === 1, 500).catch(() => {})
    assert.equal(done, false, 'the batch is running, shutdown waits')
    held.release()
    await stopping
    assert.equal(h.adapter.committed('g', 'orders', 0), '2')
  })

  test('each batch subscription may have its own serializer, and the level delay applies per message before it joins a batch', async () => {
    const h = harness()
    const { processed } = events(h)
    const seen: unknown[][] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribeBatch<string>('logs', (messages) => { seen.push(messages.map((message) => message.value)) }, {
      size: 2,
      serializer: { serialize: (value) => Buffer.from(value), deserialize: (bytes) => bytes.toString('utf8').toUpperCase() }
    })
    await consumer.start()
    await h.harbor.producer<string>({ serializer: { serialize: (value) => Buffer.from(value), deserialize: (bytes) => bytes.toString() } }).sendBatch('logs', [{ value: 'a' }, { value: 'b' }])
    await until(() => processed.length === 2)
    assert.deepEqual(seen, [['A', 'B']])
    await h.harbor.shutdown()
  })

  test('the batch handler runs inside the instrumentation, and a hook that fails changes nothing', async () => {
    const h = harness({
      instrumentation: {
        async wrapBatchHandler (messages, ctx, run) {
          calls.push(`wrap ${ctx.topic}[${ctx.partition}] x${messages.length}`)
          return await run()
        }
      } satisfies Instrumentation
    })
    const calls: string[] = []
    const { processed } = events(h)
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribeBatch<Order>('orders', () => { calls.push('handler') }, { size: 2 })
    await consumer.start()
    await h.harbor.producer<Order>().sendBatch('orders', orders('a', 'b'))
    await until(() => processed.length === 2)
    assert.deepEqual(calls, ['wrap orders[0] x2', 'handler'])
    await h.harbor.shutdown()

    const broken = harness({ instrumentation: { wrapBatchHandler: async () => { throw new Error('hook down') } } })
    const done = events(broken)
    let ran = 0
    const plain = broken.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    plain.subscribeBatch<Order>('orders', () => { ran++ }, { size: 2 })
    await plain.start()
    await broken.harbor.producer<Order>().sendBatch('orders', orders('a', 'b'))
    await until(() => done.processed.length === 2)
    assert.equal(ran, 1)
    assert.ok(broken.logs.some((log) => log.message.includes('hook down')))
    await broken.harbor.shutdown()
  })

  test('BatchFailedError fails only the messages it names; the rest of the batch is committed as processed', async () => {
    const h = harness()
    const { processed, failed, batches: runs } = events(h)
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    consumer.subscribeBatch<Order>('orders', (messages) => {
      const bad = messages.filter((message) => message.value.id === 'b')
      if (bad.length > 0) throw new BatchFailedError(bad, Object.assign(new Error('bad sku'), { retryable: false }))
    }, { size: 3 })
    await consumer.start()
    await h.harbor.producer<Order>().sendBatch('orders', orders('a', 'b', 'c'))
    await until(() => processed.length === 2 && failed.length === 1)
    assert.deepEqual(processed.map((event) => event.offset), ['0', '2'])
    assert.deepEqual([failed[0]?.offset, failed[0]?.outcome, (failed[0]?.error as Error).message], ['1', 'dead-letter', 'bad sku'])
    assert.equal(h.adapter.messages('orders-retry-1').length, 0, 'the cause decides: not retryable, so straight to the DLQ')
    assert.equal(h.adapter.messages('orders-dlq').length, 1)
    assert.equal(h.adapter.committed('g', 'orders', 0), '3')
    assert.deepEqual(runs.map((run) => run.outcome), ['processed'])
    await h.harbor.shutdown()
  })

  test('a BatchFailedError without a cause fails its messages with itself, and the batch still counts as processed', async () => {
    const h = harness()
    const { processed, failed, batches: runs } = events(h)
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribeBatch<Order>('orders', (messages) => {
      throw new BatchFailedError(messages.filter((message) => message.value.id === 'b'), undefined)
    }, { size: 2 })
    await consumer.start()
    await h.harbor.producer<Order>().sendBatch('orders', orders('a', 'b'))
    await until(() => processed.length === 1 && failed.length === 1)
    assert.equal(failed[0]?.outcome, 'dead-letter')
    assert.equal((failed[0]?.error as { code: string }).code, ERROR_CODES.BATCH_FAILED)
    assert.deepEqual(runs.map((run) => run.outcome), ['processed'])
    await h.harbor.shutdown()
  })

  test('a BatchFailedError naming a message of another topic or partition is a bug as well', async () => {
    for (const [elsewhere, named] of [[{ topic: 'payments' }, 'payments[0]@0'], [{ partition: 7 }, 'orders[7]@0']] as const) {
      const h = harness()
      const stopped = captureEvents(h.harbor, 'consumerStopped')
      const errors = captureErrors(h.harbor)
      const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
      consumer.subscribeBatch<Order>('orders', (messages) => {
        throw new BatchFailedError([{ ...(messages[0] as Message<Order>), ...elsewhere }], new Error('elsewhere'))
      }, { size: 2 })
      await consumer.start()
      await h.harbor.producer<Order>().sendBatch('orders', orders('a', 'b'))
      await until(() => stopped.length === 1)
      assert.equal(stopped[0]?.reason, 'crash')
      assert.ok((errors[0]?.error as Error).message.includes(`names ${named}, which is not in the batch of orders[0] 0..1`), (errors[0]?.error as Error).message)
      assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
      await h.harbor.shutdown()
    }
  })

  test('a BatchFailedError naming a message outside the batch is a bug: the consumer stops, uncommitted', async () => {
    const h = harness()
    const stopped = captureEvents(h.harbor, 'consumerStopped')
    const errors = captureErrors(h.harbor)
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribeBatch<Order>('orders', (messages) => {
      throw new BatchFailedError([{ ...(messages[0] as Message<Order>), offset: '99' }], new Error('elsewhere'))
    }, { size: 2 })
    await consumer.start()
    await h.harbor.producer<Order>().sendBatch('orders', orders('a', 'b'))
    await until(() => stopped.length === 1)
    assert.equal(stopped[0]?.reason, 'crash')
    assert.match((errors[0]?.error as Error).message, /names orders\[0\]@99, which is not in the batch of orders\[0\] 0\.\.1/)
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    await h.harbor.shutdown()
  })

  test('a batch subscription cannot share a retry topic with another subscription', () => {
    const h = harness()
    const shared = { levels: [{ delay: 0 }], topicNaming: (_topic: string, level: number) => `svc-retry-${level}` }
    const batchFirst = h.harbor.consumer({ groupId: 'a', retry: shared })
    batchFirst.subscribeBatch('orders', () => {})
    assert.throws(() => batchFirst.subscribe('invoices', () => {}), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
      assert.match((error as Error).message, /retry topic "svc-retry-1" would be shared between "invoices" and "orders", and a batch subscription cannot share/)
      return true
    })
    const eachFirst = h.harbor.consumer({ groupId: 'b', retry: shared })
    eachFirst.subscribe('orders', () => {})
    assert.throws(() => eachFirst.subscribeBatch('invoices', () => {}), /a batch subscription cannot share a retry topic/)
    const plain = h.harbor.consumer({ groupId: 'c', retry: { levels: [{ delay: 0 }] } })
    plain.subscribeBatch('orders', () => {}).subscribeBatch('invoices', () => {})
  })

  test('a revocation runs the batch still collecting on the partition and commits it before releasing; a delivery that waited through it is left to the next owner', async () => {
    const h = harness()
    const { processed } = events(h)
    const batches: string[][] = []
    const held = gate()
    let revoke!: (partitions: Array<{ topic: string, partition: number }>) => Promise<void>
    const original = h.adapter.consume
    h.adapter.consume = async (options) => {
      revoke = options.onPartitionsRevoked as typeof revoke
      return await original(options)
    }
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribeBatch<Order>('orders', async (messages) => {
      batches.push(idsOf(messages))
      if (batches.length === 1) await held.wait
    }, { size: 10, maxWait: '1m' })
    await consumer.start()
    // Collecting only: the revocation runs the batch now, and waits for it.
    await h.harbor.producer<Order>().sendBatch('orders', orders('a', 'b'))
    await until(() => h.clock.waiting === 1)
    let revoked = false
    const revoking = revoke([{ topic: 'orders', partition: 0 }]).then(() => { revoked = true })
    await until(() => batches.length === 1)
    assert.equal(revoked, false, 'the revocation waits for the flushed batch')
    // A delivery arriving while that batch runs waits for it; once the
    // partition was revoked meanwhile, it must not start a batch here.
    await h.harbor.producer<Order>().send('orders', { value: { id: 'c', total: 1 } })
    await new Promise((resolve) => setImmediate(resolve))
    held.release()
    await revoking
    await until(() => processed.length === 2)
    assert.equal(h.adapter.committed('g', 'orders', 0), '2', 'the flushed batch committed before the release')
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(h.clock.waiting, 0, 'no batch collects on the revoked partition')
    assert.deepEqual(batches, [['a', 'b']])
    await h.harbor.shutdown()
  })

  test('the batch outcome follows the handler\'s failure, not a message that failed to decode', async () => {
    const h = harness()
    const { failed, batches: runs } = events(h)
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '1m' }] } })
    consumer.subscribeBatch<Order>('orders', () => { throw new Error('handler bug') }, { size: 3 })
    await consumer.start()
    await h.harbor.connect()
    await h.adapter.produce([
      { topic: 'orders', key: null, value: Buffer.from('{"id":"a","total":1}'), headers: {} },
      { topic: 'orders', key: null, value: Buffer.from('{"id":"b","total":1}'), headers: {} },
      { topic: 'orders', key: null, value: Buffer.from('not json'), headers: {} }
    ])
    await until(() => failed.length === 3)
    assert.deepEqual(failed.map((event) => [event.offset, event.outcome]), [['0', 'retry'], ['1', 'retry'], ['2', 'dead-letter']])
    assert.deepEqual(runs.map((run) => run.outcome), ['retry'])
    await h.harbor.shutdown()
  })

  test('a batch in which nothing decodes dead-letters its messages without a handler run to report', async () => {
    const h = harness()
    const { failed, batches: runs } = events(h)
    let ran = 0
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribeBatch<Order>('orders', () => { ran++ }, { size: 2 })
    await consumer.start()
    await h.harbor.connect()
    await h.adapter.produce([
      { topic: 'orders', key: null, value: Buffer.from('not json'), headers: {} },
      { topic: 'orders', key: null, value: Buffer.from('nor this'), headers: {} }
    ])
    await until(() => failed.length === 2)
    assert.equal(ran, 0)
    assert.equal(runs.length, 0)
    assert.equal(h.adapter.messages('orders-dlq').length, 2)
    assert.equal(h.adapter.committed('g', 'orders', 0), '2')
    await h.harbor.shutdown()
  })

  test('validates its options and refuses a consumer configured with idempotency', () => {
    const h = harness()
    const consumer = h.harbor.consumer({ groupId: 'g' })
    assert.throws(() => consumer.subscribeBatch('orders', () => {}, { size: 0 }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => consumer.subscribeBatch('orders', () => {}, { maxWait: '-1s' }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => consumer.subscribeBatch('orders', 'nope' as never), /handler for "orders" must be a function/)
    assert.throws(() => consumer.subscribeBatch('', () => {}), { code: ERROR_CODES.CONFIG_INVALID })
    consumer.subscribeBatch('orders', () => {})
    assert.throws(() => consumer.subscribe('orders', () => {}), /claimed by both/)
    const engine = { async executeWithMetadata<T> (_input: unknown, run: () => Promise<T>) { return { value: await run(), replayed: false } } }
    const deduplicated = h.harbor.consumer({ groupId: 'd', idempotency: { engine } })
    assert.throws(() => deduplicated.subscribeBatch('orders', () => {}), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
      assert.match((error as Error).message, /idempotency applies to single-message handlers/)
      return true
    })
  })

  test('subscribeBatch() after start() is refused', async () => {
    const h = harness()
    const consumer = h.harbor.consumer({ groupId: 'g', autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    await consumer.start()
    assert.throws(() => consumer.subscribeBatch('payments', () => {}), /subscribeBatch\(\) must be called before start\(\)/)
    await h.harbor.shutdown()
  })
})

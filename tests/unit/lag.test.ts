import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { ERROR_CODES } from '../../src/index'
import { captureErrors, gate, harness } from '../helpers/harness'
import { until } from '../helpers/manual-clock'

describe('consumer.lag() and harbor.lag()', () => {
  test('reports high watermark, committed offset and lag per consumed partition, retry topics included', async () => {
    const h = harness({}, { partitions: 2 })
    const held = gate()
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 60_000 }] } })
    let handled = 0
    consumer.subscribe<number>('orders', async (message) => {
      await held.wait
      handled++
      if (message.value === 3) throw new Error('to the retry topic')
    })
    await consumer.start()
    // Three records on partition 0 and one on partition 1, none committed while the handlers wait.
    await h.harbor.producer<number>().sendBatch('orders', [
      { value: 1, partition: 0 }, { value: 2, partition: 0 }, { value: 3, partition: 0 }, { value: 4, partition: 1 }
    ])
    const before = await consumer.lag()
    assert.deepEqual(before.map(({ topic, partition, low, high, committed, lag }) => ({ topic, partition, low, high, committed, lag })), [
      { topic: 'orders', partition: 0, low: '0', high: '3', committed: null, lag: 3 },
      { topic: 'orders', partition: 1, low: '0', high: '1', committed: null, lag: 1 },
      { topic: 'orders-retry-1', partition: 0, low: '0', high: '0', committed: null, lag: 0 }
    ])

    held.release()
    await until(() => handled === 4)
    await until(() => h.clock.waiting === 1)
    const after = await consumer.lag()
    assert.deepEqual(after.map(({ groupId, topic, partition, high, committed, lag }) => ({ groupId, topic, partition, high, committed, lag })), [
      { groupId: 'g', topic: 'orders', partition: 0, high: '3', committed: '3', lag: 0 },
      { groupId: 'g', topic: 'orders', partition: 1, high: '1', committed: '1', lag: 0 },
      // The retried record sits on the retry topic, uncommitted while it waits out its delay.
      { groupId: 'g', topic: 'orders-retry-1', partition: 0, high: '1', committed: null, lag: 1 }
    ])
    await h.harbor.shutdown(0)
  })

  test('without a commit, a group that starts from the latest offset has no backlog', async () => {
    const h = harness()
    const consumer = h.harbor.consumer({ groupId: 'g', autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    await h.harbor.connect()
    await h.harbor.producer().sendBatch('orders', [{ value: 1 }, { value: 2 }])
    assert.deepEqual((await consumer.lag()).map(({ committed, lag }) => ({ committed, lag })), [{ committed: null, lag: 0 }])
    await h.harbor.shutdown()
  })

  test('harbor.lag() covers the running consumers only', async () => {
    const h = harness()
    const running = h.harbor.consumer({ groupId: 'running', fromBeginning: true, autoCreateTopics: true })
    running.subscribe('orders', () => {})
    const idle = h.harbor.consumer({ groupId: 'idle', fromBeginning: true, autoCreateTopics: true })
    idle.subscribe('payments', () => {})
    await h.harbor.connect()
    h.adapter.createTopic('payments')
    await h.harbor.producer().send('orders', { value: 1 })
    assert.deepEqual(await h.harbor.lag(), [])
    await running.start()
    await h.adapter.whenDrained('running', 'orders')
    assert.deepEqual((await h.harbor.lag()).map(({ groupId, topic, lag }) => ({ groupId, topic, lag })), [{ groupId: 'running', topic: 'orders', lag: 0 }])
    await h.harbor.shutdown()
  })

  test('a watermark or committed offset that is not a number is refused, never a negative lag', async () => {
    const h = harness()
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    await h.harbor.connect()
    h.adapter.createTopic('orders')
    h.adapter.admin.fetchTopicOffsets = async (topics) => topics.map((topic) => ({ topic, partition: 0, low: '0', high: '-1' }))
    await assert.rejects(consumer.lag(), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.ADAPTER)
      assert.equal((error as { retryable: boolean }).retryable, false)
      assert.match((error as Error).message, /invalid high watermark for orders\[0\]: "-1"/)
      return true
    })
    h.adapter.admin.fetchTopicOffsets = async (topics) => topics.map((topic) => ({ topic, partition: 0, low: '1x', high: '5' }))
    await assert.rejects(consumer.lag(), /invalid low watermark for orders\[0\]: "1x"/)
    h.adapter.admin.fetchTopicOffsets = async (topics) => topics.map((topic) => ({ topic, partition: 0, low: '0', high: '5' }))
    h.adapter.admin.fetchCommittedOffsets = async (_groupId, topics) => topics.map((topic) => ({ topic, partition: 0, offset: '-1001' }))
    await assert.rejects(consumer.lag(), /invalid committed offset for orders\[0\]: "-1001"/)
    // Offsets are decimal strings of any length; a large backlog reads back exactly.
    h.adapter.admin.fetchTopicOffsets = async (topics) => topics.map((topic) => ({ topic, partition: 0, low: '10', high: '9007199254741012' }))
    h.adapter.admin.fetchCommittedOffsets = async (_groupId, topics) => topics.map((topic) => ({ topic, partition: 0, offset: '9007199254741000' }))
    assert.deepEqual((await consumer.lag()).map(({ low, high, committed, lag }) => ({ low, high, committed, lag })), [{ low: '10', high: '9007199254741012', committed: '9007199254741000', lag: 12 }])
  })

  test('a committed offset outside the partition\'s range counts as no position, the way the broker would reset it', async () => {
    const h = harness()
    const fromBeginning = h.harbor.consumer({ groupId: 'early', fromBeginning: true, autoCreateTopics: true })
    fromBeginning.subscribe('orders', () => {})
    const fromLatest = h.harbor.consumer({ groupId: 'late', autoCreateTopics: true })
    fromLatest.subscribe('orders', () => {})
    await h.harbor.connect()
    h.adapter.createTopic('orders')
    h.adapter.admin.fetchTopicOffsets = async (topics) => topics.map((topic) => ({ topic, partition: 0, low: '3', high: '8' }))
    const committedAt = (offset: string): void => {
      h.adapter.admin.fetchCommittedOffsets = async (_groupId, topics) => topics.map((topic) => ({ topic, partition: 0, offset }))
    }
    const lagOf = async (): Promise<Array<{ committed: string | null, lag: number }>> =>
      (await Promise.all([fromBeginning.lag(), fromLatest.lag()])).map(([entry]) => ({ committed: entry?.committed ?? null, lag: entry?.lag ?? -1 }))

    committedAt('5')
    assert.deepEqual(await lagOf(), [{ committed: '5', lag: 3 }, { committed: '5', lag: 3 }], 'in range: the committed offset is the position')
    committedAt('1')
    assert.deepEqual(await lagOf(), [{ committed: '1', lag: 5 }, { committed: '1', lag: 0 }], 'below the low watermark: the records expired')
    committedAt('9')
    assert.deepEqual(await lagOf(), [{ committed: '9', lag: 5 }, { committed: '9', lag: 0 }], 'past the high watermark: the topic was recreated')
    committedAt('8')
    assert.deepEqual(await lagOf(), [{ committed: '8', lag: 0 }, { committed: '8', lag: 0 }], 'at the high watermark: caught up')
    committedAt('3')
    assert.deepEqual(await lagOf(), [{ committed: '3', lag: 5 }, { committed: '3', lag: 5 }], 'at the low watermark: still in range')
    await h.harbor.shutdown()
  })

  test('harbor.lag() reports a consumer whose offsets failed through the error event, adapter scope, and returns the others', async () => {
    const h = harness()
    const errors = captureErrors(h.harbor)
    const orders = h.harbor.consumer({ groupId: 'orders-group', fromBeginning: true, autoCreateTopics: true })
    orders.subscribe('orders', () => {})
    const payments = h.harbor.consumer({ groupId: 'payments-group', fromBeginning: true, autoCreateTopics: true })
    payments.subscribe('payments', () => {})
    h.adapter.createTopic('orders')
    h.adapter.createTopic('payments')
    await orders.start()
    await payments.start()
    const original = h.adapter.admin.fetchTopicOffsets as (topics: readonly string[]) => Promise<never[]>
    h.adapter.admin.fetchTopicOffsets = async (topics) => {
      if (topics.includes('payments')) throw new Error('broker away')
      return await original.call(h.adapter.admin, topics)
    }
    assert.deepEqual((await h.harbor.lag()).map(({ groupId, topic }) => ({ groupId, topic })), [{ groupId: 'orders-group', topic: 'orders' }])
    assert.equal(errors.length, 1)
    assert.equal(errors[0]?.scope, 'adapter')
    assert.equal(errors[0]?.groupId, 'payments-group')
    assert.equal((errors[0]?.error as Error).message, 'broker away')

    h.adapter.admin.fetchTopicOffsets = async () => { throw new Error('broker gone') }
    await assert.rejects(h.harbor.lag(), /broker gone/, 'when no consumer answered, the failure is thrown')
    assert.equal(errors.length, 3, 'and each consumer was reported')
    await h.harbor.shutdown()
  })

  test('concurrent harbor.lag() calls share one round of admin calls', async () => {
    const h = harness()
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    h.adapter.createTopic('orders')
    await consumer.start()
    h.adapter.clearCalls()
    const [first, second] = await Promise.all([h.harbor.lag(), h.harbor.lag()])
    assert.equal(first, second, 'the same result object')
    assert.equal(h.adapter.calls.filter((call) => call.method === 'fetchTopicOffsets').length, 1)
    await h.harbor.lag()
    assert.equal(h.adapter.calls.filter((call) => call.method === 'fetchTopicOffsets').length, 2, 'a later call is a new round')
    await h.harbor.shutdown()
  })

  test('lag() after shutdown is a ClosedError', async () => {
    const h = harness()
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    await consumer.start()
    await h.harbor.shutdown()
    await assert.rejects(consumer.lag(), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.CLOSED)
      return true
    })
  })

  test('an adapter without offsets makes lag() a ConfigError naming the capability', async () => {
    const h = harness()
    delete (h.adapter.admin as { fetchTopicOffsets?: unknown }).fetchTopicOffsets
    const consumer = h.harbor.consumer({ groupId: 'g', autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    await assert.rejects(consumer.lag(), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
      assert.match((error as Error).message, /adapter "memory" does not report offsets/)
      return true
    })
  })
})

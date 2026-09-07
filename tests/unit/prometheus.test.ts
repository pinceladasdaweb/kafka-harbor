import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { Registry, register as globalRegistry } from 'prom-client'

import { ERROR_CODES } from '../../src/index'
import { prometheusMetrics } from '../../src/prometheus/index'
import { captureErrors, gate, harness } from '../helpers/harness'
import { until } from '../helpers/manual-clock'

/** The value of one series, or 0 when it was never touched. */
const value = async (registry: Registry, name: string, labels: Record<string, string> = {}): Promise<number> => {
  const metrics = await registry.getMetricsAsJSON()
  // Histogram series (_count, _sum, _bucket) live inside the parent metric with their own metricName.
  const metric = metrics.find((entry) => name === entry.name || name.startsWith(`${entry.name}_`))
  if (metric === undefined) return 0
  const values = metric.values as Array<{ value: number, metricName?: string, labels: Record<string, string | number> }>
  return values.find((candidate) =>
    (candidate.metricName ?? metric.name) === name &&
    Object.entries(labels).every(([label, expected]) => String(candidate.labels[label]) === expected)
  )?.value ?? 0
}

describe('kafka-harbor/prometheus', () => {
  test('counts every outcome of the pipeline with low-cardinality labels', async () => {
    const h = harness()
    const registry = new Registry()
    const metrics = prometheusMetrics(h.harbor, { registry })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    consumer.subscribe<string>('orders', (message) => {
      if (message.value === 'fails') throw new Error('always')
    })
    await consumer.start()
    await h.harbor.producer<string>().sendBatch('orders', [{ value: 'ok' }, { value: 'fails' }])
    await until(() => h.adapter.messages('orders-dlq').length === 1)
    await h.adapter.whenDrained('g', 'orders-retry-1')

    assert.equal(await value(registry, 'kafka_harbor_messages_produced_total', { topic: 'orders', kind: 'send' }), 2)
    assert.equal(await value(registry, 'kafka_harbor_produce_duration_seconds_count', { topic: 'orders', kind: 'send' }), 1)
    // The hops are produce calls too, counted by their destination and kind.
    assert.equal(await value(registry, 'kafka_harbor_messages_produced_total', { topic: 'orders-retry-1', kind: 'retry' }), 1)
    assert.equal(await value(registry, 'kafka_harbor_messages_produced_total', { topic: 'orders-dlq', kind: 'dead-letter' }), 1)
    assert.equal(await value(registry, 'kafka_harbor_produce_duration_seconds_count', { topic: 'orders-dlq', kind: 'dead-letter' }), 1)
    assert.equal(await value(registry, 'kafka_harbor_messages_processed_total', { group: 'g', topic: 'orders' }), 1)
    assert.equal(await value(registry, 'kafka_harbor_messages_failed_total', { group: 'g', topic: 'orders', outcome: 'retry' }), 1)
    assert.equal(await value(registry, 'kafka_harbor_messages_failed_total', { group: 'g', topic: 'orders-retry-1', outcome: 'dead-letter' }), 1)
    assert.equal(await value(registry, 'kafka_harbor_messages_retried_total', { group: 'g', topic: 'orders', level: '1' }), 1)
    assert.equal(await value(registry, 'kafka_harbor_messages_dead_lettered_total', { group: 'g', topic: 'orders-retry-1' }), 1)
    assert.equal(await value(registry, 'kafka_harbor_message_processing_duration_seconds_count', { group: 'g', topic: 'orders', outcome: 'processed' }), 1)
    assert.equal(await value(registry, 'kafka_harbor_message_processing_duration_seconds_count', { group: 'g', topic: 'orders', outcome: 'retry' }), 1)

    const result = await h.harbor.redrive({ from: 'orders-dlq', idleTimeout: 0 })
    assert.equal(result.reprocessed, 1)
    assert.equal(await value(registry, 'kafka_harbor_messages_redriven_total', { from: 'orders-dlq', to: 'orders' }), 1)
    assert.equal(await value(registry, 'kafka_harbor_messages_produced_total', { topic: 'orders', kind: 'redrive' }), 1)

    await h.harbor.shutdown()
    assert.equal(await value(registry, 'kafka_harbor_consumer_stops_total', { group: 'g', reason: 'shutdown' }), 1)
    metrics.detach()
  })

  test('without a registry the metrics land in prom-client\'s global one', async () => {
    const h = harness()
    const metrics = prometheusMetrics(h.harbor, { lag: false })
    try {
      assert.equal(metrics.registry, globalRegistry)
      await h.harbor.producer().send('orders', { value: 1 })
      assert.equal(await value(globalRegistry, 'kafka_harbor_messages_produced_total', { topic: 'orders', kind: 'send' }), 1)
    } finally {
      metrics.detach()
      globalRegistry.clear()
      await h.harbor.shutdown()
    }
  })

  test('errors are counted by scope, producer failures included', async () => {
    const h = harness()
    const registry = new Registry()
    prometheusMetrics(h.harbor, { registry, lag: false })
    h.adapter.failNextProduce(Object.assign(new Error('refused'), { retryable: false }))
    await assert.rejects(h.harbor.producer().send('orders', { value: 1 }), /refused/)
    assert.equal(await value(registry, 'kafka_harbor_errors_total', { scope: 'producer' }), 1)
    assert.equal(await value(registry, 'kafka_harbor_messages_produced_total', { topic: 'orders' }), 0)
    await h.harbor.shutdown()
  })

  test('consumer lag is collected on scrape, per running consumer and partition, and replaced wholesale', async () => {
    const h = harness({}, { partitions: 2 })
    const registry = new Registry()
    prometheusMetrics(h.harbor, { registry })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    const held = gate()
    consumer.subscribe('orders', async () => { await held.wait })
    await h.harbor.connect()
    await h.harbor.producer().sendBatch('orders', [{ value: 1, partition: 0 }, { value: 2, partition: 0 }, { value: 3, partition: 1 }])
    assert.equal((await registry.getMetricsAsJSON()).find((m) => m.name === 'kafka_harbor_consumer_lag')?.values.length, 0, 'no running consumer, no series')

    await consumer.start()
    assert.equal(await value(registry, 'kafka_harbor_consumer_lag', { group: 'g', topic: 'orders', partition: '0' }), 2)
    assert.equal(await value(registry, 'kafka_harbor_consumer_lag', { group: 'g', topic: 'orders', partition: '1' }), 1)
    held.release()
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(await value(registry, 'kafka_harbor_consumer_lag', { group: 'g', topic: 'orders', partition: '0' }), 0)
    await h.harbor.shutdown()
  })

  test('a lag collection that fails drops the lag series, is reported through the error event, and the scrape still succeeds', async () => {
    const h = harness()
    const registry = new Registry()
    const errors = captureErrors(h.harbor)
    prometheusMetrics(h.harbor, { registry })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    await h.harbor.connect()
    await h.harbor.producer().send('orders', { value: 1 })
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(await value(registry, 'kafka_harbor_consumer_lag', { group: 'g', topic: 'orders', partition: '0' }), 0)

    h.adapter.admin.fetchTopicOffsets = async () => { throw new Error('broker away') }
    const scraped = await registry.getMetricsAsJSON()
    assert.ok(scraped.some((m) => m.name === 'kafka_harbor_messages_processed_total'), 'the scrape succeeded')
    assert.equal(scraped.find((m) => m.name === 'kafka_harbor_consumer_lag')?.values.length, 0, 'unknown lag is no series, not the last value')
    assert.equal(errors.length, 1)
    assert.equal(errors[0]?.scope, 'adapter')
    assert.equal(errors[0]?.groupId, 'g')
    assert.equal((errors[0]?.error as Error).message, 'broker away')
    await h.harbor.shutdown()
  })

  const bucketsOf = async (registry: Registry, name: string): Promise<string[]> => {
    const histogram = (await registry.getMetricsAsJSON()).find((m) => m.name === name)
    const les = new Set((histogram?.values as Array<{ metricName?: string, labels: Record<string, string | number> }>).filter((v) => v.metricName?.endsWith('_bucket')).map((v) => String(v.labels.le)))
    return [...les].sort((a, b) => Number(a) - Number(b))
  }

  test('durations are recorded in seconds, in the default 5ms-to-10s buckets or the ones given', async () => {
    const h = harness()
    const registry = new Registry()
    prometheusMetrics(h.harbor, { registry, lag: false })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => { h.clock.advance(250) })
    const original = h.adapter.produce
    h.adapter.produce = async (records) => { h.clock.advance(40); await original(records) }
    // Produced before the consumer starts, so the handler's time is not inside the produce call.
    await h.harbor.producer().send('orders', { value: 1 })
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(await value(registry, 'kafka_harbor_message_processing_duration_seconds_sum', { group: 'g', topic: 'orders', outcome: 'processed' }), 0.25)
    assert.equal(await value(registry, 'kafka_harbor_produce_duration_seconds_sum', { topic: 'orders', kind: 'send' }), 0.04)
    assert.deepEqual(await bucketsOf(registry, 'kafka_harbor_message_processing_duration_seconds'), ['0.005', '0.01', '0.025', '0.05', '0.1', '0.25', '0.5', '1', '2.5', '5', '10', '+Inf'])

    const custom = new Registry()
    prometheusMetrics(h.harbor, { registry: custom, buckets: [0.5, 5], lag: false })
    await h.harbor.producer().send('orders', { value: 2 })
    await h.adapter.whenDrained('g', 'orders')
    assert.deepEqual(await bucketsOf(custom, 'kafka_harbor_message_processing_duration_seconds'), ['0.5', '5', '+Inf'])
    assert.deepEqual(await bucketsOf(custom, 'kafka_harbor_produce_duration_seconds'), ['0.5', '5', '+Inf'])
    await h.harbor.shutdown()
  })

  test('detach removes the lag gauge: no admin calls on later scrapes, the counters stay', async () => {
    const h = harness()
    const registry = new Registry()
    const metrics = prometheusMetrics(h.harbor, { registry })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    await h.harbor.connect()
    await h.harbor.producer().send('orders', { value: 1 })
    await consumer.start()
    assert.equal((await registry.getMetricsAsJSON()).find((m) => m.name === 'kafka_harbor_consumer_lag')?.values.length, 1)
    metrics.detach()
    h.adapter.clearCalls()
    const scraped = await registry.getMetricsAsJSON()
    assert.equal(scraped.find((m) => m.name === 'kafka_harbor_consumer_lag'), undefined, 'the gauge is gone')
    assert.ok(scraped.some((m) => m.name === 'kafka_harbor_messages_produced_total'), 'the counters stay')
    assert.equal(h.adapter.calls.filter((call) => call.method === 'fetchTopicOffsets').length, 0)
    await h.harbor.shutdown()
  })

  test('detach stops the counting; the prefix is validated; lag can be switched off', async () => {
    const h = harness()
    const registry = new Registry()
    const metrics = prometheusMetrics(h.harbor, { registry, prefix: 'svc_', lag: false })
    assert.equal(metrics.registry, registry)
    assert.equal((await registry.getMetricsAsJSON()).some((m) => m.name === 'svc_consumer_lag'), false)
    await h.harbor.producer().send('orders', { value: 1 })
    assert.equal(await value(registry, 'svc_messages_produced_total', { topic: 'orders' }), 1)
    metrics.detach()
    await h.harbor.producer().send('orders', { value: 2 })
    assert.equal(await value(registry, 'svc_messages_produced_total', { topic: 'orders' }), 1)
    assert.throws(() => prometheusMetrics(h.harbor, { registry: new Registry(), prefix: '9bad' }), { code: ERROR_CODES.CONFIG_INVALID })
    const bare = new Registry()
    prometheusMetrics(h.harbor, { registry: bare, prefix: '', lag: false })
    assert.ok((await bare.getMetricsAsJSON()).some((m) => m.name === 'messages_produced_total'), 'an empty prefix is no prefix')
    await h.harbor.shutdown()
  })

  test('an adapter without offsets is refused at construction unless lag is off', async () => {
    const h = harness()
    delete (h.adapter.admin as { fetchTopicOffsets?: unknown }).fetchTopicOffsets
    assert.throws(() => prometheusMetrics(h.harbor, { registry: new Registry() }), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
      assert.match((error as Error).message, /adapter "memory" does not report offsets.*pass lag: false/)
      return true
    })
    prometheusMetrics(h.harbor, { registry: new Registry(), lag: false })
    await h.harbor.shutdown()
  })
})

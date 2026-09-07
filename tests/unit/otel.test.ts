import assert from 'node:assert/strict'
import { after, before, describe, test } from 'node:test'

import { SpanKind, SpanStatusCode, context, propagation } from '@opentelemetry/api'
import { AsyncLocalStorageContextManager } from '@opentelemetry/context-async-hooks'
import { CompositePropagator, W3CBaggagePropagator, W3CTraceContextPropagator } from '@opentelemetry/core'
import { InMemoryMetricExporter, MeterProvider, PeriodicExportingMetricReader, AggregationTemporality, type DataPoint } from '@opentelemetry/sdk-metrics'
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'

import { ERROR_CODES } from '../../src/index'
import { otelMetrics, otelTracing } from '../../src/otel/index'
import { captureErrors, gate, harness, silentLogger } from '../helpers/harness'
import { until } from '../helpers/manual-clock'

const metering = () => {
  const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE)
  const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 60_000 })
  const provider = new MeterProvider({ readers: [reader] })
  const points = async (name: string): Promise<Array<DataPoint<number> & { attributes: Record<string, unknown> }>> => {
    const { resourceMetrics } = await reader.collect()
    const metric = resourceMetrics.scopeMetrics.flatMap((scope) => scope.metrics).find((entry) => entry.descriptor.name === name)
    return (metric?.dataPoints ?? []) as Array<DataPoint<number> & { attributes: Record<string, unknown> }>
  }
  const point = async (name: string, attributes: Record<string, unknown>): Promise<number | undefined> =>
    (await points(name)).find((candidate) => Object.entries(attributes).every(([key, expected]) => candidate.attributes[key] === expected))?.value
  const descriptors = async (): Promise<Array<{ name: string, description: string, unit: string }>> => {
    const { resourceMetrics } = await reader.collect()
    return resourceMetrics.scopeMetrics.flatMap((scope) => scope.metrics).map(({ descriptor }) => ({ name: descriptor.name, description: descriptor.description, unit: descriptor.unit }))
  }
  return { provider, points, point, descriptors, shutdown: async () => { await provider.shutdown() } }
}

describe('kafka-harbor/otel metrics', () => {
  test('records every outcome as instruments with low-cardinality attributes', async () => {
    const h = harness()
    const m = metering()
    const metrics = otelMetrics(h.harbor, { meterProvider: m.provider })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    consumer.subscribe<string>('orders', (message) => {
      if (message.value === 'fails') throw new Error('always')
    })
    await consumer.start()
    await h.harbor.producer<string>().sendBatch('orders', [{ value: 'ok' }, { value: 'fails' }])
    await until(() => h.adapter.messages('orders-dlq').length === 1)
    await h.adapter.whenDrained('g', 'orders-retry-1')

    assert.equal(await m.point('kafka_harbor.messages.produced', { 'kafka_harbor.topic': 'orders', 'kafka_harbor.kind': 'send' }), 2)
    assert.equal(await m.point('kafka_harbor.messages.produced', { 'kafka_harbor.topic': 'orders-retry-1', 'kafka_harbor.kind': 'retry' }), 1)
    assert.equal(await m.point('kafka_harbor.messages.produced', { 'kafka_harbor.topic': 'orders-dlq', 'kafka_harbor.kind': 'dead-letter' }), 1)
    assert.equal(await m.point('kafka_harbor.messages.processed', { 'kafka_harbor.group': 'g', 'kafka_harbor.topic': 'orders' }), 1)
    assert.equal(await m.point('kafka_harbor.messages.failed', { 'kafka_harbor.topic': 'orders', 'kafka_harbor.outcome': 'retry' }), 1)
    assert.equal(await m.point('kafka_harbor.messages.failed', { 'kafka_harbor.topic': 'orders-retry-1', 'kafka_harbor.outcome': 'dead-letter' }), 1)
    assert.equal(await m.point('kafka_harbor.messages.retried', { 'kafka_harbor.topic': 'orders', 'kafka_harbor.level': 1 }), 1)
    assert.equal(await m.point('kafka_harbor.messages.dead_lettered', { 'kafka_harbor.topic': 'orders-retry-1' }), 1)
    const durations = await m.points('kafka_harbor.message.processing.duration')
    assert.deepEqual(durations.map((p) => p.attributes['kafka_harbor.outcome']).sort(), ['dead-letter', 'processed', 'retry'])
    assert.deepEqual((await m.points('kafka_harbor.produce.duration')).map((p) => p.attributes['kafka_harbor.kind']).sort(), ['dead-letter', 'retry', 'send'])

    await h.harbor.redrive({ from: 'orders-dlq', idleTimeout: 0 })
    assert.equal(await m.point('kafka_harbor.messages.redriven', { 'kafka_harbor.from': 'orders-dlq', 'kafka_harbor.to': 'orders' }), 1)
    assert.equal(await m.point('kafka_harbor.messages.produced', { 'kafka_harbor.topic': 'orders', 'kafka_harbor.kind': 'redrive' }), 1)
    await h.harbor.shutdown()
    assert.equal(await m.point('kafka_harbor.consumer.stops', { 'kafka_harbor.group': 'g', 'kafka_harbor.reason': 'shutdown' }), 1)
    metrics.detach()
    await m.shutdown()
  })

  test('without a meter provider the global one is used, which is inert until an SDK registers', async () => {
    const h = harness()
    const metrics = otelMetrics(h.harbor)
    await h.harbor.producer().send('orders', { value: 1 })
    metrics.detach()
    await h.harbor.shutdown()
  })

  test('a lag failure is reported through the harbor error event and the observation still completes', async () => {
    const h = harness()
    const m = metering()
    const errors = captureErrors(h.harbor)
    otelMetrics(h.harbor, { meterProvider: m.provider })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    await consumer.start()
    h.adapter.admin.fetchTopicOffsets = async () => { throw new Error('broker away') }
    assert.deepEqual(await m.points('kafka_harbor.consumer.lag'), [])
    assert.equal(errors.length, 1)
    assert.equal(errors[0]?.scope, 'adapter')
    assert.equal(errors[0]?.groupId, 'g')
    assert.equal((errors[0]?.error as Error).message, 'broker away')
    await h.harbor.shutdown()
    await m.shutdown()
  })

  test('an adapter without offsets is refused at construction unless lag is off, and then nothing asks for offsets', async () => {
    const h = harness()
    const m = metering()
    delete (h.adapter.admin as { fetchCommittedOffsets?: unknown }).fetchCommittedOffsets
    assert.throws(() => otelMetrics(h.harbor, { meterProvider: m.provider }), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
      assert.match((error as Error).message, /adapter "memory" does not report offsets.*pass lag: false/)
      return true
    })
    const metrics = otelMetrics(h.harbor, { meterProvider: m.provider, lag: false })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    await consumer.start()
    h.adapter.clearCalls()
    assert.equal((await m.descriptors()).some((d) => d.name === 'kafka_harbor.consumer.lag'), false)
    assert.equal(h.adapter.calls.filter((call) => call.method === 'fetchTopicOffsets').length, 0)
    metrics.detach()
    await h.harbor.shutdown()
    await m.shutdown()
  })

  test('every instrument carries a description and a unit; durations are seconds in the default or the given boundaries', async () => {
    const h = harness()
    const m = metering()
    otelMetrics(h.harbor, { meterProvider: m.provider })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => { h.clock.advance(250) })
    const original = h.adapter.produce
    h.adapter.produce = async (records) => { h.clock.advance(40); await original(records) }
    // Produced before the consumer starts, so the handler's time is not inside the produce call.
    await h.harbor.producer().send('orders', { value: 1 })
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    // The SDK exports the instruments that recorded something.
    const described = await m.descriptors()
    assert.deepEqual(described.map((d) => d.name).sort(), ['kafka_harbor.consumer.lag', 'kafka_harbor.message.processing.duration', 'kafka_harbor.messages.processed', 'kafka_harbor.messages.produced', 'kafka_harbor.produce.duration'])
    for (const { name, description, unit } of described) {
      assert.match(name, /^kafka_harbor\./)
      assert.ok(description.length > 20, `${name} has a description`)
      assert.ok(unit.length > 0, `${name} has a unit`)
    }
    assert.deepEqual(described.filter((d) => d.unit === 's').map((d) => d.name).sort(), ['kafka_harbor.message.processing.duration', 'kafka_harbor.produce.duration'])
    const processing = (await m.points('kafka_harbor.message.processing.duration'))[0] as unknown as { value: { sum: number, buckets: { boundaries: number[] } } }
    assert.equal(processing.value.sum, 0.25)
    assert.deepEqual(processing.value.buckets.boundaries, [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10])
    const produce = (await m.points('kafka_harbor.produce.duration'))[0] as unknown as { value: { sum: number } }
    assert.equal(produce.value.sum, 0.04)

    const custom = metering()
    otelMetrics(h.harbor, { meterProvider: custom.provider, boundaries: [0.5, 5], lag: false })
    await h.harbor.producer().send('orders', { value: 2 })
    await h.adapter.whenDrained('g', 'orders')
    const shaped = (await custom.points('kafka_harbor.produce.duration'))[0] as unknown as { value: { buckets: { boundaries: number[] } } }
    assert.deepEqual(shaped.value.buckets.boundaries, [0.5, 5])
    await h.harbor.shutdown()
    await m.shutdown()
    await custom.shutdown()
  })

  test('producer failures count as errors with the producer scope', async () => {
    const h = harness()
    const m = metering()
    otelMetrics(h.harbor, { meterProvider: m.provider, lag: false })
    h.adapter.failNextProduce(Object.assign(new Error('refused'), { retryable: false }))
    await assert.rejects(h.harbor.producer().send('orders', { value: 1 }), /refused/)
    assert.equal(await m.point('kafka_harbor.errors', { 'kafka_harbor.scope': 'producer' }), 1)
    await h.harbor.shutdown()
    await m.shutdown()
  })

  test('lag is an observable gauge collected per running consumer and partition, until detach', async () => {
    const h = harness({}, { partitions: 2 })
    const m = metering()
    const metrics = otelMetrics(h.harbor, { meterProvider: m.provider })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    const held = gate()
    consumer.subscribe('orders', async () => { await held.wait })
    await h.harbor.connect()
    await h.harbor.producer().sendBatch('orders', [{ value: 1, partition: 0 }, { value: 2, partition: 0 }, { value: 3, partition: 1 }])
    await consumer.start()
    assert.equal(await m.point('kafka_harbor.consumer.lag', { 'kafka_harbor.group': 'g', 'kafka_harbor.topic': 'orders', 'kafka_harbor.partition': 0 }), 2)
    assert.equal(await m.point('kafka_harbor.consumer.lag', { 'kafka_harbor.partition': 1 }), 1)
    held.release()
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(await m.point('kafka_harbor.consumer.lag', { 'kafka_harbor.partition': 0 }), 0)

    metrics.detach()
    h.adapter.clearCalls()
    await h.harbor.producer().send('orders', { value: 4 })
    assert.equal(await m.point('kafka_harbor.messages.produced', { 'kafka_harbor.topic': 'orders' }), 3, 'detached: the fourth record is not counted')
    assert.equal(h.adapter.calls.filter((call) => call.method === 'fetchTopicOffsets').length, 0, 'detached: lag is not collected')
    await h.harbor.shutdown()
    await m.shutdown()
  })
})

describe('kafka-harbor/otel tracing', () => {
  const manager = new AsyncLocalStorageContextManager()
  before(() => { context.setGlobalContextManager(manager.enable()) })
  after(() => { context.disable() })

  const tracing = () => {
    const exporter = new InMemorySpanExporter()
    const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] })
    return { exporter, instrumentation: otelTracing({ tracerProvider: provider, propagator: new W3CTraceContextPropagator() }) }
  }

  test('the consumer span is a child of the producer span, through the headers, across the retry ladder', async () => {
    const { exporter, instrumentation } = tracing()
    const h = harness({ instrumentation })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    let attempts = 0
    consumer.subscribe('orders', () => { attempts++; if (attempts === 1) throw new Error('first fails') })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1, headers: { 'x-correlation-id': 'corr-1' } })
    await until(() => attempts === 2)
    await h.adapter.whenDrained('g', 'orders-retry-1')

    const spans = exporter.getFinishedSpans()
    const send = spans.find((span) => span.name === 'orders send')
    const first = spans.find((span) => span.name === 'orders process')
    const second = spans.find((span) => span.name === 'orders-retry-1 process')
    assert.ok(send !== undefined && first !== undefined && second !== undefined, `spans: ${spans.map((s) => s.name).join(', ')}`)

    assert.equal(send.kind, SpanKind.PRODUCER)
    assert.equal(send.attributes['messaging.system'], 'kafka')
    assert.equal(send.attributes['messaging.destination.name'], 'orders')
    assert.equal(send.attributes['messaging.operation.name'], 'send')
    assert.equal(send.attributes['messaging.batch.message_count'], 1)
    assert.equal(send.attributes['kafka_harbor.kind'], 'send')
    assert.match(String(h.adapter.messages('orders')[0]?.headers.traceparent), new RegExp(`^00-${send.spanContext().traceId}-${send.spanContext().spanId}-01$`))

    for (const span of [first, second]) {
      assert.equal(span.kind, SpanKind.CONSUMER)
      assert.equal(span.spanContext().traceId, send.spanContext().traceId, 'one trace for the whole ladder')
      assert.equal(span.parentSpanContext?.spanId, send.spanContext().spanId, 'parented to the producer span')
      assert.equal(span.attributes['messaging.consumer.group.name'], 'g')
      assert.equal(span.attributes['messaging.destination.partition.id'], '0')
      assert.equal(span.attributes['messaging.kafka.offset'], 0)
      assert.equal(span.attributes['kafka_harbor.correlation_id'], 'corr-1')
    }
    assert.equal(first.attributes['kafka_harbor.attempt'], 1)
    assert.equal(first.status.code, SpanStatusCode.ERROR)
    assert.equal(first.events[0]?.name, 'exception')
    assert.equal(second.attributes['kafka_harbor.attempt'], 2)
    assert.equal(second.attributes['kafka_harbor.original_topic'], 'orders')
    assert.equal(second.status.code, SpanStatusCode.UNSET)
    await h.harbor.shutdown()
  })

  test('every hop is a producer span of its own, in the trace of the message it forwards', async () => {
    const { exporter, instrumentation } = tracing()
    const h = harness({ instrumentation })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    consumer.subscribe('orders', () => { throw new Error('always') })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await until(() => h.adapter.messages('orders-dlq').length === 1)
    await h.adapter.whenDrained('g', 'orders-retry-1')
    await consumer.stop()
    const result = await h.harbor.redrive({ from: 'orders-dlq', idleTimeout: 0 })
    assert.equal(result.reprocessed, 1)

    const spans = exporter.getFinishedSpans()
    const send = spans.find((span) => span.name === 'orders send' && span.attributes['kafka_harbor.kind'] === 'send')
    assert.ok(send !== undefined)
    const hops = [
      ['orders-retry-1 send', 'retry'],
      ['orders-dlq send', 'dead-letter'],
      ['orders send', 'redrive']
    ] as const
    for (const [name, kind] of hops) {
      const hop = spans.find((span) => span.name === name && span.attributes['kafka_harbor.kind'] === kind)
      assert.ok(hop !== undefined, `${name} (${kind}) among ${spans.map((s) => `${s.name}/${String(s.attributes['kafka_harbor.kind'] ?? '')}`).join(', ')}`)
      assert.equal(hop.kind, SpanKind.PRODUCER)
      assert.equal(hop.spanContext().traceId, send.spanContext().traceId, 'the hop stays in the trace of the message')
      assert.equal(hop.parentSpanContext?.spanId, send.spanContext().spanId, 'parented to the context the message carries')
      assert.equal(hop.attributes['messaging.batch.message_count'], 1)
    }
    // The forwarded records keep the trace context they arrived with, not the hop's.
    const expected = new RegExp(`^00-${send.spanContext().traceId}-${send.spanContext().spanId}-01$`)
    for (const topic of ['orders-retry-1', 'orders-dlq']) assert.match(String(h.adapter.messages(topic)[0]?.headers.traceparent), expected)
    assert.match(String(h.adapter.messages('orders')[1]?.headers.traceparent), expected, 'the redriven record too')
    await h.harbor.shutdown()
  })

  test('what else the producer propagated, baggage, is active in the handler as well as the span', async () => {
    const exporter = new InMemorySpanExporter()
    const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] })
    const propagator = new CompositePropagator({ propagators: [new W3CTraceContextPropagator(), new W3CBaggagePropagator()] })
    const h = harness({ instrumentation: otelTracing({ tracerProvider: provider, propagator }) })
    const seen: Array<string | undefined> = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => { seen.push(propagation.getActiveBaggage()?.getEntry('tenant')?.value) })
    await consumer.start()
    const withBaggage = propagation.setBaggage(context.active(), propagation.createBaggage({ tenant: { value: 'acme' } }))
    await context.with(withBaggage, async () => { await h.harbor.producer().send('orders', { value: 1 }) })
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(h.adapter.messages('orders')[0]?.headers.baggage, 'tenant=acme')
    assert.deepEqual(seen, ['acme'])
    await h.harbor.shutdown()
  })

  test('a message without a trace context starts a new trace, and a produce failure marks the send span', async () => {
    const { exporter, instrumentation } = tracing()
    const h = harness({ instrumentation })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => {})
    await h.harbor.connect()
    await h.adapter.produce([{ topic: 'orders', key: null, value: Buffer.from('1'), headers: {} }])
    await consumer.start()
    await h.adapter.whenDrained('g', 'orders')
    const process = exporter.getFinishedSpans().find((span) => span.name === 'orders process')
    assert.equal(process?.parentSpanContext, undefined)
    assert.equal(process?.attributes['kafka_harbor.correlation_id'], undefined)

    h.adapter.failNextProduce(Object.assign(new Error('refused'), { retryable: false }))
    await assert.rejects(h.harbor.producer().send('orders', { value: 2 }), /refused/)
    const send = exporter.getFinishedSpans().find((span) => span.name === 'orders send')
    assert.equal(send?.status.code, SpanStatusCode.ERROR)
    assert.equal(send?.status.message, 'refused')
    await h.harbor.shutdown()
  })

  test('a handler that throws a non-Error value still marks the span', async () => {
    const { exporter, instrumentation } = tracing()
    const h = harness({ instrumentation })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    // eslint-disable-next-line no-throw-literal
    consumer.subscribe('orders', () => { throw 'plain string' })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await h.adapter.whenDrained('g', 'orders')
    const span = exporter.getFinishedSpans().find((candidate) => candidate.name === 'orders process')
    assert.equal(span?.status.code, SpanStatusCode.ERROR)
    assert.equal(span?.status.message, 'plain string')
    assert.equal(span?.events[0]?.attributes?.['exception.message'], 'plain string')
    await h.harbor.shutdown()
  })

  test('an offset past 2^53 is kept exact as a string and left out of the integer attribute', async () => {
    const { exporter, instrumentation } = tracing()
    const message = { topic: 'orders', partition: 0, offset: '9007199254740993', key: null, value: null, headers: {}, timestamp: new Date(0) }
    const handlerContext = { groupId: 'g', correlationId: undefined, logger: silentLogger, signal: new AbortController().signal, attempt: 1 }
    await instrumentation.wrapHandler?.(message, handlerContext, async () => {})
    const span = exporter.getFinishedSpans().find((candidate) => candidate.name === 'orders process')
    assert.equal(span?.attributes['messaging.kafka.offset'], undefined)
    assert.equal(span?.attributes['kafka_harbor.offset'], '9007199254740993')
  })

  test('without an SDK the hooks are inert and the pipeline runs as usual', async () => {
    const h = harness({ instrumentation: otelTracing() })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    let handled = 0
    consumer.subscribe('orders', () => { handled++ })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(handled, 1)
    assert.equal(h.adapter.messages('orders')[0]?.headers.traceparent, undefined, 'the global propagator is a no-op without an SDK')
    assert.deepEqual(h.logs.filter((log) => log.level === 'error'), [])
    await h.harbor.shutdown()
  })
})

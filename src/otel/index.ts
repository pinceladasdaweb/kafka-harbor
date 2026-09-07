/**
 * OpenTelemetry for a harbor, through @opentelemetry/api (an optional peer
 * dependency): metrics derived from the typed events and `lag()`, and
 * tracing hooks that carry the trace context through the message headers.
 * The core never imports this module.
 */
import {
  SpanKind,
  SpanStatusCode,
  context,
  defaultTextMapGetter,
  defaultTextMapSetter,
  metrics,
  propagation,
  trace,
  type Attributes,
  type Context,
  type MeterProvider,
  type ObservableResult,
  type Span,
  type TextMapPropagator,
  type TracerProvider
} from '@opentelemetry/api'

import {
  ConfigError,
  DEFAULT_DURATION_BUCKETS,
  describeError,
  reportsOffsets,
  subscribe,
  type Harbor,
  type HarborEvents,
  type Instrumentation,
  type Listeners,
  type MessageHeaders
} from '../index'

/** The instrumentation scope every kafka-harbor signal reports under. */
const SCOPE = 'kafka-harbor'

export interface OtelMetricsOptions {
  /**
   * The MeterProvider the instruments are created from. Default: the API's
   * global provider. The metrics API has no late-binding proxy: instruments
   * created before `metrics.setGlobalMeterProvider()` (or before your SDK
   * starts) are no-ops forever. Start the SDK first, or pass the provider.
   */
  meterProvider?: MeterProvider
  /**
   * Bucket boundaries for the duration histograms, in seconds. Default: 5ms
   * to 10s, the same shape as the Prometheus entry point. The SDK's own
   * default boundaries are calibrated for milliseconds and would put nearly
   * every value in the first bucket.
   */
  boundaries?: readonly number[]
  /**
   * Whether to export `kafka_harbor.consumer.lag`. Every collection then
   * asks the broker for the watermarks and committed offsets of each running
   * consumer. Default: true. An adapter that does not report offsets is
   * refused with a ConfigError unless this is false.
   */
  lag?: boolean
}

export interface OtelMetrics {
  /** Unsubscribes from the harbor and stops observing lag. */
  detach: () => void
}

/**
 * The OTel counterpart of `kafka-harbor/prometheus`: the same signals as
 * instruments under the `kafka_harbor.` namespace, with low-cardinality
 * attributes (group, topic, outcome, level, kind, scope, reason, from, to,
 * partition). A lag collection that fails contributes no data points for
 * that observation; the harbor reports the failure through its `error`
 * event.
 */
export function otelMetrics (harbor: Harbor, options: OtelMetricsOptions = {}): OtelMetrics {
  const lag = options.lag ?? true
  if (lag && !reportsOffsets(harbor.config.adapter.admin)) {
    throw new ConfigError(`adapter "${harbor.config.adapter.name}" does not report offsets, so consumer lag cannot be collected; pass lag: false`)
  }
  const meter = (options.meterProvider ?? metrics.getMeterProvider()).getMeter(SCOPE)
  const advice = { explicitBucketBoundaries: [...(options.boundaries ?? DEFAULT_DURATION_BUCKETS)] }

  const processed = meter.createCounter('kafka_harbor.messages.processed', { description: 'Messages whose handler succeeded and whose offset was committed.', unit: '{message}' })
  const processingDuration = meter.createHistogram('kafka_harbor.message.processing.duration', { description: 'Handler duration by outcome, retry delays excluded.', unit: 's', advice })
  const failed = meter.createCounter('kafka_harbor.messages.failed', { description: 'Handler failures by what happened next: retry, dead-letter, abort or crash.', unit: '{message}' })
  const retried = meter.createCounter('kafka_harbor.messages.retried', { description: 'Messages forwarded to a retry topic, by level.', unit: '{message}' })
  const deadLettered = meter.createCounter('kafka_harbor.messages.dead_lettered', { description: 'Messages forwarded to the dead-letter topic, counted after the broker acknowledged the produce.', unit: '{message}' })
  const redriven = meter.createCounter('kafka_harbor.messages.redriven', { description: 'Dead letters re-injected by harbor.redrive().', unit: '{message}' })
  const produced = meter.createCounter('kafka_harbor.messages.produced', { description: 'Records acknowledged by the broker, by destination topic and kind: send, retry, dead-letter or redrive.', unit: '{record}' })
  const produceDuration = meter.createHistogram('kafka_harbor.produce.duration', { description: 'Time from the produce call to the broker acknowledgment, retries included.', unit: 's', advice })
  const errors = meter.createCounter('kafka_harbor.errors', { description: 'Errors reported through the error event, by scope.', unit: '{error}' })
  const stops = meter.createCounter('kafka_harbor.consumer.stops', { description: 'Consumers that stopped, by reason.', unit: '{stop}' })

  const at = (event: { groupId: string, topic: string }): Attributes => ({ 'kafka_harbor.group': event.groupId, 'kafka_harbor.topic': event.topic })
  const listeners: Listeners<HarborEvents> = {
    messageProcessed: (event) => {
      const attributes = at(event)
      processed.add(1, attributes)
      processingDuration.record(event.durationMs / 1_000, { ...attributes, 'kafka_harbor.outcome': 'processed' })
    },
    messageFailed: (event) => {
      const attributes = { ...at(event), 'kafka_harbor.outcome': event.outcome }
      failed.add(1, attributes)
      processingDuration.record(event.durationMs / 1_000, attributes)
    },
    messageRetried: (event) => { retried.add(1, { ...at(event), 'kafka_harbor.level': event.level }) },
    messageDeadLettered: (event) => { deadLettered.add(1, at(event)) },
    messageRedriven: (event) => { redriven.add(1, { 'kafka_harbor.from': event.from, 'kafka_harbor.to': event.to }) },
    messageProduced: (event) => {
      const attributes = { 'kafka_harbor.topic': event.topic, 'kafka_harbor.kind': event.kind }
      produced.add(event.records, attributes)
      produceDuration.record(event.durationMs / 1_000, attributes)
    },
    error: (event) => { errors.add(1, { 'kafka_harbor.scope': event.scope }) },
    consumerStopped: (event) => { stops.add(1, { 'kafka_harbor.group': event.groupId, 'kafka_harbor.reason': event.reason }) }
  }
  const disposers = [subscribe(harbor, listeners)]

  if (lag) {
    const gauge = meter.createObservableGauge('kafka_harbor.consumer.lag', {
      description: 'Records between a running consumer group\'s position and the high watermark, per partition. Collected on observation.',
      unit: '{record}'
    })
    // A failed collection was reported by the harbor already; here it is no data points.
    const observeLag = async (result: ObservableResult): Promise<void> => {
      for (const entry of await harbor.lag().catch(() => [])) {
        result.observe(entry.lag, { 'kafka_harbor.group': entry.groupId, 'kafka_harbor.topic': entry.topic, 'kafka_harbor.partition': entry.partition })
      }
    }
    gauge.addCallback(observeLag)
    disposers.push(() => { gauge.removeCallback(observeLag) })
  }

  return {
    detach () {
      for (const dispose of disposers) dispose()
    }
  }
}

export interface OtelTracingOptions {
  /**
   * The TracerProvider the spans come from. Default: the API's global
   * provider, which late-binds: hooks created before the SDK starts pick it
   * up once it is registered.
   */
  tracerProvider?: TracerProvider
  /**
   * The propagator that writes and reads the trace context in the message
   * headers. Default: the API's global propagator (W3C `traceparent` and
   * `tracestate` once an SDK registered one).
   */
  propagator?: TextMapPropagator
}

const spanAttributes = (topic: string, operation: 'send' | 'process'): Attributes => ({
  'messaging.system': 'kafka',
  'messaging.destination.name': topic,
  'messaging.operation.name': operation,
  'messaging.operation.type': operation
})

/** Marks the span with what the work threw. */
const recordFailure = (span: Span, error: unknown): void => {
  span.recordException(error instanceof Error ? error : String(error))
  span.setStatus({ code: SpanStatusCode.ERROR, message: describeError(error) })
}

/**
 * Tracing hooks for `createHarbor({ instrumentation })`: a PRODUCER span per
 * produce call whose context is written into every record's headers, and a
 * CONSUMER span per handler run, parented to the context found in the
 * message headers. A retry, DLQ or redrive hop is a PRODUCER span of its own
 * (`kafka_harbor.kind` says which), parented to the trace the forwarded
 * message carries, and the hop copies that message's headers, so the whole
 * ladder of one message, and its redrive, belong to the trace that produced
 * it. Attribute names follow the OpenTelemetry messaging semantic
 * conventions.
 *
 * A hook that fails never changes the outcome of what it wrapped: the core
 * runs the work and logs the failure.
 */
export function otelTracing (options: OtelTracingOptions = {}): Instrumentation {
  const tracer = (options.tracerProvider ?? trace.getTracerProvider()).getTracer(SCOPE)
  // The global propagator is looked up per call: the SDK registers it after
  // these hooks are usually built.
  const propagator = (): TextMapPropagator => options.propagator ?? propagation
  const extracted = (headers: MessageHeaders): Context => propagator().extract(context.active(), headers, defaultTextMapGetter)

  const traced = async <T>(span: Span, parent: Context, run: () => Promise<T>): Promise<T> => {
    try {
      return await context.with(trace.setSpan(parent, span), run)
    } catch (error) {
      recordFailure(span, error)
      throw error
    } finally {
      span.end()
    }
  }

  return {
    async wrapProduce (batch, run) {
      // A hop belongs to the trace of the message it forwards; the
      // application's own send to whatever is active where it was called.
      const parent = batch.origin === undefined ? context.active() : extracted(batch.origin.headers)
      const span = tracer.startSpan(`${batch.topic} send`, {
        kind: SpanKind.PRODUCER,
        attributes: {
          ...spanAttributes(batch.topic, 'send'),
          'messaging.batch.message_count': batch.records,
          'kafka_harbor.kind': batch.kind
        }
      }, parent)
      return await traced(span, parent, run)
    },

    onProduce () {
      // Runs inside wrapProduce, so the active context carries the send span.
      const carrier: MessageHeaders = {}
      propagator().inject(context.active(), carrier, defaultTextMapSetter)
      return carrier
    },

    async wrapHandler (message, handlerContext, run) {
      const parent = extracted(message.headers)
      const span = tracer.startSpan(`${message.topic} process`, {
        kind: SpanKind.CONSUMER,
        attributes: {
          ...spanAttributes(message.topic, 'process'),
          'messaging.consumer.group.name': handlerContext.groupId,
          'messaging.destination.partition.id': String(message.partition),
          // The convention types the offset as an integer; one past 2^53 is
          // kept exact as a string in the library's own attribute instead.
          'messaging.kafka.offset': Number.isSafeInteger(Number(message.offset)) ? Number(message.offset) : undefined,
          'kafka_harbor.offset': message.offset,
          'kafka_harbor.attempt': handlerContext.attempt,
          // Attributes drop undefined values, which is the omit-when-absent semantics wanted here.
          'kafka_harbor.correlation_id': handlerContext.correlationId,
          'kafka_harbor.original_topic': message.retry?.originalTopic
        }
      }, parent)
      // The handler runs under the extracted context, so what else the
      // producer propagated (baggage) is active there too, not only the span.
      return await traced(span, parent, run)
    }
  }
}

/**
 * Prometheus metrics for a harbor, through prom-client (an optional peer
 * dependency). Everything is derived from the harbor's typed events and its
 * `lag()`; the core never imports this module.
 */
import { Counter, Gauge, Histogram, register as globalRegistry, type Registry } from 'prom-client'

import { ConfigError, DEFAULT_DURATION_BUCKETS, reportsOffsets, subscribe, type Harbor, type HarborEvents, type Listeners } from '../index'

export interface PrometheusMetricsOptions {
  /**
   * The prom-client registry the metrics are registered in. Default:
   * prom-client's global registry. Metric names are unique per registry, so
   * attach ONE harbor per registry, or give each its own prefix.
   */
  registry?: Registry
  /** Prepended to every metric name; '' for none. Default: 'kafka_harbor_'. */
  prefix?: string
  /** Buckets for the duration histograms, in seconds. Default: 5ms to 10s. */
  buckets?: readonly number[]
  /**
   * Whether to export `consumer_lag`. Every scrape then asks the broker for
   * the watermarks and committed offsets of each running consumer, one
   * round of admin calls, never per message. Default: true. An adapter that
   * does not report offsets is refused with a ConfigError unless this is
   * false.
   */
  lag?: boolean
}

/** What `prometheusMetrics()` returns: the registry to serve, and the way out. */
export interface PrometheusMetrics {
  readonly registry: Registry
  /**
   * Unsubscribes from the harbor. The counters and histograms stay
   * registered with their last values (call `registry.clear()` to drop
   * them); the lag gauge, which nothing collects any more, is removed.
   */
  detach: () => void
}

// The prefix starts every metric name, so it must be a valid name by itself, or nothing.
const METRIC_PREFIX = /^(?:[a-zA-Z_:][a-zA-Z0-9_:]*)?$/

/**
 * Registers the harbor's metrics and subscribes to its events. Label sets
 * are deliberately low-cardinality: group, topic, outcome, level, kind,
 * scope, reason, from, to and partition; never an offset or a correlation id.
 *
 * A lag collection that fails (the broker did not answer) leaves the gauge
 * without series for that scrape; the harbor reports the failure through
 * its `error` event, and the scrape succeeds.
 *
 * Useful queries:
 *   sum by (group, topic) (rate(kafka_harbor_messages_failed_total{outcome="dead-letter"}[5m]))
 *   max by (group, topic) (kafka_harbor_consumer_lag)
 *   histogram_quantile(0.95, sum by (le, topic) (rate(kafka_harbor_message_processing_duration_seconds_bucket[5m])))
 */
export function prometheusMetrics (harbor: Harbor, options: PrometheusMetricsOptions = {}): PrometheusMetrics {
  const registry = options.registry ?? globalRegistry
  const prefix = options.prefix ?? 'kafka_harbor_'
  if (!METRIC_PREFIX.test(prefix)) {
    throw new ConfigError(`prometheusMetrics prefix must match ${METRIC_PREFIX}, got ${JSON.stringify(prefix)}`)
  }
  const lag = options.lag ?? true
  if (lag && !reportsOffsets(harbor.config.adapter.admin)) {
    throw new ConfigError(`adapter "${harbor.config.adapter.name}" does not report offsets, so consumer lag cannot be collected; pass lag: false`)
  }
  const registers = [registry]
  const buckets = [...(options.buckets ?? DEFAULT_DURATION_BUCKETS)]

  const processed = new Counter({
    name: `${prefix}messages_processed_total`,
    help: 'Messages whose handler succeeded and whose offset was committed.',
    labelNames: ['group', 'topic'],
    registers
  })
  const processingDuration = new Histogram({
    name: `${prefix}message_processing_duration_seconds`,
    help: 'Handler duration by outcome (processed, retry, dead-letter, abort, crash), retry delays excluded.',
    labelNames: ['group', 'topic', 'outcome'],
    buckets,
    registers
  })
  const failed = new Counter({
    name: `${prefix}messages_failed_total`,
    help: 'Handler failures by what happened next: retry, dead-letter, abort or crash.',
    labelNames: ['group', 'topic', 'outcome'],
    registers
  })
  const retried = new Counter({
    name: `${prefix}messages_retried_total`,
    help: 'Messages forwarded to a retry topic, by the level they were sent to.',
    labelNames: ['group', 'topic', 'level'],
    registers
  })
  const deadLettered = new Counter({
    name: `${prefix}messages_dead_lettered_total`,
    help: 'Messages forwarded to the dead-letter topic, counted after the broker acknowledged the produce.',
    labelNames: ['group', 'topic'],
    registers
  })
  const redriven = new Counter({
    name: `${prefix}messages_redriven_total`,
    help: 'Dead letters re-injected by harbor.redrive(), by source and destination topic.',
    labelNames: ['from', 'to'],
    registers
  })
  const produced = new Counter({
    name: `${prefix}messages_produced_total`,
    help: 'Records acknowledged by the broker, by destination topic and kind: send (harbor.producer()), retry, dead-letter or redrive.',
    labelNames: ['topic', 'kind'],
    registers
  })
  const produceDuration = new Histogram({
    name: `${prefix}produce_duration_seconds`,
    help: 'Time from the produce call to the broker acknowledgment, retries included, by destination topic and kind.',
    labelNames: ['topic', 'kind'],
    buckets,
    registers
  })
  const errors = new Counter({
    name: `${prefix}errors_total`,
    help: 'Errors reported through the error event, by scope: consumer, producer or adapter.',
    labelNames: ['scope'],
    registers
  })
  const stops = new Counter({
    name: `${prefix}consumer_stops_total`,
    help: 'Consumers that stopped, by reason: shutdown, abort or crash.',
    labelNames: ['group', 'reason'],
    registers
  })

  const lagGauge = `${prefix}consumer_lag`
  if (lag) {
    // The gauge collects on scrape: one round of admin calls per scrape,
    // series replaced wholesale so a partition that went away disappears.
    // A failed collection was reported by the harbor already; here it is
    // no data, never the last value.
    new Gauge({ // eslint-disable-line no-new
      name: lagGauge,
      help: 'Records between a running consumer group\'s position and the high watermark, per partition. Collected on scrape.',
      labelNames: ['group', 'topic', 'partition'],
      registers,
      async collect () {
        const lags = await harbor.lag().catch(() => [])
        this.reset()
        for (const entry of lags) this.set({ group: entry.groupId, topic: entry.topic, partition: String(entry.partition) }, entry.lag)
      }
    })
  }

  const listeners: Listeners<HarborEvents> = {
    messageProcessed: (event) => {
      processed.inc({ group: event.groupId, topic: event.topic })
      processingDuration.observe({ group: event.groupId, topic: event.topic, outcome: 'processed' }, event.durationMs / 1_000)
    },
    messageFailed: (event) => {
      failed.inc({ group: event.groupId, topic: event.topic, outcome: event.outcome })
      processingDuration.observe({ group: event.groupId, topic: event.topic, outcome: event.outcome }, event.durationMs / 1_000)
    },
    messageRetried: (event) => { retried.inc({ group: event.groupId, topic: event.topic, level: String(event.level) }) },
    messageDeadLettered: (event) => { deadLettered.inc({ group: event.groupId, topic: event.topic }) },
    messageRedriven: (event) => { redriven.inc({ from: event.from, to: event.to }) },
    messageProduced: (event) => {
      produced.inc({ topic: event.topic, kind: event.kind }, event.records)
      produceDuration.observe({ topic: event.topic, kind: event.kind }, event.durationMs / 1_000)
    },
    error: (event) => { errors.inc({ scope: event.scope }) },
    consumerStopped: (event) => { stops.inc({ group: event.groupId, reason: event.reason }) }
  }
  const unsubscribe = subscribe(harbor, listeners)

  return {
    registry,
    detach () {
      unsubscribe()
      if (lag) registry.removeSingleMetric(lagGauge)
    }
  }
}

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
   * prom-client's global registry. Several harbors may share one (and a
   * harbor may be attached again after `detach()`): they count into the
   * same metrics, and the lag gauge covers every harbor attached to it. A
   * metric of the same name that something else registered with another
   * type or other labels is refused; give this one a prefix of its own.
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
   * them), and a harbor attached later counts on from there; the lag gauge
   * stops collecting this harbor's lag, and is removed once no harbor
   * attached to the registry is left.
   */
  detach: () => void
}

// The prefix starts every metric name, so it must be a valid name by itself, or nothing.
const METRIC_PREFIX = /^(?:[a-zA-Z_:][a-zA-Z0-9_:]*)?$/

/**
 * The attachments a registry's lag gauge collects, kept on the gauge itself
 * under a registered symbol: a registry cleared since carries no stale list,
 * and every copy of this module (an ESM and a CJS build side by side) finds
 * the same one.
 */
const LAG_SOURCES = Symbol.for('kafka-harbor:lag-sources')
interface LagGauge extends Gauge {
  [LAG_SOURCES]?: Set<{ readonly harbor: Harbor }>
}

/**
 * Where a harbor is attached. `current` says whether the registry still
 * holds the metrics (a `registry.clear()` drops them without a detach), and
 * `detach` lets go of the listeners.
 */
interface Attachment {
  readonly registry: Registry
  readonly prefix: string
  readonly current: () => boolean
  readonly detach: () => void
}

/** The registries and prefixes each harbor is attached to: attached twice, every event would count twice. */
const attachments = new WeakMap<Harbor, Attachment[]>()

type MetricSpec = readonly [name: string, help: string, labelNames: readonly string[]]

/**
 * The metric already registered under `name`, when it is one these metrics
 * can count into: the same type, the same labels and, for a histogram, the
 * same buckets (another harbor's, or this one's before a detach). Anything
 * else under that name is refused.
 */
function registered<M extends object> (registry: Registry, [name, , labelNames]: MetricSpec, type: abstract new (...args: never[]) => M, buckets?: readonly number[]): M | undefined {
  const existing: unknown = registry.getSingleMetric(name)
  if (existing === undefined) return undefined
  // Label names and bucket bounds cannot hold a comma, so the joined lists
  // compare exactly; one registered without a list compares as different.
  const shape = (metric: unknown): string => `${String((metric as { labelNames?: unknown }).labelNames)}|${String((metric as { upperBounds?: unknown }).upperBounds)}`
  if (existing instanceof type && shape(existing) === `${labelNames.join(',')}|${String(buckets)}`) return existing
  throw new ConfigError(`the registry already has a metric named "${name}" of another type, with other labels or buckets, or from another copy of prom-client; give prometheusMetrics a prefix of its own`)
}

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
  const previous = (attachments.get(harbor) ?? []).find((entry) => entry.registry === registry && entry.prefix === prefix)
  if (previous?.current() === true) {
    throw new ConfigError(`this harbor is attached to this registry with prefix ${JSON.stringify(prefix)} already; detach() it first, or every event counts twice`)
  }
  // Cleared from the registry since: its listeners would keep counting into
  // metrics nobody scrapes.
  previous?.detach()
  const attached = attachments.get(harbor) ?? []

  const counters = {
    processed: [`${prefix}messages_processed_total`, 'Messages whose handler succeeded and whose offset was committed.', ['group', 'topic']],
    replayed: [`${prefix}messages_replayed_total`, 'Messages whose handler did not run because the idempotency engine replayed an earlier outcome; counted in messages_processed_total too.', ['group', 'topic']],
    batches: [`${prefix}batches_processed_total`, 'Batch handler runs (subscribeBatch) by outcome: processed, retry, dead-letter, abort or crash.', ['group', 'topic', 'outcome']],
    failed: [`${prefix}messages_failed_total`, 'Handler failures by what happened next: retry, dead-letter, abort or crash.', ['group', 'topic', 'outcome']],
    retried: [`${prefix}messages_retried_total`, 'Messages forwarded to a retry topic, by the level they were sent to.', ['group', 'topic', 'level']],
    deadLettered: [`${prefix}messages_dead_lettered_total`, 'Messages forwarded to the dead-letter topic, counted after the broker acknowledged the produce.', ['group', 'topic']],
    circuitChanges: [`${prefix}circuit_state_changes_total`, 'Circuit breaker transitions per topic, by the state entered: open means the partitions of that topic are being held.', ['group', 'topic', 'to']],
    transactions: [`${prefix}transactions_total`, 'Transactions ended, by outcome: committed or aborted.', ['outcome']],
    redriven: [`${prefix}messages_redriven_total`, 'Dead letters re-injected by harbor.redrive(), by source and destination topic.', ['from', 'to']],
    produced: [`${prefix}messages_produced_total`, 'Records acknowledged by the broker, by destination topic and kind: send (harbor.producer()), retry, dead-letter, redrive or transaction.', ['topic', 'kind']],
    errors: [`${prefix}errors_total`, 'Errors reported through the error event, by scope: consumer, producer or adapter.', ['scope']],
    stops: [`${prefix}consumer_stops_total`, 'Consumers that stopped, by reason: shutdown, abort or crash.', ['group', 'reason']]
  } as const satisfies Record<string, MetricSpec>
  const histograms = {
    processingDuration: [`${prefix}message_processing_duration_seconds`, 'Handler duration per message by outcome (processed, retry, dead-letter, abort, crash), retry delays excluded; messages handled in a batch are measured by batch_processing_duration_seconds instead.', ['group', 'topic', 'outcome']],
    batchDuration: [`${prefix}batch_processing_duration_seconds`, 'Batch handler duration by outcome; the messages of the batch are counted in messages_processed_total and messages_failed_total.', ['group', 'topic', 'outcome']],
    produceDuration: [`${prefix}produce_duration_seconds`, 'Time from the produce call to the broker acknowledgment, retries included, by destination topic and kind.', ['topic', 'kind']]
  } as const satisfies Record<string, MetricSpec>
  const lagGauge = `${prefix}consumer_lag`

  // Every conflict is found before anything is registered: a refused
  // attachment leaves the registry as it was.
  const existingCounters = Object.fromEntries(Object.entries(counters).map(([key, spec]) => [key, registered(registry, spec, Counter)]))
  const existingHistograms = Object.fromEntries(Object.entries(histograms).map(([key, spec]) => [key, registered(registry, spec, Histogram, buckets)]))
  const existingGauge = lag ? registry.getSingleMetric(lagGauge) as LagGauge | undefined : undefined
  if (existingGauge !== undefined && existingGauge[LAG_SOURCES] === undefined) {
    throw new ConfigError(`the registry already has a metric named "${lagGauge}" that these metrics did not register; give prometheusMetrics a prefix of its own`)
  }

  const counter = (key: keyof typeof counters): Counter => {
    const [name, help, labelNames] = counters[key]
    return existingCounters[key] ?? new Counter({ name, help, labelNames: [...labelNames], registers })
  }
  const histogram = (key: keyof typeof histograms): Histogram => {
    const [name, help, labelNames] = histograms[key]
    return existingHistograms[key] ?? new Histogram({ name, help, labelNames: [...labelNames], buckets, registers })
  }
  const processed = counter('processed')
  const replayed = counter('replayed')
  const processingDuration = histogram('processingDuration')
  const batches = counter('batches')
  const batchDuration = histogram('batchDuration')
  const failed = counter('failed')
  const retried = counter('retried')
  const deadLettered = counter('deadLettered')
  const circuitChanges = counter('circuitChanges')
  const transactions = counter('transactions')
  const redriven = counter('redriven')
  const produced = counter('produced')
  const produceDuration = histogram('produceDuration')
  const errors = counter('errors')
  const stops = counter('stops')

  // This attachment, one of the gauge's sources.
  const source = { harbor }
  let gauge: LagGauge | undefined
  if (lag) {
    gauge = existingGauge
    if (gauge === undefined) {
      const sources = new Set<{ readonly harbor: Harbor }>()
      // The gauge collects on scrape: one round of admin calls per harbor
      // and scrape, series replaced wholesale so a partition that went
      // away disappears. A failed collection was reported by its harbor
      // already; here it is no data, never the last value. Two harbors
      // reporting the same group, topic and partition write one series.
      gauge = new Gauge({
        name: lagGauge,
        help: 'Records between a running consumer group\'s position and the high watermark, per partition. Collected on scrape.',
        labelNames: ['group', 'topic', 'partition'],
        registers,
        async collect () {
          const lags = await Promise.all([...sources].map(async (entry) => await entry.harbor.lag().catch(() => [])))
          this.reset()
          for (const entry of lags.flat()) this.set({ group: entry.groupId, topic: entry.topic, partition: String(entry.partition) }, entry.lag)
        }
      }) as LagGauge
      gauge[LAG_SOURCES] = sources
    }
    ;(gauge[LAG_SOURCES] as Set<{ readonly harbor: Harbor }>).add(source)
  }
  const attachment: Attachment = {
    registry,
    prefix,
    current: () => registry.getSingleMetric(counters.processed[0]) === processed,
    // Idempotent: every step is a no-op the second time.
    detach () {
      unsubscribe()
      attachments.set(harbor, (attachments.get(harbor) as Attachment[]).filter((entry) => entry !== attachment))
      const sources = gauge?.[LAG_SOURCES]
      if (sources === undefined || !sources.delete(source) || sources.size > 0) return
      // The gauge goes with its last source, unless the registry already
      // let go of it (cleared since) and holds another one now.
      if (registry.getSingleMetric(lagGauge) === gauge) registry.removeSingleMetric(lagGauge)
    }
  }
  attachments.set(harbor, [...attached, attachment])

  const listeners: Listeners<HarborEvents> = {
    messageProcessed: (event) => {
      processed.inc({ group: event.groupId, topic: event.topic })
      if (event.replayed) replayed.inc({ group: event.groupId, topic: event.topic })
      if (event.batch === undefined) processingDuration.observe({ group: event.groupId, topic: event.topic, outcome: 'processed' }, event.durationMs / 1_000)
    },
    messageFailed: (event) => {
      failed.inc({ group: event.groupId, topic: event.topic, outcome: event.outcome })
      if (event.batch === undefined) processingDuration.observe({ group: event.groupId, topic: event.topic, outcome: event.outcome }, event.durationMs / 1_000)
    },
    batchProcessed: (event) => {
      batches.inc({ group: event.groupId, topic: event.topic, outcome: event.outcome })
      batchDuration.observe({ group: event.groupId, topic: event.topic, outcome: event.outcome }, event.durationMs / 1_000)
    },
    messageRetried: (event) => { retried.inc({ group: event.groupId, topic: event.topic, level: String(event.level) }) },
    messageDeadLettered: (event) => { deadLettered.inc({ group: event.groupId, topic: event.topic }) },
    messageRedriven: (event) => { redriven.inc({ from: event.from, to: event.to }) },
    batchProduced: (event) => {
      produced.inc({ topic: event.topic, kind: event.kind }, event.records)
      produceDuration.observe({ topic: event.topic, kind: event.kind }, event.durationMs / 1_000)
    },
    error: (event) => { errors.inc({ scope: event.scope }) },
    consumerStopped: (event) => { stops.inc({ group: event.groupId, reason: event.reason }) },
    circuitStateChanged: (event) => { circuitChanges.inc({ group: event.groupId, topic: event.topic, to: event.to }) },
    transactionCompleted: (event) => { transactions.inc({ outcome: event.outcome }) }
  }
  const unsubscribe = subscribe(harbor, listeners)

  return {
    registry,
    detach: () => { attachment.detach() }
  }
}

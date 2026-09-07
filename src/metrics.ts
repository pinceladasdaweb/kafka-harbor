/**
 * Bucket boundaries for the duration histograms, in seconds: 5ms to 10s, the
 * shape prom-client uses by default. Declared once so the Prometheus and
 * OpenTelemetry entry points export comparable histograms whatever either
 * SDK's own default is.
 */
export const DEFAULT_DURATION_BUCKETS: readonly number[] = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]

/** What `lag()` needs from an adapter; the metrics entry points refuse an adapter without it unless told not to collect lag. */
export const reportsOffsets = (admin: { fetchTopicOffsets?: unknown, fetchCommittedOffsets?: unknown }): boolean =>
  admin.fetchTopicOffsets !== undefined && admin.fetchCommittedOffsets !== undefined

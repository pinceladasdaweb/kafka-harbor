/**
 * kafka-harbor: the application layer over Kafka clients.
 *
 * The public API is tracked by the API Extractor report in `etc/`; CI fails
 * when exports drift from the committed report.
 *
 * Subpath entry points (adapters, testing) import their runtime values from
 * THIS module, never from deep paths: the build externalizes only the core
 * bundle, so a deep import would inline a private copy of anything it
 * touches and break `instanceof` and nominal types across entry points.
 *
 * @packageDocumentation
 */

export { Harbor, abortProcessing, createHarbor } from './harbor'
export type { ConsumerHealth, ExposedHarborConfig, HarborConfig, HarborEvents, HarborHealth, HarborState, HeaderOptions } from './harbor'

export type { HarborErrorEvent } from './context'

export type { Instrumentation, OutgoingRecord, ProduceBatch, ProduceKind } from './instrumentation'
export { defaultIdempotencyKey } from './idempotency'

export type { Transaction, TransactionEvents, TransactionOptions } from './transaction'

export type { ConsumerBreakerOptions } from './breaker'
export type { IdempotencyEngine, IdempotencyInput, IdempotencyOptions } from './idempotency'
export type { ProduceEvents, ProducedEvent } from './produce'

export type { RedriveEvents, RedriveOptions, RedriveResult } from './redrive'

// The pipelines are built by the harbor (`harbor.producer()`,
// `harbor.consumer()`): their classes are types here, not constructors.
export type { Producer, ProducerEvents, ProducerOptions, ProducerRetryOptions } from './producer'

export type {
  BatchContext,
  BatchHandler,
  Consumer,
  ConsumerDlqOptions,
  ConsumerEvents,
  ConsumerOptions,
  ConsumerRetryOptions,
  ConsumerState,
  FailureOutcome,
  Handler,
  HandlerContext,
  PartitionLag,
  RetryContext,
  StopReason,
  SubscribeBatchOptions,
  SubscribeOptions,
  TopicDefaults
} from './consumer'

export type { Clock, Duration, MessageHeaders, Logger, Message, OutgoingMessage, RetryInfo } from './types'

export {
  AbortProcessingError,
  BatchFailedError,
  HoldExpiredError,
  AdapterError,
  ClosedError,
  ConfigError,
  ERROR_CODES,
  FencedError,
  HarborError,
  SerializationError,
  ShutdownTimeoutError,
  TopicMissingError,
  describeError,
  isAbortProcessingError,
  isAdapterError,
  isBatchFailedError,
  isClosedError,
  isConfigError,
  isFencedError,
  isHoldExpiredError,
  isDeclaredRetryable,
  isHarborError,
  isRetryable,
  isSerializationError,
  isShutdownTimeoutError,
  isTopicMissingError
} from './errors'
export type { HarborErrorCode } from './errors'

export { jsonSerializer, rawSerializer, stringSerializer } from './serializer'
export type { Serializer, SyncSerializer } from './serializer'

export { MAX_DURATION_MS, parseDuration } from './duration'

export { OFFSET_PATTERN } from './commit'

export { partitionKey, splitPartitionKey } from './topic-partition'

export { DEFAULT_DURATION_BUCKETS, reportsOffsets } from './metrics'

export { murmur2, partitionForKey } from './partitioner'

export { defaultDlqTopicNaming, defaultRetryTopicNaming } from './retry-topics'
export type { DlqTopicNaming, RetryLevel, RetryTopicNaming } from './retry-topics'

// Adapter authoring surface. Everything an adapter (in-tree or external)
// needs is exported here: the contract and the header helpers, with the
// error identity the core expects.
export { decodeHeaders } from './headers'
export type { HeaderNames } from './headers'
export type {
  AdminApi,
  BrokerConfig,
  ClientAdapter,
  CommittedOffset,
  ConsumeOptions,
  ConsumerHandle,
  PartitionOffsets,
  RawHeaders,
  RawMessage,
  RawRecord,
  SaslConfig,
  TopicPartition,
  TopicPartitionOffset,
  TopicSpec,
  TransactionHandle
} from './adapter'

export { subscribe } from './events'
export type { EventMap, Listener, Listeners, Observable } from './events'

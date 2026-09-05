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
export type { HarborConfig, HarborEvents, HarborState, HeaderOptions } from './harbor'

export { Producer } from './producer'
export type { ProducerContext, ProducerOptions, ProducerRetryOptions } from './producer'

export { Consumer } from './consumer'
export type {
  ConsumerContext,
  ConsumerDlqOptions,
  ConsumerEvents,
  ConsumerOptions,
  ConsumerRetryOptions,
  ConsumerState,
  FailureOutcome,
  Handler,
  HandlerContext,
  StopReason,
  SubscribeOptions,
  TopicDefaults
} from './consumer'

export type { Clock, Duration, MessageHeaders, Logger, Message, OutgoingMessage, RetryInfo } from './types'

export {
  AbortProcessingError,
  AdapterError,
  ClosedError,
  ConfigError,
  ERROR_CODES,
  HarborError,
  SerializationError,
  ShutdownTimeoutError,
  TopicMissingError,
  describeError,
  isAbortProcessingError,
  isHarborError,
  isRetryable,
  isSerializationError,
  isShutdownTimeoutError,
  isTopicMissingError
} from './errors'
export type { HarborErrorCode } from './errors'

export { jsonSerializer, rawSerializer, stringSerializer } from './serializer'
export type { Serializer } from './serializer'

export { parseDuration } from './duration'

export { defaultDlqTopicNaming, defaultRetryTopicNaming } from './retry-topics'
export type { DlqTopicNaming, RetryLevel, RetryTopicNaming } from './retry-topics'

// Adapter authoring surface. Everything an adapter (in-tree or external)
// needs is exported here: the contract and the header helpers, with the
// error identity the core expects.
export { decodeHeaders, headerNames } from './headers'
export type { HeaderNames } from './headers'
export type {
  AdminApi,
  BrokerConfig,
  ClientAdapter,
  ConsumeOptions,
  ConsumerHandle,
  RawHeaders,
  RawMessage,
  RawRecord,
  SaslConfig,
  TopicPartition,
  TopicPartitionOffset,
  TopicSpec
} from './adapter'

export { systemClock } from './clock'

export type { EventMap, Listener, Observable } from './events'

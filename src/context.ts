import type { RetryPolicy } from 'breakwater'

import type { HeaderNames } from './headers'
import type { Clock, Logger } from './types'
import type { ClientAdapter } from './adapter'
import type { Serializer } from './serializer'
import type { Instrumentation } from './instrumentation'

/** The payload of the `error` event, the same for every pipeline that emits it. */
export interface HarborErrorEvent {
  error: unknown
  scope: 'consumer' | 'producer' | 'adapter'
  groupId?: string
  topic?: string
}

/**
 * What the harbor hands to the pipelines it runs (consumers, redrives): the
 * client, the conventions in effect and the harbor's own lifecycle. One
 * shape, built once, so a field added here reaches every pipeline.
 */
export interface CoreContext<E extends object> {
  readonly adapter: ClientAdapter
  readonly clientId: string
  readonly serializer: Serializer
  readonly headerNames: HeaderNames
  /** Generates a correlation id for a message that carries none. */
  readonly correlationId: () => string
  readonly logger: Logger
  readonly clock: Clock
  /** Retry of the produce calls the pipeline makes on its own behalf. */
  readonly producePolicy: RetryPolicy
  readonly emit: <K extends keyof E>(event: K, payload: E[K]) => void
  readonly isClosed: () => boolean
  readonly ensureConnected: () => Promise<void>
  /** The tracing hooks configured on the harbor, if any. */
  readonly instrumentation?: Instrumentation
}

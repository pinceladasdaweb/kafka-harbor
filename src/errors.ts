/**
 * Error taxonomy. Every error the library raises carries a stable `code`;
 * message text is documentation, never contract.
 *
 * Identity across module copies: an application may end up with the ESM and
 * the CJS build of this package in the same process (a dual-published
 * dependency that requires one while the app imports the other), and then
 * `instanceof HarborError` is false for errors thrown by the other copy. The
 * type guards below therefore check `code`, not the prototype chain, and
 * they are the supported way to branch on an error.
 */
export const ERROR_CODES = {
  /** The configuration passed to the library is invalid. */
  CONFIG_INVALID: 'CONFIG_INVALID',
  /** A value could not be serialized or deserialized faithfully. */
  SERIALIZATION: 'SERIALIZATION',
  /** The handler asked the consumer to stop without committing (`harbor.abort`). */
  ABORT_PROCESSING: 'ABORT_PROCESSING',
  /** A topic the flow depends on does not exist and auto-creation is off. */
  TOPIC_MISSING: 'TOPIC_MISSING',
  /** The client adapter reported a failure the library cannot classify further. */
  ADAPTER: 'ADAPTER',
  /** An operation was attempted on a harbor that is shutting down or closed. */
  CLOSED: 'CLOSED',
  /** Handlers were still running when the shutdown timeout elapsed. */
  SHUTDOWN_TIMEOUT: 'SHUTDOWN_TIMEOUT'
} as const

export type HarborErrorCode = (typeof ERROR_CODES)[keyof typeof ERROR_CODES]

export class HarborError extends Error {
  readonly code: HarborErrorCode
  /**
   * Whether retrying the failed operation could succeed. Read by the
   * consumer's default `retryIf` and understood by breakwater's retry policy,
   * so a handler that throws any error with `retryable: false` sends the
   * message straight to the DLQ without depending on class identity.
   */
  readonly retryable: boolean

  constructor (code: HarborErrorCode, message: string, options: ErrorOptions & { retryable?: boolean } = {}) {
    const { retryable, ...errorOptions } = options
    super(message, errorOptions)
    this.name = new.target.name
    this.code = code
    this.retryable = retryable ?? true
  }
}

export class ConfigError extends HarborError {
  constructor (message: string, options?: ErrorOptions) {
    super(ERROR_CODES.CONFIG_INVALID, message, { ...options, retryable: false })
  }
}

export class SerializationError extends HarborError {
  constructor (message: string, options?: ErrorOptions) {
    // A value that cannot be encoded will not encode on the next attempt
    // either: this is a deterministic failure, hence DLQ, never retry.
    super(ERROR_CODES.SERIALIZATION, message, { ...options, retryable: false })
  }
}

/**
 * Thrown by `harbor.abort(cause)` inside a handler. The consumer does NOT
 * commit the offset and stops fetching: the message will be redelivered
 * after a restart. Reserved for infrastructure bugs where reprocessing is
 * the correct outcome and neither retry topic nor DLQ is.
 */
export class AbortProcessingError extends HarborError {
  constructor (cause: unknown) {
    super(ERROR_CODES.ABORT_PROCESSING, 'handler aborted processing; the offset was not committed', { cause })
  }
}

export class TopicMissingError extends HarborError {
  readonly topic: string

  constructor (topic: string) {
    super(ERROR_CODES.TOPIC_MISSING, `topic "${topic}" does not exist; create it or enable autoCreateTopics`, { retryable: false })
    this.topic = topic
  }
}

export class AdapterError extends HarborError {
  constructor (message: string, options?: ErrorOptions & { retryable?: boolean }) {
    super(ERROR_CODES.ADAPTER, message, options)
  }
}

export class ClosedError extends HarborError {
  constructor (what: string) {
    super(ERROR_CODES.CLOSED, `${what} is closed`, { retryable: false })
  }
}

export class ShutdownTimeoutError extends HarborError {
  /** Handlers that were still running when the deadline passed. */
  readonly inFlight: number

  constructor (inFlight: number, timeoutMs: number) {
    super(ERROR_CODES.SHUTDOWN_TIMEOUT, `${inFlight} handler(s) still running after ${timeoutMs}ms; their offsets were not committed`)
    this.inFlight = inFlight
  }
}

const hasCode = (error: unknown, code: HarborErrorCode): boolean =>
  typeof error === 'object' && error !== null && (error as { code?: unknown }).code === code

export const isHarborError = (error: unknown): error is HarborError =>
  typeof error === 'object' && error !== null &&
  (Object.values(ERROR_CODES) as unknown[]).includes((error as { code?: unknown }).code)

export const isAbortProcessingError = (error: unknown): error is AbortProcessingError =>
  hasCode(error, ERROR_CODES.ABORT_PROCESSING)

export const isSerializationError = (error: unknown): error is SerializationError =>
  hasCode(error, ERROR_CODES.SERIALIZATION)

export const isTopicMissingError = (error: unknown): error is TopicMissingError =>
  hasCode(error, ERROR_CODES.TOPIC_MISSING)

export const isShutdownTimeoutError = (error: unknown): error is ShutdownTimeoutError =>
  hasCode(error, ERROR_CODES.SHUTDOWN_TIMEOUT)

/**
 * The default retry predicate: every error is retried unless it says
 * otherwise through `retryable: false`. Class identity is not consulted, so
 * the flag works for the library's own errors, for breakwater's, for the
 * RabbitMQ sibling's `RetryableError` convention and for any plain object.
 */
export const isRetryable = (error: unknown): boolean =>
  (error as { retryable?: unknown } | null | undefined)?.retryable !== false

/** The text stored in the `last-error` tracking header, bounded in size. */
export const describeError = (error: unknown, maxLength = 1024): string => {
  let text: string
  if (error instanceof Error) {
    text = error.name === 'Error' || error.message.startsWith(error.name) ? error.message : `${error.name}: ${error.message}`
  } else if (typeof error === 'string') {
    text = error
  } else {
    try {
      text = JSON.stringify(error) ?? String(error)
    } catch {
      text = String(error)
    }
  }
  return text.length > maxLength ? `${text.slice(0, maxLength - 3)}...` : text
}

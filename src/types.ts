/**
 * Message headers as the application sees them: always strings. The adapter
 * delivers the wire headers as the client exposes them (Buffer, string, or
 * an array of values for a repeated key) and the core decodes them, keeping
 * the LAST value of a repeated key. A value that is not valid UTF-8 still
 * arrives as a string, so a handler that needs binary data puts it in the
 * value with a custom serializer, never in a header.
 */
export type MessageHeaders = Record<string, string>

/**
 * Retry metadata carried by a message that reached the handler through a
 * retry topic. Read from the tracking headers and validated: a blank or
 * corrupt header never turns into `0` or `NaN`; the whole block is omitted
 * instead and the message is treated as a first delivery.
 */
export interface RetryInfo {
  /** How many times the handler has already failed for this message. */
  readonly count: number
  /** The topic the message was originally produced to. */
  readonly originalTopic: string
  /** When the first handler failure happened. */
  readonly firstFailureAt: Date
  /** The message of the most recent failure, truncated by the producer side. */
  readonly lastError: string
}

/** A message as the handler receives it: already deserialized. */
export interface Message<T = unknown> {
  readonly topic: string
  readonly partition: number
  /** Offsets are strings: they exceed 2^53 on long-lived topics. */
  readonly offset: string
  readonly key: string | null
  readonly value: T
  readonly headers: MessageHeaders
  readonly timestamp: Date
  /** Present only when the message came through a retry topic. */
  readonly retry?: RetryInfo
}

/** A message as the application sends it. */
export interface OutgoingMessage<T = unknown> {
  key?: string | null
  /** The payload, or `null` for a tombstone (a record with no value, which compaction reads as a delete). */
  value: T | null
  headers?: MessageHeaders
  /** Explicit partition. Rarely needed: the key is the normal routing path. */
  partition?: number
}

/**
 * The logger the library writes to. Structurally compatible with pino,
 * winston and console; `debug` is optional and only called when present.
 */
export interface Logger {
  debug?: (message: string, ...args: unknown[]) => void
  info: (message: string, ...args: unknown[]) => void
  warn: (message: string, ...args: unknown[]) => void
  error: (message: string, ...args: unknown[]) => void
}

/**
 * A duration: milliseconds as a number, or a string with a unit
 * (`'250ms'`, `'5s'`, `'1m'`, `'2h'`, `'1d'`). Parsed once, at construction.
 */
export type Duration = number | string

/**
 * Injectable time source. Production uses the wall clock; tests freeze it.
 * One sample per operation: two `now()` calls in the same statement are two
 * different truths.
 */
export interface Clock {
  now: () => number
  sleep: (ms: number, signal?: AbortSignal) => Promise<void>
}

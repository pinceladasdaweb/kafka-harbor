/**
 * A circuit breaker in front of a topic's handler, on breakwater's policy.
 * When the dependency behind a handler is down, failing every message down
 * the retry ladder fills the retry topics with work that will fail again;
 * the breaker holds the partition instead. A held delivery is neither
 * committed nor forwarded: the partition simply waits, up to `hold`, for
 * the circuit to let a probe through, and only a hold that runs out sends
 * the message down the ladder, with a retryable error, so it comes back
 * when the dependency may be up.
 */
import { circuitBreaker, type BreakerState, type CircuitBreakerOptions, type CircuitBreakerPolicy } from 'breakwater'

import { ConfigError, isAbortProcessingError, isBatchFailedError, isRetryable } from './errors'
import { parseDuration } from './duration'
import type { Duration } from './types'

export interface ConsumerBreakerOptions extends CircuitBreakerOptions {
  /**
   * The longest a message waits for the circuit to admit it, counted from
   * when it was received, retry delay included; then it fails with a
   * `CircuitOpenError` (retryable) and walks the ladder. Must fit under
   * `maxProcessingTime`, the longest a delivery may take anyway. Default:
   * what is left of `maxProcessingTime`.
   */
  hold?: Duration
  /**
   * A breaker built elsewhere (breakwater's `circuitBreaker()`), for a
   * circuit shared by several topics or with other code that calls the same
   * dependency. The other options are the breaker's own then, and it is
   * left open when the consumer stops.
   */
  policy?: CircuitBreakerPolicy
}

/** What the consumer keeps per guarded topic. */
export interface Breaker {
  readonly topic: string
  readonly policy: CircuitBreakerPolicy
  readonly holdMs: number | undefined
  /** Built here, so released here. */
  readonly owned: boolean
}

export interface CircuitStateChange {
  readonly from: BreakerState
  readonly to: BreakerState
}

/**
 * The codes breakwater rejects with when the function was NOT run: the
 * circuit is open, or isolated by hand. Compared by code, never by class,
 * because a process may hold two copies of breakwater.
 */
const REJECTION_CODES = new Set(['CIRCUIT_OPEN', 'CIRCUIT_ISOLATED'])

export const isCircuitRejection = (error: unknown): boolean =>
  typeof error === 'object' && error !== null && REJECTION_CODES.has(String((error as { code?: unknown }).code))

/**
 * What counts as the dependency failing, unless the options say otherwise:
 * a failure the handler declared deterministic (`retryable: false`) is the
 * message's fault, a `harbor.abort()` is a decision, not an outage, and a
 * batch handler that resolved for the rest of its batch (`BatchFailedError`)
 * did reach the dependency.
 */
export const defaultFailureIf = (error: unknown): boolean =>
  isRetryable(error) && !isAbortProcessingError(error) && !isBatchFailedError(error)

export function resolveBreaker (options: ConsumerBreakerOptions | false | undefined, groupId: string, topic: string, maxProcessingTimeMs: number): Breaker | undefined {
  if (options === undefined || options === false) return undefined
  const { hold, policy, ...circuit } = options
  const holdMs = hold === undefined ? undefined : parseDuration(hold, `breaker.hold for "${topic}"`)

  if (holdMs !== undefined && holdMs > maxProcessingTimeMs) {
    throw new ConfigError(`breaker.hold for "${topic}" (${holdMs}ms) exceeds maxProcessingTime (${maxProcessingTimeMs}ms): a delivery held longer than the client tolerates gets the member removed from the group`)
  }

  if (policy !== undefined) return { topic, policy, holdMs, owned: false }

  return {
    topic,
    policy: circuitBreaker({ name: `${groupId}:${topic}`, failureIf: defaultFailureIf, ...circuit }),
    holdMs,
    owned: true
  }
}

import type { RawHeaders } from './adapter'
import type { MessageHeaders, RetryInfo } from './types'

/**
 * The headers the library writes. Names are kebab-case with the `x-` prefix
 * the RabbitMQ sibling uses (`x-death-reason`, `x-original-exchange`), so an
 * application running both libraries sees one convention. The prefix is
 * configurable per harbor for shops with their own naming rules.
 */
export interface HeaderNames {
  /** The prefix every name below starts with. */
  readonly prefix: string
  /** Propagated end to end; generated when the producer has none. */
  readonly correlationId: string
  /** ISO-8601 instant the producer sent the message. */
  readonly producedAt: string
  /** The `clientId` of the producing application. */
  readonly producer: string
  /** Number of handler failures so far, as a decimal integer. */
  readonly retryCount: string
  /** The topic the message was originally produced to. */
  readonly originalTopic: string
  /** ISO-8601 instant of the first handler failure. */
  readonly firstFailureAt: string
  /** Description of the most recent failure, bounded in length. */
  readonly lastError: string
  /** ISO-8601 instant the message was sent to the dead-letter topic. */
  readonly deadLetteredAt: string
  /** The dead-letter topic a redriven message came back from. */
  readonly redrivenFrom: string
  /** ISO-8601 instant a message was redriven from the DLQ. */
  readonly redrivenAt: string
}

export function headerNames (prefix = 'x-'): HeaderNames {
  return {
    prefix,
    correlationId: `${prefix}correlation-id`,
    producedAt: `${prefix}produced-at`,
    producer: `${prefix}producer`,
    retryCount: `${prefix}retry-count`,
    originalTopic: `${prefix}original-topic`,
    firstFailureAt: `${prefix}first-failure-at`,
    lastError: `${prefix}last-error`,
    deadLetteredAt: `${prefix}dead-lettered-at`,
    redrivenFrom: `${prefix}redriven-from`,
    redrivenAt: `${prefix}redriven-at`
  }
}

/**
 * Wire headers to application headers. A repeated key keeps its LAST value:
 * the tracking headers are rewritten on every hop, and the newest hop is the
 * truth. Bytes are decoded as UTF-8; there is no binary header contract.
 */
export function decodeHeaders (raw: RawHeaders): MessageHeaders {
  const headers: MessageHeaders = {}
  for (const [name, value] of Object.entries(raw)) {
    const last = Array.isArray(value) ? value[value.length - 1] : value
    if (last === undefined) continue
    headers[name] = Buffer.isBuffer(last) ? last.toString('utf8') : last
  }
  return headers
}

const nonBlank = (value: string | undefined): string | undefined =>
  value !== undefined && value.trim() !== '' ? value : undefined

/**
 * `Number('')` and `Number(' ')` are 0, and 0 is a finite integer: checking
 * the number alone cannot tell "zero retries" from "blank header". The
 * string is validated before coercion.
 */
const parseCount = (value: string | undefined): number | undefined => {
  const text = value?.trim() ?? ''
  if (!/^\d{1,9}$/.test(text)) return undefined
  return Number(text)
}

const parseInstant = (value: string | undefined): Date | undefined => {
  const date = new Date(nonBlank(value) ?? Number.NaN)
  return Number.isNaN(date.getTime()) ? undefined : date
}

/**
 * Reads the retry metadata a previous hop wrote. Headers arrive from the
 * network and may have been produced by anything, so every field is
 * validated; when the block is inconsistent the whole thing is discarded and
 * the message counts as a first delivery. That is the safe failure: a
 * corrupt count must never send a message into an infinite retry loop nor
 * skip the DLQ.
 */
export function readRetryInfo (headers: MessageHeaders, names: HeaderNames): RetryInfo | undefined {
  const count = parseCount(headers[names.retryCount])
  const originalTopic = nonBlank(headers[names.originalTopic])
  const firstFailureAt = parseInstant(headers[names.firstFailureAt])
  if (count === undefined || count < 1 || originalTopic === undefined || firstFailureAt === undefined) {
    return undefined
  }
  // The last error is advisory: unreadable, it is omitted, the block survives.
  return { count, originalTopic, firstFailureAt, lastError: headers[names.lastError] ?? '' }
}

/**
 * The headers every record leaving this process carries: a correlation id
 * (kept when the message has a non-blank one, minted otherwise), the
 * instant it was produced and the producing application. The one writer for
 * the producer, the retry/DLQ hop and the redrive, so the three agree.
 */
export function stampProducer (headers: MessageHeaders, names: HeaderNames, stamp: { clientId: string, at: Date, correlationId: () => string }): MessageHeaders {
  return {
    ...headers,
    [names.correlationId]: nonBlank(headers[names.correlationId]) ?? stamp.correlationId(),
    [names.producedAt]: stamp.at.toISOString(),
    [names.producer]: stamp.clientId
  }
}

export interface TrackingInput {
  readonly previous: RetryInfo | undefined
  readonly originalTopic: string
  readonly error: string
  readonly now: Date
}

/** The tracking headers for the next hop, given what this hop learned. */
export function writeRetryInfo (headers: MessageHeaders, names: HeaderNames, input: TrackingInput): MessageHeaders {
  const count = (input.previous?.count ?? 0) + 1
  const firstFailureAt = input.previous?.firstFailureAt ?? input.now
  return {
    ...headers,
    [names.retryCount]: String(count),
    [names.originalTopic]: input.previous?.originalTopic ?? input.originalTopic,
    [names.firstFailureAt]: firstFailureAt.toISOString(),
    [names.lastError]: input.error
  }
}

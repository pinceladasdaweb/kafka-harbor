import { stampProducer } from './headers'
import type { CoreContext } from './context'
import type { RetryPolicy } from 'breakwater'
import type { MessageHeaders } from './types'
import type { RawMessage, RawRecord } from './adapter'
import { observe, wrapped, type ProduceBatch, type ProduceKind } from './instrumentation'

/** One produce call acknowledged by the broker. */
export interface ProducedEvent {
  topic: string
  /** What the batch was for: the application's send, a retry or DLQ hop, a redrive, or a transaction's send. */
  kind: ProduceKind
  /** How many records it carried. */
  records: number
  /** From the call to the acknowledgment, retries included. */
  durationMs: number
}

/** The event every pipeline that produces emits. */
export interface ProduceEvents {
  batchProduced: ProducedEvent
}

/**
 * The one way records leave a harbor: the application's `send()`, the
 * retry and DLQ hops of a consumer, and a redrive all come through here. The
 * batch runs through the instrumentation's `wrapProduce`, every record gets
 * the headers `onProduce` returns, the client is retried through `policy`
 * (the harbor's produce policy unless the caller has its own), and the
 * acknowledged batch is reported as `batchProduced`. A failure is thrown
 * to the caller, which knows what the batch was for and reports it
 * accordingly.
 *
 * A header the record already carries wins over the hook's (a trace context
 * the application set). A hop copies the headers of the message it
 * forwards; there the propagation headers the instrumentation declares are
 * the hook's, so the record carries the hop's own trace context and the
 * next attempt is its child, while every other copied header stays.
 */
export async function produceRecords<E extends ProduceEvents> (context: CoreContext<E>, batch: Omit<ProduceBatch, 'records'>, records: readonly RawRecord[], policy: RetryPolicy | null = context.producePolicy, produce: (records: readonly RawRecord[]) => Promise<void> = context.adapter.produce): Promise<void> {
  const { instrumentation, logger, clock } = context
  const onProduce = instrumentation?.onProduce
  const wrapProduce = instrumentation?.wrapProduce
  const startedAt = clock.now()
  const send = async (): Promise<void> => {
    // Inside the wrapper, so a context it set up (an active span) is what the hook injects from.
    // On a hop, the propagation headers are the hook's to write: the
    // forwarded message's own are dropped, whatever the hook returns. The
    // harbor's own tracking headers stay the hop's, whatever it declares.
    const tracking = new Set<string>(Object.values(context.headerNames))
    const declared = batch.origin === undefined ? undefined : observe(() => [...instrumentation?.propagationFields?.() ?? []], logger)
    const owned = new Set((declared ?? []).filter((name) => !tracking.has(name)))
    const outgoing = onProduce === undefined
      ? records
      : records.map((record) => {
        const added = textOnly(observe(() => onProduce(record), logger))
        // The copied headers win over the hook's, but for the fields it owns.
        const copied = Object.entries(record.headers).filter(([name]) => !owned.has(name))
        return { ...record, headers: { ...added, ...Object.fromEntries(copied) } }
      })
    // `null`: no retry, the caller's own ladder decides (a transaction aborts on the first failure).
    if (policy === null) await produce(outgoing)
    // A retry waiting out its backoff when the harbor releases the client fails at once, with ClosedError.
    else await policy.execute(async () => { await produce(outgoing) }, { signal: context.closed })
  }
  const full: ProduceBatch = { ...batch, records: records.length }
  await wrapped(wrapProduce === undefined ? undefined : (run) => wrapProduce(full, run), send, logger)
  context.emit('batchProduced', { topic: batch.topic, kind: batch.kind, records: records.length, durationMs: clock.now() - startedAt })
}

/** The string headers of what a hook returned: a header is text on the wire, and anything else would fail inside the client. */
const textOnly = (headers: MessageHeaders | undefined): MessageHeaders =>
  Object.fromEntries(Object.entries(headers ?? {}).filter((entry): entry is [string, string] => typeof entry[1] === 'string'))

/**
 * Forwards a consumed message to another topic: the ORIGINAL bytes (key and
 * value untouched) under `outgoing` headers stamped as produced now, and
 * resolves only after the broker acknowledged. The caller commits the
 * consumed offset after this resolves, never before: a produce that fails
 * leaves the message where it is. The consumed message, with the `headers`
 * it arrived with, is the batch's origin for the instrumentation. Returns
 * the record as it left, headers stamped.
 */
export async function produceHop<E extends ProduceEvents> (context: CoreContext<E>, raw: RawMessage, headers: MessageHeaders, topic: string, kind: Exclude<ProduceKind, 'send'>, outgoing: MessageHeaders, at = new Date(context.clock.now())): Promise<RawRecord> {
  const record: RawRecord = {
    topic,
    key: raw.key,
    value: raw.value,
    headers: stampProducer(outgoing, context.headerNames, { clientId: context.clientId, at, correlationId: context.correlationId })
  }
  const origin = { topic: raw.topic, partition: raw.partition, offset: raw.offset, headers }
  await produceRecords(context, { topic, kind, origin }, [record])
  return record
}

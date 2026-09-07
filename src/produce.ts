import { stampProducer } from './headers'
import type { CoreContext } from './context'
import type { RetryPolicy } from 'breakwater'
import type { MessageHeaders } from './types'
import type { RawMessage, RawRecord } from './adapter'
import { observe, wrapped, type ProduceBatch, type ProduceKind } from './instrumentation'

/** One produce call acknowledged by the broker. */
export interface ProducedEvent {
  topic: string
  /** What the batch was for: the application's send, a retry or DLQ hop, or a redrive. */
  kind: ProduceKind
  /** How many records it carried. */
  records: number
  /** From the call to the acknowledgment, retries included. */
  durationMs: number
}

/** The event every pipeline that produces emits. */
export interface ProduceEvents {
  messageProduced: ProducedEvent
}

/**
 * The one way records leave a harbor: the application's `send()`, the
 * retry and DLQ hops of a consumer, and a redrive all come through here. The
 * batch runs through the instrumentation's `wrapProduce`, every record gets
 * the headers `onProduce` returns (a header the record already carries
 * wins: a trace context set by the application, or copied from the message
 * a hop forwards), the client is retried through `policy` (the harbor's
 * produce policy unless the caller has its own), and the acknowledged batch
 * is reported as `messageProduced`. A failure is thrown to the caller, which
 * knows what the batch was for and reports it accordingly.
 */
export async function produceRecords<E extends ProduceEvents> (context: CoreContext<E>, batch: Omit<ProduceBatch, 'records'>, records: readonly RawRecord[], policy: RetryPolicy = context.producePolicy): Promise<void> {
  const { instrumentation, adapter, logger, clock } = context
  const onProduce = instrumentation?.onProduce
  const wrapProduce = instrumentation?.wrapProduce
  const startedAt = clock.now()
  const send = async (): Promise<void> => {
    // Inside the wrapper, so a context it set up (an active span) is what the hook injects from.
    const outgoing = onProduce === undefined
      ? records
      : records.map((record) => ({ ...record, headers: { ...observe(() => onProduce(record), logger), ...record.headers } }))
    await policy.execute(() => adapter.produce(outgoing))
  }
  const full: ProduceBatch = { ...batch, records: records.length }
  await wrapped(wrapProduce === undefined ? undefined : (run) => wrapProduce(full, run), send, logger)
  context.emit('messageProduced', { topic: batch.topic, kind: batch.kind, records: records.length, durationMs: clock.now() - startedAt })
}

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

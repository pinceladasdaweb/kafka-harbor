import { describeError } from './errors'
import type { HandlerContext } from './consumer'
import type { Logger, Message, MessageHeaders } from './types'

/**
 * What a batch of records is for: the application's own `send()`, the hop
 * of a failed message to a retry topic or to the DLQ, or a redrive back into
 * service.
 */
export type ProduceKind = 'send' | 'retry' | 'dead-letter' | 'redrive'

/** One produce call about to reach the client. */
export interface ProduceBatch {
  readonly topic: string
  readonly kind: ProduceKind
  /** How many records the batch carries. */
  readonly records: number
  /**
   * For a hop (`retry`, `dead-letter`, `redrive`): the consumed message the
   * batch forwards, with the headers it arrived with, so an instrumentation
   * can tie the hop to the trace that message carries. Absent for `send`.
   */
  readonly origin?: Pick<Message, 'topic' | 'partition' | 'offset' | 'headers'>
}

/** One record about to be produced, with the headers it carries so far. */
export interface OutgoingRecord {
  readonly topic: string
  readonly headers: MessageHeaders
}

/**
 * The hooks a tracing integration plugs into the pipelines. Every member is
 * optional. A hook that throws, rejects, or never runs the work it wraps does
 * not change the outcome of that work: the failure is logged and the work
 * runs (or its own outcome stands). Instrumentation observes; it never
 * decides.
 */
export interface Instrumentation {
  /**
   * Wraps one produce call, the application's `sendBatch()` as much as a
   * retry, DLQ or redrive hop: `run` produces the whole batch and resolves
   * once the broker acknowledged it. `onProduce` runs inside `run`, so an
   * async context set up here (an active span) is what it sees.
   */
  wrapProduce?: <T>(batch: ProduceBatch, run: () => Promise<T>) => Promise<T>
  /**
   * Called for every record right before it is produced. Whatever it
   * returns is added to the record's headers: the place for a trace context
   * that has to travel with the message. A header the record already
   * carries wins over the hook's, so a context set by the application, or
   * copied from the message a hop forwards, is never overwritten.
   */
  onProduce?: (record: OutgoingRecord) => MessageHeaders | undefined
  /**
   * Wraps one handler invocation. The message carries the headers the
   * producer side wrote, so a trace context extracted here parents the
   * handler's span to the producer's, however many retry hops lie between.
   */
  wrapHandler?: <T>(message: Message, context: HandlerContext, run: () => Promise<T>) => Promise<T>
}

/**
 * Runs `work` through `wrap`, holding the line that instrumentation never
 * changes an outcome. Three misbehaviours are contained: a wrapper that
 * throws before running the work (the work runs unwrapped), one that never
 * calls `run` (same), and one that fails after the work ran (the work's own
 * outcome is what the caller gets, the wrapper's failure is logged).
 */
export async function wrapped<T> (wrap: ((run: () => Promise<T>) => Promise<T>) | undefined, work: () => Promise<T>, logger: Logger): Promise<T> {
  if (wrap === undefined) return await work()
  let outcome: Promise<T> | undefined
  const run = (): Promise<T> => {
    if (outcome === undefined) {
      outcome = work()
      // The outcome is awaited below whatever the wrapper does with it; a
      // wrapper that parks it while awaiting something else must not leave
      // a rejection unobserved in the meantime.
      outcome.catch(() => undefined)
    }
    return outcome
  }
  let wrapperFailure: { error: unknown } | undefined
  try {
    await wrap(run)
  } catch (error) {
    wrapperFailure = { error }
  }
  if (outcome === undefined) {
    logger.error(`[kafka-harbor] instrumentation ${wrapperFailure === undefined ? 'did not run the work it wrapped' : `threw before running the work: ${describeError(wrapperFailure.error)}`}; running it unwrapped`)
    // Through run(), not work(): a wrapper that comes back late and calls
    // run() then finds the work already under way instead of starting it again.
    return await run()
  }
  // The work ran: its promise is the truth. A wrapper that rejected because
  // the work rejected propagates that; a wrapper that failed on its own
  // around work that succeeded is reported and the result stands.
  const value = await outcome
  if (wrapperFailure !== undefined) {
    logger.error(`[kafka-harbor] instrumentation threw after the work completed: ${describeError(wrapperFailure.error)}`)
  }
  return value
}

/** Calls an observer hook, containing whatever it throws. */
export function observe<T> (hook: () => T, logger: Logger): T | undefined {
  try {
    return hook()
  } catch (error) {
    logger.error(`[kafka-harbor] instrumentation threw: ${describeError(error)}`)
    return undefined
  }
}

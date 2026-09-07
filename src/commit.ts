import type { Logger } from './types'
import { describeError } from './errors'
import type { HarborErrorEvent } from './context'
import type { ConsumerHandle, RawMessage, TopicPartitionOffset } from './adapter'

/** An offset as the contract carries it: a non-negative decimal integer. */
export const OFFSET_PATTERN = /^\d+$/

/**
 * Records between two offsets of one partition, `to` not included. Offsets
 * are strings because they outgrow 2^53 on long-lived topics; a distance
 * between two of them does not, so it comes back as a number.
 */
export function offsetDistance (from: string, to: string): number {
  return Number(BigInt(to) - BigInt(from))
}

/** The offset to commit once `message` is done: the next one to read, as the contract spells it. */
export function offsetAfter (message: RawMessage): TopicPartitionOffset {
  return { topic: message.topic, partition: message.partition, offset: (BigInt(message.offset) + 1n).toString() }
}

export interface CommitReport {
  readonly groupId: string
  readonly logger: Logger
  readonly emit: (event: 'error', payload: HarborErrorEvent) => void
}

/**
 * Commits the offset after `message` and returns whether it landed. A commit
 * that fails (a rebalance in progress, a coordinator timeout) is reported
 * through `error` and the pipeline carries on: the work behind it is safe
 * (handler done, or the produce it followed acknowledged), and the only
 * consequence of the missing commit is a redelivery, which at-least-once
 * already allows. Every pipeline commits through here so they all make the
 * same call.
 */
export async function commitAfter (handle: ConsumerHandle, message: RawMessage, report: CommitReport): Promise<boolean> {
  try {
    await handle.commit([offsetAfter(message)])
    return true
  } catch (error) {
    report.logger.warn(`[kafka-harbor] commit failed for ${message.topic}[${message.partition}]@${message.offset}; the message will be redelivered: ${describeError(error)}`)
    report.emit('error', { error, scope: 'consumer', groupId: report.groupId, topic: message.topic })
    return false
  }
}

import { ConfigError } from './errors'
import type { Duration } from './types'
import { parseDuration } from './duration'

export interface RetryLevel {
  /** How long a message waits on this level before the handler runs again. */
  readonly delay: Duration
}

export type RetryTopicNaming = (topic: string, level: number) => string
export type DlqTopicNaming = (topic: string) => string

/**
 * Default topic names: `orders-retry-1`, `orders-retry-2`, `orders-dlq`.
 *
 * Hyphens, not dots: Kafka warns at topic creation that '.' and '_' collide
 * in metric names, and Spring Kafka's own defaults are `-retry` and `-dlt`.
 * The RabbitMQ sibling's `_dlq` suffix would trip the same metric warning.
 * Configurable per consumer for shops with their own naming rules.
 */
export const defaultRetryTopicNaming: RetryTopicNaming = (topic, level) => `${topic}-retry-${level}`
export const defaultDlqTopicNaming: DlqTopicNaming = (topic) => `${topic}-dlq`

export interface ResolvedRetryLevel {
  readonly level: number
  readonly delayMs: number
}

/**
 * Validates the retry ladder once, at construction. Every delay must be a
 * valid duration and no delay may exceed `maxDelayMs`: a retry topic
 * consumer waits the level's delay before running the handler, and a wait
 * longer than the poll interval would get the consumer kicked out of the
 * group. The error names the option the caller passed.
 */
export function resolveRetryLevels (levels: readonly RetryLevel[], maxDelayMs: number): ResolvedRetryLevel[] {
  return levels.map((level, index) => {
    const name = `retry.levels[${index}].delay`
    const delayMs = parseDuration(level.delay, name)
    if (delayMs > maxDelayMs) {
      throw new ConfigError(`${name} (${delayMs}ms) exceeds maxProcessingTime (${maxDelayMs}ms); a retry consumer waits the delay before the handler runs, and a wait longer than the poll interval leaves the group`)
    }
    return { level: index + 1, delayMs }
  })
}

/**
 * The topic map for one subscription: which retry topics belong to which
 * original topic, and which level each one is.
 */
export class TopicPlan {
  readonly original: string
  readonly retryTopics: readonly string[]
  readonly dlqTopic: string | undefined

  constructor (original: string, levels: number, retryNaming: RetryTopicNaming, dlqNaming: DlqTopicNaming | undefined) {
    this.original = original
    const retryTopics: string[] = []
    const taken = new Set([original])
    for (let level = 1; level <= levels; level++) {
      const topic = retryNaming(original, level)
      if (taken.has(topic)) {
        throw new ConfigError(`retry.topicNaming produced a duplicate topic name "${topic}" for "${original}" level ${level}`)
      }
      taken.add(topic)
      retryTopics.push(topic)
    }
    this.retryTopics = retryTopics
    if (dlqNaming === undefined) {
      this.dlqTopic = undefined
      return
    }
    const dlqTopic = dlqNaming(original)
    if (taken.has(dlqTopic)) {
      throw new ConfigError(`dlq.topicNaming produced a topic name "${dlqTopic}" that collides with the retry ladder of "${original}"`)
    }
    this.dlqTopic = dlqTopic
  }

  /** Every topic this plan consumes from: the original plus the retry ladder. */
  get consumedTopics (): string[] {
    return [this.original, ...this.retryTopics]
  }

  /** The retry topic for a given level, 1-based. */
  retryTopic (level: number): string | undefined {
    return this.retryTopics[level - 1]
  }
}

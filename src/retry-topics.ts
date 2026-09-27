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
 * Validates the retry ladder once, at construction: every delay must be a
 * valid duration. The error names the option the caller passed. Whether a
 * delay fits under the client's poll interval depends on the adapter (one
 * with pause/resume parks the message instead of waiting inside the
 * delivery), so that check belongs to `start()`.
 */
export function resolveRetryLevels (levels: readonly RetryLevel[]): ResolvedRetryLevel[] {
  return levels.map((level, index) => ({ level: index + 1, delayMs: parseDuration(level.delay, `retry.levels[${index}].delay`) }))
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

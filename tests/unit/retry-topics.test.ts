import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { ERROR_CODES, defaultDlqTopicNaming, defaultRetryTopicNaming } from '../../src/index'
import { TopicPlan, resolveRetryLevels } from '../../src/retry-topics'

describe('default topic naming', () => {
  test('uses hyphens: orders-retry-1, orders-dlq', () => {
    assert.equal(defaultRetryTopicNaming('orders', 1), 'orders-retry-1')
    assert.equal(defaultRetryTopicNaming('orders', 3), 'orders-retry-3')
    assert.equal(defaultDlqTopicNaming('orders'), 'orders-dlq')
  })
})

describe('resolveRetryLevels', () => {
  test('parses each delay and numbers the levels from 1', () => {
    assert.deepEqual(resolveRetryLevels([{ delay: '5s' }, { delay: 60_000 }], 300_000), [
      { level: 1, delayMs: 5_000 },
      { level: 2, delayMs: 60_000 }
    ])
  })

  test('an empty ladder is valid', () => {
    assert.deepEqual(resolveRetryLevels([], 300_000), [])
  })

  test('a delay above the processing bound is rejected naming the level', () => {
    assert.throws(() => resolveRetryLevels([{ delay: '1s' }, { delay: '10m' }], 300_000), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
      assert.match((error as Error).message, /retry\.levels\[1\]\.delay/)
      assert.match((error as Error).message, /maxProcessingTime/)
      return true
    })
  })

  test('a delay equal to the bound is accepted', () => {
    assert.equal(resolveRetryLevels([{ delay: 300_000 }], 300_000)[0]?.delayMs, 300_000)
  })
})

describe('TopicPlan', () => {
  test('lists the consumed topics and maps each back to its level', () => {
    const plan = new TopicPlan('orders', 2, defaultRetryTopicNaming, defaultDlqTopicNaming)
    assert.deepEqual(plan.consumedTopics, ['orders', 'orders-retry-1', 'orders-retry-2'])
    assert.equal(plan.dlqTopic, 'orders-dlq')
    assert.equal(plan.retryTopic(1), 'orders-retry-1')
    assert.equal(plan.retryTopic(2), 'orders-retry-2')
    assert.equal(plan.retryTopic(3), undefined)
  })

  test('without a DLQ, dlqTopic is undefined', () => {
    const plan = new TopicPlan('orders', 0, defaultRetryTopicNaming, undefined)
    assert.equal(plan.dlqTopic, undefined)
    assert.deepEqual(plan.consumedTopics, ['orders'])
  })

  test('a naming that collides with the original or with itself is rejected', () => {
    assert.throws(() => new TopicPlan('orders', 1, (topic) => topic, defaultDlqTopicNaming), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => new TopicPlan('orders', 2, (topic) => `${topic}-retry`, defaultDlqTopicNaming), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => new TopicPlan('orders', 1, defaultRetryTopicNaming, () => 'orders-retry-1'), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => new TopicPlan('orders', 0, defaultRetryTopicNaming, (topic) => topic), { code: ERROR_CODES.CONFIG_INVALID })
  })
})

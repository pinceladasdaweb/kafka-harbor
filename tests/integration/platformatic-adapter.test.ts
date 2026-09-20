/**
 * The platformatic adapter on a real broker: the same contract suite the
 * Confluent adapter passes, unmodified, and the harbor's own flow (retry
 * ladder and DLQ) running over it. The second adapter is what proves the
 * contract did not leak the client.
 */
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { after, before, describe, test } from 'node:test'

import { platformaticAdapter } from '../../src/adapters/platformatic/index'
import { createHarbor, type Harbor, type HarborEvents } from '../../src/index'
import { runAdapterContract } from '../../src/testing/index'
import { silentLogger } from '../helpers/harness'
import { startKafka, type KafkaFixture } from '../helpers/kafka'

let kafka: KafkaFixture | undefined

before(async () => {
  kafka = await startKafka()
})

after(async () => {
  await kafka?.stop()
})

const waitFor = async (condition: () => boolean, timeoutMs: number, what: string): Promise<void> => {
  const deadline = Date.now() + timeoutMs
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
}

/** Group timing short enough for the suite: the client's defaults are sized for production. */
const fastGroup = { consumer: { sessionTimeout: 10_000, heartbeatInterval: 1_000, rebalanceTimeout: 30_000 } }

describe('platformatic adapter against a real broker', () => {
  test('broker available', (t) => {
    if (kafka === undefined) t.skip('no Kafka container')
  })
})

runAdapterContract('platformatic', async () => {
  if (kafka === undefined) {
    throw Object.assign(new Error('Kafka container not available'), { skip: true })
  }
  const adapter = platformaticAdapter(fastGroup)
  await adapter.connect({ clientId: 'contract-platformatic', brokers: kafka.brokers })
  const run = randomUUID().slice(0, 8)
  return {
    adapter,
    topic: async (label, partitions = 1) => {
      const topic = `plt-${label}-${run}`
      await adapter.admin.createTopics([{ topic, partitions, replicationFactor: 1 }])
      return topic
    },
    group: (label) => `plt-${label}-${run}`,
    timeoutMs: 60_000,
    teardown: async () => { await adapter.disconnect() }
  }
})

describe('the harbor over the platformatic adapter', () => {
  test('a failing message walks the retry ladder into the DLQ, and the good one is processed', async (t) => {
    if (kafka === undefined) return t.skip('no Kafka container')
    const run = randomUUID().slice(0, 8)
    const topic = `plt-orders-${run}`
    const adapter = platformaticAdapter(fastGroup)
    const harbor: Harbor = createHarbor({ clientId: `plt-flow-${run}`, brokers: kafka.brokers, adapter, logger: silentLogger })
    const events: Array<{ type: keyof HarborEvents, payload: unknown }> = []
    for (const type of ['messageProcessed', 'messageRetried', 'messageDeadLettered', 'error'] as const) {
      harbor.on(type, (payload) => { events.push({ type, payload }) })
    }
    try {
      await harbor.connect()
      await adapter.admin.createTopics([{ topic, partitions: 2, replicationFactor: 1 }])

      const attempts: Array<{ topic: string, attempt: number }> = []
      const consumer = harbor.consumer({
        groupId: `plt-workers-${run}`,
        fromBeginning: true,
        autoCreateTopics: true,
        retry: { levels: [{ delay: '500ms' }] }
      })
      consumer.subscribe<{ id: string }>(topic, (message, ctx) => {
        if (message.value.id === 'ok') return
        attempts.push({ topic: message.topic, attempt: ctx.attempt })
        throw new Error(`attempt ${ctx.attempt} failed`)
      })
      await consumer.start()

      const producer = harbor.producer<{ id: string }>()
      await producer.send(topic, { key: 'bad', value: { id: 'bad' }, headers: { 'x-tenant': 'acme' } })
      await producer.send(topic, { key: 'good', value: { id: 'ok' } })

      await waitFor(() => events.some((e) => e.type === 'messageDeadLettered'), 90_000, 'dead-letter event')
      await waitFor(() => events.some((e) => e.type === 'messageProcessed'), 30_000, 'the good message')

      assert.deepEqual(attempts.map((a) => a.attempt), [1, 2])
      assert.deepEqual(attempts.map((a) => a.topic), [topic, `${topic}-retry-1`])
      assert.equal(events.filter((e) => e.type === 'messageRetried').length, 1)
      assert.equal(events.filter((e) => e.type === 'error').length, 0)

      // Lag reads through the adapter's offsets: everything consumed, nothing behind.
      const lag = await harbor.lag()
      assert.ok(lag.length >= 2, 'lag reported per partition')
      assert.equal(lag.reduce((sum, entry) => sum + entry.lag, 0), 0)
    } finally {
      await harbor.shutdown('20s')
    }
    assert.equal(harbor.status, 'closed')
  })
})

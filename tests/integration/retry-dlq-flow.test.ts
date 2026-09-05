/**
 * The acceptance flow on a real broker: produce, consume, fail,
 * walk the retry ladder, dead-letter, and shut down cleanly, all through the
 * public API with the Confluent adapter.
 */
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { after, before, describe, test } from 'node:test'

import { confluentAdapter } from '../../src/adapters/confluent/index'
import { createHarbor, type Harbor, type HarborEvents, type Message } from '../../src/index'
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

/** A harbor on the shared broker, with its own adapter so the test can create topics. */
const openHarbor = (clientId: string) => {
  const adapter = confluentAdapter()
  const harbor = createHarbor({
    clientId,
    brokers: kafka!.brokers,
    adapter,
    logger: silentLogger,
    headers: { correlationId: () => `corr-${clientId}` }
  })
  return { harbor, adapter }
}

/** Shuts every harbor down even when the test failed, so a failure never leaves the process alive. */
const withHarbors = async (harbors: Harbor[], run: () => Promise<void>): Promise<void> => {
  try {
    await run()
  } finally {
    await Promise.allSettled(harbors.map((harbor) => harbor.shutdown('20s')))
  }
}

describe('retry topics and DLQ on a real broker', () => {
  test('a failing message walks the ladder, honors the delays and lands in the DLQ with its trail', async (t) => {
    if (kafka === undefined) return t.skip('no Kafka container')
    const run = randomUUID().slice(0, 8)
    const topic = `orders-${run}`
    const { harbor, adapter } = openHarbor(`flow-${run}`)
    const inspector = openHarbor(`inspect-${run}`).harbor

    await withHarbors([harbor, inspector], async () => {
      const events: Array<{ type: keyof HarborEvents, payload: unknown }> = []
      for (const type of ['messageProcessed', 'messageRetried', 'messageDeadLettered', 'messageFailed', 'error'] as const) {
        harbor.on(type, (payload) => { events.push({ type, payload }) })
      }

      await harbor.connect()
      await adapter.admin.createTopics([{ topic, partitions: 1, replicationFactor: 1 }])

      const attempts: Array<{ topic: string, attempt: number, at: number, retry: Message['retry'] }> = []
      const consumer = harbor.consumer({
        groupId: `workers-${run}`,
        fromBeginning: true,
        autoCreateTopics: true,
        retry: { levels: [{ delay: '1s' }, { delay: '2s' }] }
      })
      consumer.subscribe<{ id: string }>(topic, (message, ctx) => {
        if (message.value.id === 'ok') return
        attempts.push({ topic: message.topic, attempt: ctx.attempt, at: Date.now(), retry: message.retry })
        throw new Error(`attempt ${ctx.attempt} failed`)
      })
      await consumer.start()

      const producer = harbor.producer<{ id: string }>()
      await producer.send(topic, { key: 'bad', value: { id: 'bad' }, headers: { 'x-tenant': 'acme' } })
      await producer.send(topic, { key: 'good', value: { id: 'ok' } })

      await waitFor(() => events.some((e) => e.type === 'messageDeadLettered'), 90_000, 'dead-letter event')

      assert.equal(attempts.length, 3)
      assert.deepEqual(attempts.map((a) => a.attempt), [1, 2, 3])
      assert.deepEqual(attempts.map((a) => a.topic), [topic, `${topic}-retry-1`, `${topic}-retry-2`])
      assert.equal(attempts[1]?.retry?.count, 1)
      assert.equal(attempts[2]?.retry?.count, 2)
      assert.equal(attempts[1]?.retry?.originalTopic, topic)
      // The delays were honored: at least 1s before level 1, at least 2s before level 2.
      assert.ok((attempts[1]?.at ?? 0) - (attempts[0]?.at ?? 0) >= 950, 'level 1 waited about 1s')
      assert.ok((attempts[2]?.at ?? 0) - (attempts[1]?.at ?? 0) >= 1_950, 'level 2 waited about 2s')

      // The DLQ holds the original bytes and the full trail.
      const dlq: Message[] = []
      const dlqConsumer = inspector.consumer({ groupId: `inspect-${run}`, fromBeginning: true, dlq: { enabled: false } })
      dlqConsumer.subscribe(`${topic}-dlq`, (message) => { dlq.push(message) })
      await dlqConsumer.start()
      await waitFor(() => dlq.length === 1, 90_000, 'DLQ message')
      const dead = dlq[0]!
      assert.deepEqual(dead.value, { id: 'bad' })
      assert.equal(dead.key, 'bad')
      assert.equal(dead.headers['x-tenant'], 'acme')
      assert.equal(dead.headers['x-correlation-id'], `corr-flow-${run}`)
      assert.equal(dead.headers['x-retry-count'], '3')
      assert.equal(dead.headers['x-original-topic'], topic)
      assert.equal(dead.headers['x-last-error'], 'attempt 3 failed')
      assert.ok(dead.headers['x-first-failure-at'])
      assert.ok(dead.headers['x-dead-lettered-at'])

      // The good message was processed and nothing was reported as an error.
      assert.ok(events.some((e) => e.type === 'messageProcessed'))
      assert.equal(events.filter((e) => e.type === 'error').length, 0)
      assert.equal(events.filter((e) => e.type === 'messageRetried').length, 2)
    })
    assert.equal(harbor.status, 'closed')
  })

  test('shutdown mid-handler commits what finished and redelivers what did not', async (t) => {
    if (kafka === undefined) return t.skip('no Kafka container')
    const run = randomUUID().slice(0, 8)
    const topic = `shutdown-${run}`
    const groupId = `shutdown-${run}`

    const first = openHarbor(`sd1-${run}`)
    const second = openHarbor(`sd2-${run}`)
    await withHarbors([first.harbor, second.harbor], async () => {
      await first.harbor.connect()
      await first.adapter.admin.createTopics([{ topic, partitions: 1, replicationFactor: 1 }])

      const seen: string[] = []
      let release!: () => void
      const gate = new Promise<void>((resolve) => { release = resolve })
      const consumer = first.harbor.consumer({ groupId, fromBeginning: true, autoCreateTopics: true })
      consumer.subscribe<string>(topic, async (message) => {
        seen.push(message.value)
        if (message.value === 'slow') await gate
      })
      await first.harbor.producer<string>().sendBatch(topic, [{ value: 'fast' }, { value: 'slow' }, { value: 'later' }])
      await consumer.start()
      await waitFor(() => seen.includes('slow'), 60_000, 'slow message in flight')

      const closing = first.harbor.shutdown('20s')
      await new Promise((resolve) => setTimeout(resolve, 300))
      release()
      await closing
      assert.deepEqual(seen, ['fast', 'slow'])

      // A new member of the group resumes after the committed offsets.
      const resumed: string[] = []
      const again = second.harbor.consumer({ groupId, fromBeginning: true, autoCreateTopics: true })
      again.subscribe<string>(topic, (message) => { resumed.push(message.value) })
      await again.start()
      await waitFor(() => resumed.length === 1, 60_000, 'redelivery of the unprocessed message')
      assert.deepEqual(resumed, ['later'])
    })
  })
})

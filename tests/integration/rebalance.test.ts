/**
 * Basic rebalancing on a real broker: a second member joins the group while
 * the first is processing, partitions move, and every message is still
 * processed at least once with nothing lost. Then the redrive utility is
 * exercised end to end against real dead letters.
 */
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { after, before, describe, test } from 'node:test'

import { confluentAdapter } from '../../src/adapters/confluent/index'
import { createHarbor, type Harbor } from '../../src/index'
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

const openHarbor = (clientId: string) => {
  const adapter = confluentAdapter()
  const harbor = createHarbor({ clientId, brokers: kafka!.brokers, adapter, logger: silentLogger })
  return { harbor, adapter }
}

const withHarbors = async (harbors: Harbor[], run: () => Promise<void>): Promise<void> => {
  try {
    await run()
  } finally {
    await Promise.allSettled(harbors.map((harbor) => harbor.shutdown('20s')))
  }
}

describe('rebalancing and redrive on a real broker', () => {
  test('a member joining mid-stream takes over partitions and every message is processed at least once', async (t) => {
    if (kafka === undefined) return t.skip('no Kafka container')
    const run = randomUUID().slice(0, 8)
    const topic = `rebalance-${run}`
    const groupId = `rebalance-${run}`
    const first = openHarbor(`rb1-${run}`)
    const second = openHarbor(`rb2-${run}`)

    await withHarbors([first.harbor, second.harbor], async () => {
      await first.harbor.connect()
      await first.adapter.admin.createTopics([{ topic, partitions: 4, replicationFactor: 1 }])

      const seenBy = { first: new Set<string>(), second: new Set<string>() }
      const handler = (member: keyof typeof seenBy) => async (message: { value: string }) => {
        // Slow enough that the second member joins while the first still has work.
        await new Promise((resolve) => setTimeout(resolve, 40))
        seenBy[member].add(message.value)
      }
      const consumerA = first.harbor.consumer({ groupId, fromBeginning: true, autoCreateTopics: true, concurrency: 4 })
      consumerA.subscribe<string>(topic, handler('first'))
      await consumerA.start()

      // Enough work that joining the group (a few seconds) lands mid-stream:
      // 400 messages over 4 partitions at 40ms each is about 4s per partition.
      const total = 400
      const values = Array.from({ length: total }, (_, i) => `m${i}`)
      await first.harbor.producer<string>().sendBatch(topic, values.map((value, i) => ({ key: `k${i % 8}`, value })))
      await waitFor(() => seenBy.first.size >= 8, 60_000, 'the first member to get going')

      const consumerB = second.harbor.consumer({ groupId, fromBeginning: true, autoCreateTopics: true, concurrency: 4 })
      consumerB.subscribe<string>(topic, handler('second'))
      await consumerB.start()

      await waitFor(() => new Set([...seenBy.first, ...seenBy.second]).size === total, 90_000, 'every message to be processed')
      // A little grace for in-flight redeliveries, then the union must be complete and both members must have worked.
      await new Promise((resolve) => setTimeout(resolve, 500))
      const union = new Set([...seenBy.first, ...seenBy.second])
      assert.equal(union.size, total)
      assert.ok(seenBy.second.size > 0, 'the joining member took over some partitions')
      assert.equal(first.harbor.isHealthy(), true)
      assert.equal(second.harbor.isHealthy(), true)
    })
  })

  test('dead letters are redriven into the original topic and processed again', async (t) => {
    if (kafka === undefined) return t.skip('no Kafka container')
    const run = randomUUID().slice(0, 8)
    const topic = `redrive-${run}`
    const { harbor, adapter } = openHarbor(`rd-${run}`)

    await withHarbors([harbor], async () => {
      await harbor.connect()
      await adapter.admin.createTopics([{ topic, partitions: 1, replicationFactor: 1 }])

      let failing = true
      const processed: string[] = []
      const deadLettered: string[] = []
      harbor.on('messageDeadLettered', ({ offset }) => { deadLettered.push(offset) })
      const consumer = harbor.consumer({ groupId: `workers-${run}`, fromBeginning: true, autoCreateTopics: true })
      consumer.subscribe<{ id: string }>(topic, (message) => {
        if (failing) throw new Error('downstream is down')
        processed.push(message.value.id)
      })
      await consumer.start()
      await harbor.producer<{ id: string }>().sendBatch(topic, [{ key: 'a', value: { id: 'a' } }, { key: 'b', value: { id: 'b' } }])
      await waitFor(() => deadLettered.length === 2, 60_000, 'both messages in the DLQ')

      // Downstream is back: drain the DLQ into the original topic.
      failing = false
      const result = await harbor.redrive({ from: `${topic}-dlq`, idleTimeout: '3s' })
      assert.deepEqual(result, { from: `${topic}-dlq`, reprocessed: 2, skipped: 0 })
      await waitFor(() => processed.length === 2, 60_000, 'the redriven messages to be processed')
      assert.deepEqual(processed.sort(), ['a', 'b'])
      assert.equal(deadLettered.length, 2, 'nothing new was dead-lettered')

      // A second redrive finds the DLQ drained.
      const again = await harbor.redrive({ from: `${topic}-dlq`, idleTimeout: '2s' })
      assert.deepEqual(again, { from: `${topic}-dlq`, reprocessed: 0, skipped: 0 })
    })
  })
})

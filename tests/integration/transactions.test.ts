/**
 * Transactions on a real broker, once per adapter: what a transaction
 * produced is invisible to a read-committed consumer until the commit and
 * gone after an abort, and a handler's transaction commits the consumed
 * offset along with what it produced.
 */
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { after, before, describe, test } from 'node:test'

import { confluentAdapter } from '../../src/adapters/confluent/index'
import { platformaticAdapter } from '../../src/adapters/platformatic/index'
import { createHarbor, type ClientAdapter, type HarborEvents } from '../../src/index'
import { silentLogger } from '../helpers/harness'
import { startKafka, type KafkaFixture } from '../helpers/kafka'

let kafka: KafkaFixture | undefined

before(async () => {
  kafka = await startKafka()
})

after(async () => {
  await kafka?.stop()
})

const waitFor = async (condition: () => boolean, timeoutMs: number, what: string, poll: () => Promise<void> = async () => {}): Promise<void> => {
  const deadline = Date.now() + timeoutMs
  await poll()
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
    await new Promise((resolve) => setTimeout(resolve, 200))
    await poll()
  }
}

/** The first transactional init on a fresh broker creates the coordinator's log, which can outlast one client timeout. */
const connectWithPatience = async (harbor: { connect: () => Promise<void> }): Promise<void> => {
  try {
    await harbor.connect()
  } catch {
    await harbor.connect()
  }
}

const suite = (name: string, build: () => ClientAdapter): Promise<void> => describe(`transactions on a real broker (${name})`, () => {
  test('what a transaction produced shows up all at once on commit and never after an abort', async (t) => {
    if (kafka === undefined) return t.skip('no Kafka container')
    const run = randomUUID().slice(0, 8)
    const topic = `tx-${run}`
    const adapter = build()
    const harbor = createHarbor({ clientId: `tx-${run}`, brokers: kafka.brokers, adapter, logger: silentLogger, transactionalId: `tx-${run}` })
    const completed: Array<HarborEvents['transactionCompleted']> = []
    harbor.on('transactionCompleted', (event) => { completed.push(event) })
    try {
      await connectWithPatience(harbor)
      await adapter.admin.createTopics([{ topic, partitions: 1, replicationFactor: 1 }])
      const seen: unknown[] = []
      const consumer = harbor.consumer({ groupId: `tx-readers-${run}`, fromBeginning: true, autoCreateTopics: true })
      consumer.subscribe(topic, (message) => { seen.push(message.value) })
      await consumer.start()

      await assert.rejects(harbor.transaction(async (tx) => {
        await tx.send(topic, { value: 'aborted' })
        throw new Error('changed my mind')
      }), /changed my mind/)
      await harbor.transaction(async (tx) => {
        await tx.sendBatch(topic, [{ value: 'first' }, { value: 'second' }])
      })
      await waitFor(() => seen.length === 2, 30_000, 'the committed records')
      await new Promise((resolve) => setTimeout(resolve, 1_000))
      assert.deepEqual(seen, ['first', 'second'], 'the aborted record never surfaces')
      assert.deepEqual(completed.map((event) => [event.outcome, event.records]), [['aborted', 1], ['committed', 2]])
    } finally {
      await harbor.shutdown('20s')
    }
  })

  test('a handler transaction commits the consumed offset along with what it produced', async (t) => {
    if (kafka === undefined) return t.skip('no Kafka container')
    const run = randomUUID().slice(0, 8)
    const input = `orders-${run}`
    const output = `shipments-${run}`
    const groupId = `tx-workers-${run}`
    const adapter = build()
    const harbor = createHarbor({ clientId: `txh-${run}`, brokers: kafka.brokers, adapter, logger: silentLogger, transactionalId: `txh-${run}` })
    try {
      await connectWithPatience(harbor)
      await adapter.admin.createTopics([{ topic: input, partitions: 1, replicationFactor: 1 }, { topic: output, partitions: 1, replicationFactor: 1 }])
      const shipped: unknown[] = []
      const reader = harbor.consumer({ groupId: `tx-readers-${run}`, fromBeginning: true, autoCreateTopics: true })
      reader.subscribe(output, (message) => { shipped.push(message.value) })
      await reader.start()
      const worker = harbor.consumer({ groupId, fromBeginning: true, autoCreateTopics: true })
      worker.subscribe<{ id: string }>(input, async (message, ctx) => {
        await ctx.transaction(async (tx) => {
          await tx.send(output, { key: message.value.id, value: { shipped: message.value.id } })
        })
      })
      await worker.start()
      await harbor.producer<{ id: string }>().sendBatch(input, [{ value: { id: 'a' } }, { value: { id: 'b' } }])
      await waitFor(() => shipped.length === 2, 30_000, 'the shipments')
      assert.deepEqual(shipped, [{ shipped: 'a' }, { shipped: 'b' }])
      // The offsets land with the transaction (the consumer's own commit repeats them): both markers may take a moment.
      let committed: string | null | undefined
      await waitFor(() => committed === '2', 30_000, 'the committed offset', async () => {
        committed = (await adapter.admin.fetchCommittedOffsets?.(groupId, [input]))?.find((entry) => entry.partition === 0)?.offset
      })
    } finally {
      await harbor.shutdown('20s')
    }
  })
})

suite('confluent', () => confluentAdapter({ consumer: { 'session.timeout.ms': 10_000, 'heartbeat.interval.ms': 3_000 } }))
suite('platformatic', () => platformaticAdapter({ consumer: { sessionTimeout: 10_000, heartbeatInterval: 1_000, rebalanceTimeout: 30_000 } }))

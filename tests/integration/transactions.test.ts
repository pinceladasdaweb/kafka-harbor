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
import { createHarbor, type ClientAdapter, type Harbor, type HarborEvents } from '../../src/index'
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

const suite = (name: string, build: (transactionTimeoutMs?: number) => ClientAdapter): Promise<void> => describe(`transactions on a real broker (${name})`, () => {
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

  test('a transaction the broker aborted on timeout fails retryably, not as fenced, and the next one runs', async (t) => {
    if (kafka === undefined) return t.skip('no Kafka container')
    const run = randomUUID().slice(0, 8)
    const topic = `tx-timeout-${run}`
    const harbor = createHarbor({ clientId: `tx-timeout-${run}`, brokers: kafka.brokers, adapter: build(3_000), logger: silentLogger, transactionalId: `tx-timeout-${run}` })
    const readers = createHarbor({ clientId: `tx-timeout-readers-${run}`, brokers: kafka.brokers, adapter: build(), logger: silentLogger })
    try {
      await connectWithPatience(harbor)
      await harbor.config.adapter.admin.createTopics([{ topic, partitions: 1, replicationFactor: 1 }])
      await assert.rejects(harbor.transaction(async (tx) => {
        await tx.send(topic, { value: 'outlived its timeout' })
        await new Promise((resolve) => setTimeout(resolve, 8_000))
      }), (error: unknown) => {
        assert.notEqual((error as { code?: unknown }).code, 'FENCED', String(error))
        assert.equal((error as { retryable?: unknown }).retryable, true, String(error))
        return true
      })
      await harbor.transaction(async (tx) => { await tx.send(topic, { value: 'after the timeout' }) })
      const seen: unknown[] = []
      const consumer = readers.consumer({ groupId: `tx-timeout-readers-${run}`, fromBeginning: true, autoCreateTopics: true })
      consumer.subscribe(topic, (message) => { seen.push(message.value) })
      await consumer.start()
      await waitFor(() => seen.includes('after the timeout'), 30_000, 'the transaction after the timeout')
      assert.ok(!seen.includes('outlived its timeout'), 'what the aborted transaction produced never shows')
    } finally {
      await readers.shutdown('20s')
      await harbor.shutdown('20s')
    }
  })

  test('a process that restarts with its transactionalId takes it back, and fences an instance still alive under it', async (t) => {
    if (kafka === undefined) return t.skip('no Kafka container')
    const run = randomUUID().slice(0, 8)
    const topic = `tx-reuse-${run}`
    const transactionalId = `tx-reuse-${run}`
    const open = async (): Promise<Harbor> => {
      const harbor = createHarbor({ clientId: `tx-reuse-${run}`, brokers: kafka!.brokers, adapter: build(), logger: silentLogger, transactionalId })
      await connectWithPatience(harbor)
      return harbor
    }
    const first = await open()
    const second = await open().catch(async (error: unknown) => { await first.shutdown('20s'); throw error })
    // A reader of its own, without the id: initializing it would fence the second instance in turn.
    const readers = createHarbor({ clientId: `tx-reuse-readers-${run}`, brokers: kafka.brokers, adapter: build(), logger: silentLogger })
    try {
      await first.config.adapter.admin.createTopics([{ topic, partitions: 1, replicationFactor: 1 }])
      await first.transaction(async (tx) => { await tx.send(topic, { value: 'before the restart' }) }).catch(() => undefined)
      // The second instance initializes the same id (at connect on Confluent, at its first transaction on platformatic): it fences the first, which is the zombie now.
      await second.transaction(async (tx) => { await tx.send(topic, { value: 'after the restart' }) })
      // Fenced before any timeout could explain it: the zombie runs no more transactions, and opens no producer to fence back.
      await assert.rejects(first.transaction(async (tx) => { await tx.send(topic, { value: 'from the zombie' }) }), { code: 'FENCED' })
      await assert.rejects(first.transaction(async (tx) => { await tx.send(topic, { value: 'from the zombie again' }) }), { code: 'FENCED' })
      await second.transaction(async (tx) => { await tx.send(topic, { value: 'still going' }) })
      const seen: unknown[] = []
      const consumer = readers.consumer({ groupId: `tx-reuse-readers-${run}`, fromBeginning: true, autoCreateTopics: true })
      consumer.subscribe(topic, (message) => { seen.push(message.value) })
      await consumer.start()
      await waitFor(() => seen.includes('still going'), 30_000, 'the restarted instance\'s records')
      await new Promise((resolve) => setTimeout(resolve, 1_000))
      assert.ok(seen.includes('after the restart'))
      assert.ok(!seen.includes('from the zombie') && !seen.includes('from the zombie again'), 'nothing of the fenced instance after the fencing')
    } finally {
      await readers.shutdown('20s')
      await second.shutdown('20s')
      await first.shutdown('20s').catch(() => undefined)
    }
  })
})

suite('confluent', (transactionTimeoutMs) => confluentAdapter({
  consumer: { 'session.timeout.ms': 10_000, 'heartbeat.interval.ms': 3_000 },
  // librdkafka wants socket.timeout.ms at most the transaction timeout plus 100ms.
  ...(transactionTimeoutMs !== undefined && { producer: { 'transaction.timeout.ms': transactionTimeoutMs, 'socket.timeout.ms': transactionTimeoutMs } })
}))
suite('platformatic', (transactionTimeoutMs) => platformaticAdapter({
  consumer: { sessionTimeout: 10_000, heartbeatInterval: 1_000, rebalanceTimeout: 30_000 },
  ...(transactionTimeoutMs !== undefined && { transactionTimeout: transactionTimeoutMs })
}))

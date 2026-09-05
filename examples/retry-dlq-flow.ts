/**
 * The whole flow against a real broker: produce, fail, walk the retry
 * ladder, land in the DLQ, then redrive the DLQ back and succeed.
 *
 *   docker compose up -d
 *   node --import tsx examples/retry-dlq-flow.ts
 *
 * KAFKA_BROKERS overrides the bootstrap servers (default: localhost:9092,
 * where docker-compose.yml exposes the broker).
 */
import { randomUUID } from 'node:crypto'

import { confluentAdapter } from '../src/adapters/confluent/index'
import { createHarbor } from '../src/index'

interface Order { id: string }

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',')
const run = randomUUID().slice(0, 8)
const topic = `orders-${run}`
const adapter = confluentAdapter()
const harbor = createHarbor({ clientId: `example-${run}`, brokers, adapter })

const log = (line: string): void => { console.log(`[${new Date().toISOString().slice(11, 23)}] ${line}`) }

harbor
  .on('messageRetried', ({ offset, retryTopic, attempt }) => { log(`offset ${offset}: attempt ${attempt} failed, sent to ${retryTopic}`) })
  .on('messageDeadLettered', ({ offset, dlqTopic, attempts }) => { log(`offset ${offset}: dead-lettered to ${dlqTopic} after ${attempts} attempts`) })
  .on('messageRedriven', ({ from, to }) => { log(`redriven from ${from} back to ${to}`) })
  .on('error', ({ error, scope }) => { log(`error (${scope}): ${(error as Error).message}`) })

let downstreamUp = false
const processed: string[] = []

async function main (): Promise<void> {
  await harbor.connect()
  await adapter.admin.createTopics([{ topic, partitions: 1, replicationFactor: 1 }])

  const consumer = harbor.consumer({
    groupId: `workers-${run}`,
    fromBeginning: true,
    autoCreateTopics: true,
    retry: { levels: [{ delay: '1s' }, { delay: '2s' }] }
  })
  consumer.subscribe<Order>(topic, (message, ctx) => {
    if (!downstreamUp) throw new Error(`downstream is down (attempt ${ctx.attempt})`)
    processed.push(message.value.id)
    log(`order ${message.value.id} processed on attempt ${ctx.attempt}`)
  })
  await consumer.start()

  log(`producing to ${topic} while downstream is down`)
  await harbor.producer<Order>().send(topic, { key: 'o1', value: { id: 'o1' } })
  await waitFor(() => harbor.health().healthy && deadLettered(), 60_000, 'the message to reach the DLQ')

  log('downstream is back; redriving the DLQ')
  downstreamUp = true
  const result = await harbor.redrive({ from: `${topic}-dlq`, idleTimeout: '3s' })
  log(`redrive: ${result.reprocessed} reprocessed, ${result.skipped} skipped`)
  await waitFor(() => processed.length === 1, 60_000, 'the redriven message to be processed')

  await harbor.shutdown('10s')
  log('done')
}

let deadLetters = 0
harbor.on('messageDeadLettered', () => { deadLetters++ })
const deadLettered = (): boolean => deadLetters > 0

async function waitFor (condition: () => boolean, timeoutMs: number, what: string): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
}

main().catch(async (error) => {
  console.error(error)
  process.exitCode = 1
  await harbor.shutdown('5s').catch(() => undefined)
})

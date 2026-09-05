import { randomUUID } from 'node:crypto'
import { after, before, describe, test } from 'node:test'

import { confluentAdapter } from '../../src/adapters/confluent/index'
import { runAdapterContract } from '../../src/testing/index'
import { startKafka, type KafkaFixture } from '../helpers/kafka'

// The broker is shared by the contract suite and the flow suite in this
// file's process; each test uses topics and groups unique to the run.
let kafka: KafkaFixture | undefined

before(async () => {
  kafka = await startKafka()
})

after(async () => {
  await kafka?.stop()
})

describe('confluent adapter against a real broker', () => {
  test('broker available', (t) => {
    if (kafka === undefined) t.skip('no Kafka container')
  })
})

runAdapterContract('confluent', async () => {
  if (kafka === undefined) {
    // Report the skip through a setup that fails loudly only under KAFKA_REQUIRED.
    throw Object.assign(new Error('Kafka container not available'), { skip: true })
  }
  const adapter = confluentAdapter({ global: { 'allow.auto.create.topics': false } })
  await adapter.connect({ clientId: 'contract', brokers: kafka.brokers })
  const run = randomUUID().slice(0, 8)
  return {
    adapter,
    topic: async (label, partitions = 1) => {
      const topic = `${label}-${run}`
      await adapter.admin.createTopics([{ topic, partitions, replicationFactor: 1 }])
      return topic
    },
    group: (label) => `${label}-${run}`,
    timeoutMs: 60_000,
    teardown: async () => { await adapter.disconnect() }
  }
})

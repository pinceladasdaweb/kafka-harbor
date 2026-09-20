import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { AvroDeserializer, AvroSerializer, JsonDeserializer, JsonSerializer, MockClient, SerdeType } from '@confluentinc/schemaregistry'

import { ERROR_CODES, type Serializer } from '../../src/index'
import { isRegistryUnavailable, schemaRegistrySerializer } from '../../src/schema-registry/index'
import { captureEvents, harness } from '../helpers/harness'
import { until } from '../helpers/manual-clock'

interface Order { id: string, total: number }

const avroSchema = JSON.stringify({ type: 'record', name: 'Order', namespace: 'shop', fields: [{ name: 'id', type: 'string' }, { name: 'total', type: 'double' }] })
const jsonSchema = JSON.stringify({ type: 'object', properties: { id: { type: 'string' }, total: { type: 'number' } }, required: ['id', 'total'], additionalProperties: false })

/** A registry in memory with the Avro schema of `orders` registered, and the harbor serializer over the real serdes. */
const avroRegistry = async (): Promise<Serializer> => {
  const client = new MockClient()
  await client.register('orders-value', { schemaType: 'AVRO', schema: avroSchema })
  return schemaRegistrySerializer({
    serializer: new AvroSerializer(client, SerdeType.VALUE, { useLatestVersion: true }),
    deserializer: new AvroDeserializer(client, SerdeType.VALUE, {})
  })
}

describe('kafka-harbor/schema-registry', () => {
  test('Avro through the registry: produced with the registered schema, decoded by the consumer, the wire carries the schema id', async () => {
    const h = harness({ serializer: await avroRegistry() })
    const seen: Order[] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe<Order>('orders', (message) => { seen.push({ ...message.value }) })
    await consumer.start()
    await h.harbor.producer<Order>().send('orders', { key: '1', value: { id: 'a', total: 12.5 } })
    await h.adapter.whenDrained('g', 'orders')
    assert.deepEqual(seen, [{ id: 'a', total: 12.5 }])
    const bytes = h.adapter.messages('orders')[0]?.value as Buffer
    assert.equal(bytes[0], 0, 'the Confluent wire format starts with the magic byte')
    assert.notEqual(bytes.toString('utf8').includes('"id"'), true, 'binary Avro, not JSON')
    await h.harbor.shutdown()
  })

  test('JSON Schema through the registry validates the value: one the schema refuses is a SerializationError before any byte leaves', async () => {
    const client = new MockClient()
    await client.register('orders-value', { schemaType: 'JSON', schema: jsonSchema })
    const serializer = schemaRegistrySerializer({
      serializer: new JsonSerializer(client, SerdeType.VALUE, { useLatestVersion: true, validate: true }),
      deserializer: new JsonDeserializer(client, SerdeType.VALUE, { validate: true })
    })
    const h = harness({ serializer })
    const producer = h.harbor.producer<Order>()
    await producer.send('orders', { value: { id: 'a', total: 1 } })
    await assert.rejects(producer.sendBatch('orders', [{ value: { id: 'b', total: 2 } }, { value: { id: 'c', total: 'not a number' as never } }]), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.SERIALIZATION)
      assert.equal((error as { retryable: boolean }).retryable, false)
      assert.match((error as Error).message, /value for topic "orders" could not be serialized through the schema registry/)
      return true
    })
    assert.equal(h.adapter.messages('orders').length, 1, 'the refused batch produced nothing')
    await h.harbor.shutdown()
  })

  test('bytes no schema describes are dead-lettered as a SerializationError; the retry topic hands the original topic to the registry', async () => {
    const h = harness({ serializer: await avroRegistry() })
    const failed = captureEvents(h.harbor, 'messageFailed')
    const seen: Order[] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    let attempts = 0
    consumer.subscribe<Order>('orders', (message) => {
      attempts++
      if (attempts === 1) throw new Error('first attempt fails')
      seen.push({ ...message.value })
    })
    await consumer.start()
    await h.harbor.connect()
    await h.adapter.produce([{ topic: 'orders', key: null, value: Buffer.from('plain text, no magic byte'), headers: {} }])
    await until(() => failed.length === 1)
    assert.equal(failed[0]?.outcome, 'dead-letter')
    assert.equal((failed[0]?.error as { code: string }).code, ERROR_CODES.SERIALIZATION)
    assert.match((failed[0]?.error as Error).message, /message on topic "orders" could not be deserialized through the schema registry/)
    // A real record fails once in the handler, is retried on orders-retry-1
    // and decodes there too: the registry is asked about "orders", the
    // subject the schema was registered under.
    await h.harbor.producer<Order>().send('orders', { value: { id: 'r', total: 3 } })
    await until(() => seen.length === 1)
    assert.deepEqual(seen, [{ id: 'r', total: 3 }])
    assert.equal(h.adapter.messages('orders-dlq').length, 1)
    await h.harbor.shutdown()
  })

  test('a registry that is unavailable is a transient fault: the message walks the retry ladder and decodes once the registry is back', async () => {
    let outages = 1
    const inner = await avroRegistry()
    const flaky = schemaRegistrySerializer({
      serializer: { serialize: async (topic, value) => await inner.serialize(value, topic) },
      deserializer: {
        deserialize: async (topic, bytes) => {
          if (outages-- > 0) throw Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:8081'), { code: 'ECONNREFUSED' })
          return await inner.deserialize(bytes, topic)
        }
      }
    })
    const h = harness({ serializer: flaky })
    const failed = captureEvents(h.harbor, 'messageFailed')
    const seen: Order[] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    consumer.subscribe<Order>('orders', (message) => { seen.push({ ...message.value }) })
    await consumer.start()
    await h.harbor.producer<Order>().send('orders', { value: { id: 'a', total: 1 } })
    await until(() => seen.length === 1)
    assert.deepEqual(failed.map((event) => [event.topic, event.outcome]), [['orders', 'retry']])
    assert.equal((failed[0]?.error as { code: string }).code, ERROR_CODES.ADAPTER)
    assert.equal((failed[0]?.error as { retryable: boolean }).retryable, true)
    assert.match((failed[0]?.error as Error).message, /schema registry unavailable: .*ECONNREFUSED/)
    assert.equal(h.adapter.messages('orders-dlq').length, 0)
    await h.harbor.shutdown()
  })

  test('classifies what the registry client throws: server trouble, credentials, network and token failures are unavailability, the rest is the value or the schema', () => {
    const restError = (status: number): Error => Object.assign(new Error('rest'), { status, errorCode: 50001 })
    for (const status of [500, 502, 503, 429, 401, 403]) assert.equal(isRegistryUnavailable(restError(status)), true, String(status))
    assert.equal(isRegistryUnavailable(restError(404)), false, 'subject not found is deterministic')
    assert.equal(isRegistryUnavailable(restError(409)), false, 'incompatible schema is deterministic')
    assert.equal(isRegistryUnavailable(restError(422)), false)
    assert.equal(isRegistryUnavailable(restError(400)), false)
    for (const code of ['ECONNREFUSED', 'ETIMEDOUT', 'ENOTFOUND', 'ECONNRESET', 'EAI_AGAIN', 'ERR_NETWORK', 'CERT_HAS_EXPIRED', 'UNABLE_TO_VERIFY_LEAF_SIGNATURE']) {
      assert.equal(isRegistryUnavailable(Object.assign(new Error(code), { code })), true, code)
    }
    assert.equal(isRegistryUnavailable(Object.assign(new Error('x'), { code: 'ECONNREFUSED', status: 400 })), false, 'an answer with a status is an answer')
    assert.equal(isRegistryUnavailable(Object.assign(new Error('x'), { code: 'ERR_INVALID_ARG' })), false)
    assert.equal(isRegistryUnavailable(new Error('Failed to get token from server: ECONNREFUSED')), true, 'the OAuth layer reports a token it could not obtain as a plain Error')
    assert.equal(isRegistryUnavailable(new Error('schema rejected: Failed to get token from server')), false, 'only a message that starts with the token failure is one')
    assert.equal(isRegistryUnavailable(new Error('Unknown magic byte')), false)
    assert.equal(isRegistryUnavailable(null), false)
    assert.equal(isRegistryUnavailable('ECONNREFUSED'), false)
  })

  test('a classified failure keeps the client error as its cause and says whether to retry', async () => {
    const flaky = schemaRegistrySerializer<Order>({
      serializer: { serialize: async () => { throw Object.assign(new Error('connect ECONNREFUSED'), { code: 'ECONNREFUSED' }) } },
      deserializer: { deserialize: async () => { throw new Error('Unknown magic byte') } }
    })
    await assert.rejects(Promise.resolve(flaky.serialize({ id: 'a', total: 1 }, 'orders')), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.ADAPTER)
      assert.equal((error as { retryable: boolean }).retryable, true)
      assert.equal(((error as { cause: Error }).cause).message, 'connect ECONNREFUSED')
      return true
    })
    await assert.rejects(Promise.resolve(flaky.deserialize(Buffer.from('x'), 'orders')), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.SERIALIZATION)
      assert.equal(((error as { cause: Error }).cause).message, 'Unknown magic byte')
      return true
    })
  })

  test('one side only: a consumer-only serializer refuses to produce and a producer-only one refuses to consume, both as ConfigError', async () => {
    const inner = await avroRegistry()
    const consumeOnly = schemaRegistrySerializer<Order>({ deserializer: { deserialize: async (topic, bytes) => await inner.deserialize(bytes, topic) as Order } })
    await assert.rejects(Promise.resolve(consumeOnly.serialize({ id: 'a', total: 1 }, 'orders')), { code: ERROR_CODES.CONFIG_INVALID })
    const produceOnly = schemaRegistrySerializer<Order>({ serializer: { serialize: async (topic, value) => await inner.serialize(value, topic) } })
    await assert.rejects(Promise.resolve(produceOnly.deserialize(Buffer.from('x'), 'orders')), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => schemaRegistrySerializer({}), { code: ERROR_CODES.CONFIG_INVALID })
    // A harbor SerializationError thrown by a serde passes through unchanged.
    const passing = schemaRegistrySerializer<Order>({ serializer: { serialize: async () => { throw Object.assign(new Error('mine'), { code: ERROR_CODES.SERIALIZATION, retryable: false }) } } })
    await assert.rejects(Promise.resolve(passing.serialize({ id: 'a', total: 1 }, 'orders')), /^Error: mine$/)
  })
})

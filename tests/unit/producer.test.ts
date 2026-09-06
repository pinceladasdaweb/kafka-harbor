import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { fixed } from 'breakwater'

import { AdapterError, ERROR_CODES } from '../../src/index'
import { harness, json, text } from '../helpers/harness'

describe('Producer', () => {
  test('serializes the value, keeps the key and writes the automatic headers', async () => {
    const { adapter, clock, harbor } = harness()
    await harbor.producer().send('orders', { key: 'o-1', value: { id: 1 }, headers: { 'x-tenant': 'acme' } })
    const [message] = adapter.messages('orders')
    assert.ok(message)
    assert.equal(text(message.key), 'o-1')
    assert.deepEqual(json(message.value), { id: 1 })
    assert.deepEqual(message.headers, {
      'x-tenant': 'acme',
      'x-correlation-id': 'corr-fixed',
      'x-produced-at': new Date(clock.now()).toISOString(),
      'x-producer': 'test-app'
    })
  })

  test('a correlation id given by the caller is kept', async () => {
    const { adapter, harbor } = harness()
    await harbor.producer().send('orders', { value: 1, headers: { 'x-correlation-id': 'mine' } })
    assert.equal(adapter.messages('orders')[0]?.headers['x-correlation-id'], 'mine')
  })

  test('a blank correlation id from the caller is replaced, not kept', async () => {
    const { adapter, harbor } = harness()
    await harbor.producer().send('orders', { value: 1, headers: { 'x-correlation-id': '  ' } })
    assert.equal(adapter.messages('orders')[0]?.headers['x-correlation-id'], 'corr-fixed')
  })

  test('a null value is a tombstone: no bytes, and the serializer is never asked', async () => {
    const { adapter, harbor } = harness()
    const codec = { serialize: (): Buffer => { throw new Error('must not be called for a tombstone') }, deserialize: (): { id: number } => ({ id: 0 }) }
    await harbor.producer<{ id: number }>({ serializer: codec }).send('orders', { key: 'k', value: null })
    const [message] = adapter.messages('orders')
    assert.equal(message?.value, null)
    assert.equal(text(message?.key ?? null), 'k')
    // The default serializer still encodes a JSON null when asked for one through a batch of real values.
    await harbor.producer().sendBatch('orders', [{ value: 1 }, { value: null }])
    assert.deepEqual(adapter.messages('orders').slice(1).map((m) => m.value), [Buffer.from('1'), null])
  })

  test('every record of a batch carries the same produced-at instant', async () => {
    const { adapter, clock, harbor } = harness()
    await harbor.producer().sendBatch('orders', [{ value: 1 }, { value: 2 }])
    const stamps = new Set(adapter.messages('orders').map((m) => m.headers['x-produced-at']))
    assert.deepEqual([...stamps], [new Date(clock.now()).toISOString()])
  })

  test('the header prefix follows the harbor configuration', async () => {
    const { adapter, harbor } = harness({ headers: { prefix: '', correlationId: () => 'c' } })
    await harbor.producer().send('orders', { value: 1 })
    const headers = adapter.messages('orders')[0]?.headers ?? {}
    assert.equal(headers['correlation-id'], 'c')
    assert.equal(headers.producer, 'test-app')
    assert.equal(headers['x-producer'], undefined)
  })

  test('connects lazily on the first send', async () => {
    const { adapter, harbor } = harness()
    const producer = harbor.producer()
    assert.equal(adapter.calls.length, 0)
    assert.equal(harbor.status, 'idle')
    await producer.send('orders', { value: 1 })
    assert.equal(adapter.calls[0]?.method, 'connect')
    assert.equal(harbor.status, 'connected')
  })

  test('a null or missing key produces a null key; an explicit partition is honored', async () => {
    const { adapter, harbor } = harness({}, { partitions: 3 })
    const producer = harbor.producer()
    await producer.send('orders', { value: 1 })
    await producer.send('orders', { key: null, value: 2 })
    await producer.send('orders', { value: 3, partition: 2 })
    const messages = adapter.messages('orders')
    assert.equal(messages[0]?.key, null)
    assert.equal(messages[1]?.key, null)
    assert.equal(messages.find((m) => json(m.value) === 3)?.partition, 2)
  })

  test('sendBatch produces every message in one adapter call', async () => {
    const { adapter, harbor } = harness()
    await harbor.producer().sendBatch('orders', [{ value: 1 }, { value: 2 }, { value: 3 }])
    assert.equal(adapter.calls.filter((call) => call.method === 'produce').length, 1)
    assert.deepEqual(adapter.messages('orders').map((m) => json(m.value)), [1, 2, 3])
  })

  test('an empty batch produces nothing and does not connect', async () => {
    const { adapter, harbor } = harness()
    await harbor.producer().sendBatch('orders', [])
    assert.equal(adapter.calls.length, 0)
  })

  test('a batch with one unencodable value produces nothing at all', async () => {
    const { adapter, harbor } = harness()
    await assert.rejects(
      harbor.producer().sendBatch('orders', [{ value: 1 }, { value: new Map() }]),
      { code: ERROR_CODES.SERIALIZATION }
    )
    assert.equal(adapter.messages('orders').length, 0)
    assert.equal(adapter.calls.filter((call) => call.method === 'produce').length, 0)
  })

  test('a per-producer serializer overrides the harbor default', async () => {
    const { adapter, harbor } = harness()
    const producer = harbor.producer<string>({
      serializer: { serialize: (value) => Buffer.from(value.toUpperCase()), deserialize: (bytes) => bytes.toString() }
    })
    await producer.send('orders', { value: 'abc' })
    assert.equal(text(adapter.messages('orders')[0]?.value ?? null), 'ABC')
  })

  test('retries a transient adapter failure and gives up on a non-retryable one', async () => {
    const { adapter, harbor } = harness()
    const producer = harbor.producer({ retry: { attempts: 3, backoff: fixed(0) } })

    adapter.failNextProduce(new AdapterError('broker hiccup'))
    await producer.send('orders', { value: 1 })
    assert.equal(adapter.calls.filter((call) => call.method === 'produce').length, 2)
    assert.equal(adapter.messages('orders').length, 1)

    adapter.failNextProduce(new AdapterError('bad record', { retryable: false }))
    await assert.rejects(producer.send('orders', { value: 2 }), { code: ERROR_CODES.ADAPTER })
    assert.equal(adapter.calls.filter((call) => call.method === 'produce').length, 3)
  })

  test('exhausting the retries surfaces breakwater\'s RETRY_EXHAUSTED with the last failure as cause', async () => {
    const { adapter, harbor } = harness()
    const producer = harbor.producer({ retry: { attempts: 2, backoff: fixed(0) } })
    adapter.failNextProduce(new AdapterError('down'))
    // The memory adapter fails once per failNextProduce call; arm it twice.
    const original = adapter.produce
    let calls = 0
    adapter.produce = async (records) => {
      calls++
      if (calls <= 2) throw new AdapterError('down')
      return await original(records)
    }
    await assert.rejects(producer.send('orders', { value: 1 }), (error: unknown) => {
      assert.equal((error as { code: string }).code, 'RETRY_EXHAUSTED')
      assert.equal(((error as { cause: { code: string } }).cause).code, ERROR_CODES.ADAPTER)
      return true
    })
  })

  test('validates the topic and the retry attempts', async () => {
    const { harbor } = harness()
    await assert.rejects(harbor.producer().send('', { value: 1 }), { code: ERROR_CODES.CONFIG_INVALID })
    await assert.rejects(harbor.producer().send(42 as never, { value: 1 }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => harbor.producer({ retry: { attempts: 0 } }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => harbor.producer({ retry: { attempts: 1.5 } }), { code: ERROR_CODES.CONFIG_INVALID })
  })

  test('refuses to send after shutdown', async () => {
    const { harbor } = harness()
    const producer = harbor.producer()
    await harbor.shutdown()
    await assert.rejects(producer.send('orders', { value: 1 }), { code: ERROR_CODES.CLOSED })
  })
})

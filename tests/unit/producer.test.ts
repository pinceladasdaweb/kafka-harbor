import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { fixed } from 'breakwater'

import { AdapterError, ERROR_CODES, partitionForKey } from '../../src/index'
import { captureErrors, gate, harness, json, text } from '../helpers/harness'
import { settle, until } from '../helpers/manual-clock'

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

  test('with no retry policy given, a transient failure is retried after the default backoff and the send recovers', async () => {
    const { adapter, harbor } = harness()
    const producer = harbor.producer()
    adapter.failNextProduce(new AdapterError('broker hiccup'))
    await producer.send('orders', { value: 1 })
    assert.equal(adapter.calls.filter((call) => call.method === 'produce').length, 2)
    assert.equal(adapter.messages('orders').length, 1)
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

  test('reports an acknowledged batch as batchProduced and a failed one as a producer error, then rejects', async () => {
    const { adapter, clock, harbor } = harness()
    const produced: Array<{ topic: string, kind: string, records: number, durationMs: number }> = []
    const errors: Array<{ scope: string, topic?: string, error: unknown }> = []
    harbor.on('batchProduced', (event) => { produced.push(event) })
    harbor.on('error', (event) => { errors.push(event) })
    const producer = harbor.producer({ retry: { attempts: 2, backoff: fixed(0) } })

    const original = adapter.produce
    adapter.produce = async (records) => { clock.advance(25); await original(records) }
    await producer.sendBatch('orders', [{ value: 1 }, { value: 2 }])
    assert.deepEqual(produced, [{ topic: 'orders', kind: 'send', records: 2, durationMs: 25 }])
    assert.equal(errors.length, 0)

    adapter.produce = original
    adapter.failNextProduce(new AdapterError('refused', { retryable: false }))
    await assert.rejects(producer.send('orders', { value: 3 }), /refused/)
    assert.equal(produced.length, 1, 'a failed batch is not counted as produced')
    assert.equal(errors.length, 1)
    assert.equal(errors[0]?.scope, 'producer')
    assert.equal(errors[0]?.topic, 'orders')
    assert.equal((errors[0]?.error as Error).message, 'refused')
  })

  test('a connection that fails is the harbor\'s error, reported once with the adapter scope however many sends waited', async () => {
    const { adapter, harbor } = harness()
    const errors = captureErrors(harbor)
    adapter.connect = async () => { throw new AdapterError('no broker', { retryable: false }) }
    const producer = harbor.producer()
    const results = await Promise.allSettled([1, 2, 3].map(async (n) => await producer.send('orders', { value: n })))
    assert.deepEqual(results.map((result) => result.status), ['rejected', 'rejected', 'rejected'])
    assert.equal(errors.length, 1)
    assert.equal(errors[0]?.scope, 'adapter')
    assert.equal((errors[0]?.error as Error).message, 'no broker')
  })

  test('a send after shutdown is refused without an error event', async () => {
    const { harbor } = harness()
    const errors = captureErrors(harbor)
    await harbor.shutdown()
    await assert.rejects(harbor.producer().send('orders', { value: 1 }), { code: ERROR_CODES.CLOSED })
    assert.equal(errors.length, 0)
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

describe('Producer: what it refuses, and what a shutdown does to its sends', () => {
  test('a header or a key that is not a string, a message or a batch of the wrong shape are refused before anything is sent', async () => {
    const { adapter, harbor } = harness()
    const producer = harbor.producer()
    const refused = async (send: Promise<void>, message: RegExp): Promise<void> => {
      await assert.rejects(send, (error: unknown) => {
        assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
        assert.equal((error as { retryable: boolean }).retryable, false)
        assert.match((error as Error).message, message)
        return true
      })
    }
    await refused(producer.send('orders', { value: 1, headers: { 'x-tenant': undefined as unknown as string } }), /header "x-tenant" of a message for topic "orders" must be a string; got undefined/)
    await refused(producer.send('orders', { value: 1, headers: { n: 5 as unknown as string } }), /header "n" .* got number/)
    await refused(producer.send('orders', { value: 1, headers: { n: null as unknown as string } }), /header "n" .* got null/)
    await refused(producer.send('orders', { value: 1, headers: 'x' as never }), /the headers of a message for topic "orders" must be an object of strings; got string/)
    await refused(producer.send('orders', { value: 1, headers: null as never }), /headers .* got null/)
    await refused(producer.send('orders', { value: 1, headers: ['a'] as never }), /headers .* got array/)
    await refused(producer.send('orders', { value: 1, key: 42 as never }), /the key of a message for topic "orders" must be a string or a Buffer; got number/)
    await refused(producer.send('orders', null as never), /a message for topic "orders" must be an object; got null/)
    await refused(producer.send('orders', 'x' as never), /must be an object; got string/)
    await refused(producer.sendBatch('orders', { value: 1 } as never), /messages for topic "orders" must be an array/)
    assert.equal(adapter.calls.filter((call) => call.method === 'produce' || call.method === 'connect').length, 0, 'nothing sent, nothing retried, not even a connection')
  })

  test('a shutdown waits for a send in flight before it releases the client', async () => {
    const h = harness()
    const acknowledged = gate()
    let sending = false
    const original = h.adapter.produce
    h.adapter.produce = async (records) => {
      sending = true
      await acknowledged.wait
      await original(records)
    }
    const sent = h.harbor.producer().send('orders', { value: 1 })
    await until(() => sending)
    let closed = false
    const closing = h.harbor.shutdown().then(() => { closed = true })
    await settle()
    assert.equal(closed, false, 'the shutdown waits for the send')
    assert.equal(h.adapter.connected, true)
    acknowledged.release()
    await sent
    await closing
    assert.equal(h.adapter.messages('orders').length, 1)
    assert.equal(h.adapter.connected, false)
  })

  test('past the shutdown timeout, a send waiting out a retry backoff fails at once with ClosedError', async () => {
    const h = harness()
    const errors = captureErrors(h.harbor)
    h.adapter.failNextProduce(new AdapterError('broker hiccup'))
    const sent = h.harbor.producer({ retry: { attempts: 3, backoff: fixed(60_000) } }).send('orders', { value: 1 })
    await until(() => h.adapter.calls.some((call) => call.method === 'produce'))
    await settle()
    const startedAt = Date.now()
    await h.harbor.shutdown(0)
    await assert.rejects(sent, { code: ERROR_CODES.CLOSED })
    assert.ok(Date.now() - startedAt < 1_000, 'the backoff was not sat out')
    assert.ok(h.logs.some((entry) => entry.level === 'warn' && entry.message.includes('1 send(s) or transaction(s) still in flight after 0ms')))
    assert.equal(h.adapter.messages('orders').length, 0)
    assert.deepEqual(errors, [], 'the harbor closing is the caller\'s ClosedError, not a producer failure')
  })

  test('a send begun while the shutdown waits for the ones in flight is waited for too', async () => {
    const h = harness()
    const gates = [gate(), gate()]
    let entered = 0
    const original = h.adapter.produce
    h.adapter.produce = async (records) => {
      await gates[entered++]?.wait
      await original(records)
    }
    const producer = h.harbor.producer()
    const first = producer.send('orders', { value: 1 })
    await until(() => entered === 1)
    let closed = false
    const closing = h.harbor.shutdown('1m').then(() => { closed = true })
    await settle()
    const second = producer.send('orders', { value: 2 })
    await until(() => entered === 2)
    gates[0]?.release()
    await first
    await settle()
    assert.equal(closed, false, 'the second send is waited for as well')
    gates[1]?.release()
    await second
    await closing
    assert.equal(h.adapter.messages('orders').length, 2)
    assert.equal(h.adapter.connected, false)
  })

  test('a key may be bytes: produced as they are, and partitioned by them', async () => {
    const { adapter, harbor } = harness({}, { partitions: 4 })
    const key = Buffer.from([0xff, 0x00, 0x10])
    await harbor.producer().send('orders', { value: 1, key })
    const [record] = adapter.messages('orders')
    assert.deepEqual(record?.key, key)
    assert.equal(record?.partition, partitionForKey(key, 4))
    // Copied when the send begins: the caller may reuse its buffer.
    const reused = Buffer.from('first')
    const sending = harbor.producer().send('orders', { value: 2, key: reused })
    reused.write('later')
    await sending
    assert.deepEqual(adapter.messages('orders')[1]?.key, Buffer.from('first'))
    await harbor.shutdown()
  })
})

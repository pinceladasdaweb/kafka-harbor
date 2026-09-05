import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { ERROR_CODES, abortProcessing, createHarbor, isAbortProcessingError, isHarborError, isRetryable } from '../../src/index'
import { memoryAdapter } from '../../src/testing/index'
import { harness } from '../helpers/harness'

describe('createHarbor', () => {
  test('validates clientId, brokers and adapter', () => {
    const adapter = memoryAdapter()
    assert.throws(() => createHarbor({ clientId: '', brokers: ['b:1'], adapter }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => createHarbor({ clientId: 'a', brokers: [], adapter }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => createHarbor({ clientId: 'a', brokers: [''], adapter }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => createHarbor({ clientId: 'a', brokers: ['ok:1', ''], adapter }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => createHarbor({ clientId: 'a', brokers: 'b:1' as never, adapter }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => createHarbor({ clientId: 'a', brokers: [1 as never], adapter }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => createHarbor({ clientId: 'a', brokers: ['b:1'], adapter: { connect: adapter.connect, produce: adapter.produce } as never }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => createHarbor({ clientId: 'a', brokers: ['b:1'], adapter: {} as never }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => createHarbor({ clientId: 'a', brokers: ['b:1'], adapter: { connect: adapter.connect } as never }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => createHarbor({ clientId: 'a', brokers: ['b:1'], adapter: { connect: adapter.connect, consume: adapter.consume } as never }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => createHarbor({ clientId: 'a', brokers: ['b:1'], adapter: undefined as never }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => createHarbor(undefined as never), { code: ERROR_CODES.CONFIG_INVALID })
    assert.doesNotThrow(() => createHarbor({ clientId: 'a', brokers: ['b:1'], adapter }))
  })

  test('without header options, correlation ids are UUIDs and the prefix is x-', async () => {
    const adapter = memoryAdapter()
    const harbor = createHarbor({ clientId: 'a', brokers: ['b:1'], adapter, logger: { info: () => {}, warn: () => {}, error: () => {} } })
    await harbor.producer().send('t', { value: 1 })
    const headers = adapter.messages('t')[0]?.headers ?? {}
    assert.match(String(headers['x-correlation-id']), /^[0-9a-f-]{36}$/)
    assert.equal(harbor.headerNames.producer, 'x-producer')
    await harbor.shutdown()
  })

  test('ssl and sasl are passed to the adapter only when configured', async () => {
    const { adapter, harbor } = harness()
    await harbor.connect()
    const config = adapter.calls[0]?.args[0] as Record<string, unknown>
    assert.equal('ssl' in config, false)
    assert.equal('sasl' in config, false)
    await harbor.shutdown()
  })

  test('the default logger writes to the console', async () => {
    const adapter = memoryAdapter()
    const harbor = createHarbor({ clientId: 'a', brokers: ['b:1'], adapter })
    const written: string[] = []
    const original = { error: console.error, warn: console.warn, info: console.info }
    console.error = (message: string) => { written.push(`error:${message}`) }
    console.warn = (message: string) => { written.push(`warn:${message}`) }
    console.info = (message: string) => { written.push(`info:${message}`) }
    try {
      harbor.on('connected', () => { throw new Error('listener bug') })
      await harbor.connect()
      const consumer = harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
      consumer.subscribe('orders', async () => { await new Promise(() => {}) })
      await harbor.producer().send('orders', { value: 1 })
      await consumer.start()
      await new Promise((resolve) => setTimeout(resolve, 10))
      harbor.enableSignalHandlers(['SIGUSR2'], 0)
      process.emit('SIGUSR2')
      await new Promise((resolve) => setTimeout(resolve, 20))
    } finally {
      Object.assign(console, original)
    }
    assert.ok(written.some((line) => line.startsWith('error:') && line.includes('listener bug')))
    assert.ok(written.some((line) => line.startsWith('info:') && line.includes('SIGUSR2')))
    assert.ok(written.some((line) => line.startsWith('warn:') && line.includes('handler(s) still running')))
  })

  test('a shutdown triggered by a signal that has to abandon a handler is logged as an error', async () => {
    const h = harness()
    let started = false
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', async () => { started = true; await new Promise(() => {}) })
    await h.harbor.producer().send('orders', { value: 1 })
    await consumer.start()
    await new Promise((resolve) => setTimeout(resolve, 10))
    assert.equal(started, true)
    h.harbor.enableSignalHandlers(['SIGUSR2'], 0)
    process.emit('SIGUSR2')
    await new Promise((resolve) => setTimeout(resolve, 20))
    assert.equal(h.harbor.status, 'closed')
    assert.ok(h.logs.some((entry) => entry.level === 'error' && entry.message.includes('shutdown after SIGUSR2 failed')))
  })

  test('the default signals are SIGTERM and SIGINT', async () => {
    const { harbor } = harness()
    const term = process.listenerCount('SIGTERM')
    const int = process.listenerCount('SIGINT')
    const disable = harbor.enableSignalHandlers()
    assert.equal(process.listenerCount('SIGTERM'), term + 1)
    assert.equal(process.listenerCount('SIGINT'), int + 1)
    disable()
    await harbor.shutdown()
  })

  test('enabling signal handlers twice registers them once', async () => {
    const { harbor } = harness()
    const before = process.listenerCount('SIGTERM')
    harbor.enableSignalHandlers(['SIGTERM'])
    harbor.enableSignalHandlers(['SIGTERM'])
    assert.equal(process.listenerCount('SIGTERM'), before + 1)
    await harbor.shutdown()
    assert.equal(process.listenerCount('SIGTERM'), before)
  })

  test('shutdown while a connect is in flight waits for it, then disconnects', async () => {
    const { adapter, harbor } = harness()
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const original = adapter.connect
    adapter.connect = async (config) => { await gate; await original(config) }
    const connecting = harbor.connect()
    const closing = harbor.shutdown()
    release()
    await connecting
    await closing
    assert.deepEqual(adapter.calls.map((call) => call.method), ['connect', 'disconnect'])
    assert.equal(harbor.status, 'closed')
  })

  test('does not connect on creation; connect() is idempotent and single-flight', async () => {
    const { adapter, harbor } = harness({ ssl: true, sasl: { mechanism: 'scram-sha-512', username: 'u', password: 'p' } })
    assert.equal(adapter.calls.length, 0)
    await Promise.all([harbor.connect(), harbor.connect()])
    await harbor.connect()
    assert.equal(adapter.calls.filter((call) => call.method === 'connect').length, 1)
    assert.deepEqual(adapter.calls[0]?.args[0], {
      clientId: 'test-app',
      brokers: ['memory:9092'],
      ssl: true,
      sasl: { mechanism: 'scram-sha-512', username: 'u', password: 'p' }
    })
    assert.equal(harbor.status, 'connected')
    await harbor.shutdown()
  })

  test('a failed connect leaves the harbor idle so the next call retries', async () => {
    const { adapter, harbor } = harness()
    const original = adapter.connect
    adapter.connect = async () => { throw new Error('no broker') }
    await assert.rejects(harbor.connect(), /no broker/)
    assert.equal(harbor.status, 'idle')
    adapter.connect = original
    await harbor.connect()
    assert.equal(harbor.status, 'connected')
    await harbor.shutdown()
  })

  test('emits connected and disconnected', async () => {
    const { harbor } = harness()
    const seen: string[] = []
    harbor.on('connected', ({ adapter }) => { seen.push(`connected:${adapter}`) })
    harbor.on('disconnected', ({ adapter }) => { seen.push(`disconnected:${adapter}`) })
    await harbor.connect()
    await harbor.shutdown()
    assert.deepEqual(seen, ['connected:memory', 'disconnected:memory'])
  })

  test('shutdown of an idle harbor does not touch the adapter, and shutdown is idempotent', async () => {
    const { adapter, harbor } = harness()
    await harbor.shutdown()
    await harbor.shutdown()
    assert.equal(adapter.calls.length, 0)
    assert.equal(harbor.status, 'closed')
    await assert.rejects(harbor.connect(), { code: ERROR_CODES.CLOSED })
  })

  test('connect() during a shutdown in progress is refused as closed', async () => {
    const { adapter, harbor } = harness()
    await harbor.connect()
    let release!: () => void
    const gate = new Promise<void>((resolve) => { release = resolve })
    const original = adapter.disconnect
    adapter.disconnect = async () => { await gate; await original() }
    const closing = harbor.shutdown()
    assert.equal(harbor.status, 'closing')
    await assert.rejects(harbor.connect(), { code: ERROR_CODES.CLOSED })
    release()
    await closing
  })

  test('shutdown accepts a duration string', async () => {
    const { harbor } = harness()
    await harbor.connect()
    await harbor.shutdown('5s')
    assert.equal(harbor.status, 'closed')
  })

  test('off() removes a listener', async () => {
    const { harbor } = harness()
    let calls = 0
    const listener = (): void => { calls++ }
    harbor.on('connected', listener).off('connected', listener)
    await harbor.connect()
    assert.equal(calls, 0)
    await harbor.shutdown()
  })

  test('exposes the header names in effect', () => {
    const { harbor } = harness({ headers: { prefix: 'h-' } })
    assert.equal(harbor.headerNames.retryCount, 'h-retry-count')
  })

  test('abort() and abortProcessing() build the same abort error', () => {
    const { harbor } = harness()
    const cause = new Error('c')
    const error = harbor.abort(cause)
    assert.ok(isAbortProcessingError(error))
    assert.ok(isAbortProcessingError(abortProcessing(cause)))
    assert.equal(error.cause, cause)
    assert.ok(isHarborError(error))
  })

  test('signal handlers are opt-in, removable, and removed by shutdown', async () => {
    const { harbor } = harness()
    const before = process.listenerCount('SIGTERM')
    const disable = harbor.enableSignalHandlers(['SIGTERM'])
    assert.equal(process.listenerCount('SIGTERM'), before + 1)
    disable()
    assert.equal(process.listenerCount('SIGTERM'), before)
    harbor.enableSignalHandlers(['SIGTERM'])
    await harbor.shutdown()
    assert.equal(process.listenerCount('SIGTERM'), before)
  })

  test('an invalid signal-handler timeout is rejected at the call, before any signal', () => {
    const { harbor } = harness()
    const before = process.listenerCount('SIGTERM')
    assert.throws(() => harbor.enableSignalHandlers(['SIGTERM'], '30'), { code: ERROR_CODES.CONFIG_INVALID })
    assert.equal(process.listenerCount('SIGTERM'), before)
  })

  test('a signal triggers shutdown', async () => {
    const { harbor, logs } = harness()
    await harbor.connect()
    harbor.enableSignalHandlers(['SIGUSR2'])
    process.emit('SIGUSR2')
    await new Promise((resolve) => setTimeout(resolve, 10))
    assert.equal(harbor.status, 'closed')
    assert.ok(logs.some((entry) => entry.message.includes('SIGUSR2')))
  })
})

describe('error helpers', () => {
  test('isRetryable honors retryable: false on any object and treats everything else as retryable', () => {
    assert.equal(isRetryable(new Error('x')), true)
    assert.equal(isRetryable({ retryable: false }), false)
    assert.equal(isRetryable({ retryable: 'no' }), true)
    assert.equal(isRetryable(null), true)
    assert.equal(isRetryable('boom'), true)
  })

  test('isHarborError branches on code, not on class identity', () => {
    assert.equal(isHarborError({ code: ERROR_CODES.SERIALIZATION }), true)
    assert.equal(isHarborError({ code: 'SOMETHING_ELSE' }), false)
    assert.equal(isHarborError(new Error('x')), false)
  })
})

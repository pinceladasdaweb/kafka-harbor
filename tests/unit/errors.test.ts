import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import {
  AbortProcessingError,
  AdapterError,
  ClosedError,
  ConfigError,
  ERROR_CODES,
  HarborError,
  SerializationError,
  ShutdownTimeoutError,
  TopicMissingError,
  describeError,
  isAbortProcessingError,
  isBatchFailedError,
  isDeclaredRetryable,
  isHarborError,
  isSerializationError,
  isShutdownTimeoutError,
  isTopicMissingError,
  isAdapterError,
  isClosedError,
  isConfigError,
  isFencedError,
  FencedError
} from '../../src/index'

describe('error classes', () => {
  test('carry their code, their name and a retryable flag that defaults to true', () => {
    const error = new HarborError(ERROR_CODES.ADAPTER, 'x')
    assert.equal(error.code, 'ADAPTER')
    assert.equal(error.name, 'HarborError')
    assert.strictEqual(error.retryable, true)
    assert.strictEqual(new AdapterError('x').retryable, true)
    assert.strictEqual(new AdapterError('x', { retryable: false }).retryable, false)
  })

  test('deterministic failures are never retryable', () => {
    assert.strictEqual(new ConfigError('x').retryable, false)
    assert.strictEqual(new SerializationError('x').retryable, false)
    assert.strictEqual(new TopicMissingError('t').retryable, false)
    assert.strictEqual(new ClosedError('harbor').retryable, false)
    assert.equal(new TopicMissingError('t').topic, 't')
    assert.equal(new ClosedError('harbor').code, ERROR_CODES.CLOSED)
    assert.equal(new ConfigError('x', { cause: 'c' }).cause, 'c')
  })

  test('AbortProcessingError and ShutdownTimeoutError expose their context', () => {
    const cause = new Error('c')
    assert.equal(new AbortProcessingError(cause).cause, cause)
    assert.equal(new AbortProcessingError(cause).name, 'AbortProcessingError')
    const timeout = new ShutdownTimeoutError(3, 500)
    assert.equal(timeout.inFlight, 3)
    assert.equal(timeout.code, ERROR_CODES.SHUTDOWN_TIMEOUT)
  })
})

describe('type guards', () => {
  test('match by shape, not class: a code of the table and a boolean retryable, from any copy of the package', () => {
    assert.equal(isAbortProcessingError({ code: 'ABORT_PROCESSING', retryable: false }), true)
    assert.equal(isSerializationError({ code: 'SERIALIZATION', retryable: false }), true)
    // A code alone is someone else's error that happens to use the same word.
    assert.equal(isSerializationError({ code: 'SERIALIZATION' }), false)
    assert.equal(isConfigError(new ConfigError('x')), true)
    assert.equal(isAdapterError(new AdapterError('x')), true)
    assert.equal(isClosedError(new ClosedError('harbor')), true)
    assert.equal(isClosedError(new ConfigError('x')), false)
    assert.equal(new AbortProcessingError('stop').retryable, false, 'never retried: it stops the consumer')
    assert.equal(isTopicMissingError(new TopicMissingError('t')), true)
    assert.equal(isShutdownTimeoutError(new ShutdownTimeoutError(1, 1)), true)
    assert.equal(isTopicMissingError(new ShutdownTimeoutError(1, 1)), false)
    assert.equal(isShutdownTimeoutError({ code: 'TOPIC_MISSING' }), false)
    const fenced = new FencedError('orders-1', { cause: new Error('epoch') })
    assert.deepEqual([isFencedError(fenced), fenced.code, fenced.retryable, fenced.transactionalId, fenced.name], [true, 'FENCED', false, 'orders-1', 'FencedError'])
    assert.match(fenced.message, /transactional id "orders-1" is held by another producer/)
    assert.equal(isFencedError(new ConfigError('x')), false)
  })

  test('reject null, primitives and objects with a foreign or non-string code', () => {
    for (const value of [null, undefined, 'ABORT_PROCESSING', 42, { code: 7 }, { code: 'SOMETHING_ELSE' }, {}]) {
      assert.equal(isHarborError(value), false, `isHarborError(${String(value)})`)
      assert.equal(isAbortProcessingError(value), false)
      assert.equal(isSerializationError(value), false)
    }
    assert.equal(isHarborError({ code: 'CLOSED', retryable: false }), true)
    assert.equal(isHarborError({ code: 'CLOSED' }), false)
    assert.equal(isHarborError({ code: 'CLOSED', retryable: 'no' }), false)
  })
})

describe('shape guards', () => {
  test('isBatchFailedError needs both the code and the list of failed messages', () => {
    assert.equal(isBatchFailedError({ code: 'BATCH_FAILED', retryable: true }), false)
    assert.equal(isBatchFailedError({ failed: [] }), false)
    assert.equal(isBatchFailedError({ code: 'BATCH_FAILED', retryable: true, failed: [] }), true)
  })

  test('isDeclaredRetryable is true only for an object that says retryable: true', () => {
    for (const value of [null, undefined, 'retryable', 42, {}, { retryable: false }, { retryable: 'yes' }]) {
      assert.equal(isDeclaredRetryable(value), false, `isDeclaredRetryable(${String(value)})`)
    }
    assert.equal(isDeclaredRetryable({ retryable: true }), true)
    assert.equal(isDeclaredRetryable(new AdapterError('x', { retryable: true })), true)
    assert.equal(isDeclaredRetryable(new AdapterError('x', { retryable: false })), false)
  })
})

describe('describeError', () => {
  test('prefixes the name of a custom error and keeps a plain Error message bare', () => {
    class ValidationError extends Error { override name = 'ValidationError' }
    assert.equal(describeError(new Error('boom')), 'boom')
    assert.equal(describeError(new ValidationError('bad field')), 'ValidationError: bad field')
    const already = new ValidationError('ValidationError: twice')
    assert.equal(describeError(already), 'ValidationError: twice')
  })

  test('describes strings, objects and things JSON cannot take', () => {
    assert.equal(describeError('plain'), 'plain')
    assert.equal(describeError({ code: 'X', n: 1 }), '{"code":"X","n":1}')
    assert.equal(describeError(undefined), 'undefined')
    assert.equal(describeError(7n), '7')
    const cyclic: { self?: unknown } = {}
    cyclic.self = cyclic
    assert.equal(describeError(cyclic), '[object Object]')
  })

  test('bounds the text to maxLength, marking the cut', () => {
    assert.equal(describeError('a'.repeat(1024)).length, 1024)
    assert.equal(describeError('a'.repeat(1024)).endsWith('a'), true)
    const cut = describeError('b'.repeat(1025))
    assert.equal(cut.length, 1024)
    assert.equal(cut.endsWith('...'), true)
    assert.equal(describeError('hello world', 8), 'hello...')
  })
})

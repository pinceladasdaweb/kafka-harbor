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
  isHarborError,
  isSerializationError,
  isShutdownTimeoutError,
  isTopicMissingError
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
  test('match by code only, on any object shape', () => {
    assert.equal(isAbortProcessingError({ code: 'ABORT_PROCESSING' }), true)
    assert.equal(isSerializationError({ code: 'SERIALIZATION' }), true)
    assert.equal(isTopicMissingError(new TopicMissingError('t')), true)
    assert.equal(isShutdownTimeoutError(new ShutdownTimeoutError(1, 1)), true)
    assert.equal(isTopicMissingError(new ShutdownTimeoutError(1, 1)), false)
    assert.equal(isShutdownTimeoutError({ code: 'TOPIC_MISSING' }), false)
  })

  test('reject null, primitives and objects with a foreign or non-string code', () => {
    for (const value of [null, undefined, 'ABORT_PROCESSING', 42, { code: 7 }, { code: 'SOMETHING_ELSE' }, {}]) {
      assert.equal(isHarborError(value), false, `isHarborError(${String(value)})`)
      assert.equal(isAbortProcessingError(value), false)
      assert.equal(isSerializationError(value), false)
    }
    assert.equal(isHarborError({ code: 'CLOSED' }), true)
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

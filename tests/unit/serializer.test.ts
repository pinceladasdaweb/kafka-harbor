import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { ERROR_CODES, isSerializationError, jsonSerializer, rawSerializer, stringSerializer } from '../../src/index'

describe('jsonSerializer', () => {
  const codec = jsonSerializer()

  test('round-trips plain data', () => {
    const value = { id: 'a1', total: 10.5, tags: ['x', 'y'], nested: { ok: true, none: null } }
    assert.deepEqual(codec.deserialize(codec.serialize(value, 't'), 't'), value)
  })

  test('encodes Date as ISO-8601, the one conversion accepted', () => {
    const at = new Date('2026-09-05T12:00:00.000Z')
    assert.deepEqual(codec.deserialize(codec.serialize({ at }, 't'), 't'), { at: '2026-09-05T12:00:00.000Z' })
  })

  test('rejects every shape JSON would silently flatten or drop', () => {
    const cases: Array<[string, unknown]> = [
      ['undefined', undefined],
      ['function', () => 1],
      ['symbol', Symbol('s')],
      ['bigint', 1n],
      ['NaN', Number.NaN],
      ['Infinity', Number.POSITIVE_INFINITY],
      ['Map', new Map([['a', 1]])],
      ['Set', new Set([1])],
      ['Buffer', Buffer.from('hi')],
      ['Uint8Array', new Uint8Array([1])],
      ['ArrayBuffer', new ArrayBuffer(2)],
      ['RegExp', /x/],
      ['Error', new Error('e')],
      ['Promise', Promise.resolve(1)],
      ['WeakMap', new WeakMap()],
      ['WeakSet', new WeakSet()],
      ['invalid Date', new Date(Number.NaN)],
      ['nested undefined', { a: { b: undefined } }],
      ['undefined in array', [1, undefined]],
      ['nested function', { f () {} }]
    ]
    for (const [label, value] of cases) {
      assert.throws(() => codec.serialize(value, 't'), (error: unknown) => {
        assert.ok(isSerializationError(error), `${label}: not a SerializationError`)
        assert.equal((error as { retryable: boolean }).retryable, false)
        return true
      }, `${label} should be rejected`)
    }
  })

  test('rejects cycles', () => {
    const value: { self?: unknown } = {}
    value.self = value
    assert.throws(() => codec.serialize(value, 't'), { code: ERROR_CODES.SERIALIZATION })
  })

  test('a getter that throws while encoding surfaces as a SerializationError with the cause', () => {
    const value = { get boom (): number { throw new Error('getter exploded') } }
    assert.throws(() => codec.serialize(value, 'orders'), (error: unknown) => {
      assert.ok(isSerializationError(error))
      assert.equal(((error as { cause: Error }).cause).message, 'getter exploded')
      return true
    })
  })

  test('names the path of the offending field, and nothing else', () => {
    assert.throws(() => codec.serialize({ order: { items: [1, { price: Number.NaN }] } }, 't'), (error: unknown) => {
      assert.equal((error as Error).message, 'value.order.items[1].price is NaN; JSON turns it into null')
      return true
    })
    assert.throws(() => codec.serialize(new Map(), 't'), { message: 'value is a Map; JSON would not preserve it' })
  })

  test('deserialization failure is a SerializationError carrying the topic', () => {
    assert.throws(() => codec.deserialize(Buffer.from('{not json'), 'orders'), (error: unknown) => {
      assert.ok(isSerializationError(error))
      assert.match((error as Error).message, /orders/)
      assert.ok((error as { cause: unknown }).cause instanceof SyntaxError)
      return true
    })
  })

  test('a shared object referenced twice is not a cycle', () => {
    const shared = { x: 1 }
    assert.doesNotThrow(() => codec.serialize({ a: shared, b: shared }, 't'))
  })
})

describe('rawSerializer and stringSerializer', () => {
  test('raw passes Buffers through and rejects anything else', () => {
    const codec = rawSerializer()
    const bytes = Buffer.from([1, 2, 3])
    assert.equal(codec.serialize(bytes, 't'), bytes)
    assert.equal(codec.deserialize(bytes, 't'), bytes)
    assert.throws(() => codec.serialize('nope' as unknown as Buffer, 't'), { code: ERROR_CODES.SERIALIZATION })
  })

  test('string encodes UTF-8 and rejects non-strings', () => {
    const codec = stringSerializer()
    assert.equal(codec.deserialize(codec.serialize('olá', 't'), 't'), 'olá')
    assert.throws(() => codec.serialize(42 as unknown as string, 't'), { code: ERROR_CODES.SERIALIZATION })
  })
})

import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { ERROR_CODES, isSerializationError, jsonSerializer, rawSerializer, stringSerializer, type TransactionOptions } from '../../src/index'
import { harness } from '../helpers/harness'

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

  test('rejects cycles, naming the cycle', () => {
    const value: { self?: unknown } = {}
    value.self = value
    assert.throws(() => codec.serialize(value, 't'), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.SERIALIZATION)
      assert.match((error as Error).message, /closes a cycle/)
      return true
    })
    const deep: { items: Array<{ back?: unknown }> } = { items: [{}] }
    deep.items[0]!.back = deep
    assert.throws(() => codec.serialize(deep, 't'), /items\[0\]\.back.*closes a cycle/)
  })

  test('a getter that throws inside a nested object surfaces its own error as the cause', () => {
    const value = { order: { get boom (): number { throw new Error('nested exploded') } } }
    assert.throws(() => codec.serialize(value, 'orders'), (error: unknown) => {
      assert.ok(isSerializationError(error))
      assert.equal(((error as { cause: Error }).cause).message, 'nested exploded')
      return true
    })
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

describe('jsonSerializer and toJSON', () => {
  const codec = jsonSerializer()

  test('an object with toJSON is checked as JSON reads it: through what toJSON returns, given its key', () => {
    class Money {
      readonly format = (): string => `${this.cents} cents`
      constructor (readonly cents: number) {}
      toJSON (): unknown { return { cents: this.cents } }
    }
    const keys: string[] = []
    const traced = { toJSON (key: string): string { keys.push(key); return 'traced' } }
    const value = { url: new URL('http://example.test/a'), price: new Money(5), traced, list: [traced] }
    assert.equal(codec.serialize(value, 't').toString(), JSON.stringify(value))
    assert.deepEqual(keys.slice(0, 2), ['traced', '0'], 'the property name, then the index, as JSON.stringify passes them')
    keys.length = 0
    codec.serialize(traced, 't')
    assert.equal(keys[0], '', 'the root is the empty key')
  })

  test('what toJSON returns is held to the same rules as any value', () => {
    assert.throws(() => codec.serialize({ a: { toJSON: () => undefined } }, 't'), (error: unknown) => isSerializationError(error) && /value\.a is a undefined/.test((error as Error).message))
    assert.throws(() => codec.serialize({ a: { toJSON: () => new Map() } }, 't'), (error: unknown) => isSerializationError(error) && /value\.a is a Map/.test((error as Error).message))
  })

  test('only own enumerable keys are checked, the ones JSON reads', () => {
    const inherited = Object.create({ hidden: undefined }) as Record<string, unknown>
    inherited.shown = 1
    assert.equal(codec.serialize(inherited, 't').toString(), '{"shown":1}')
  })
  test('toJSON is called once per value, as JSON does: what it returns is written by its own keys', () => {
    class Self {
      readonly a = 1
      toJSON (): unknown { return this }
    }
    assert.equal(codec.serialize({ x: new Self() }, 't').toString(), '{"x":{"a":1}}')
    assert.throws(() => codec.serialize({ x: { toJSON: () => new Date(0) } }, 't'), (error: unknown) => isSerializationError(error) && /value\.x is a Date returned by toJSON; JSON writes it as \{\}/.test((error as Error).message))
    // A function among the keys of what toJSON returned is dropped by JSON, as anywhere else.
    assert.throws(() => codec.serialize({ x: { toJSON: () => ({ b: 1, toJSON: () => undefined }) } }, 't'), (error: unknown) => isSerializationError(error) && /value\.x\.toJSON is a function/.test((error as Error).message))
  })

  test('a cycle that runs through toJSON is named as a cycle', () => {
    const looping: { toJSON: () => unknown } = { toJSON: () => ({ again: looping }) }
    assert.throws(() => codec.serialize({ x: looping }, 't'), (error: unknown) => isSerializationError(error) && /value\.x\.again closes a cycle/.test((error as Error).message))
  })
})

describe('serializers wherever the harbor takes one', () => {
  test('a serializer of any type fits the harbor, a consumer and a transaction, not only a producer', async () => {
    const h = harness({ serializer: stringSerializer(), transactionalId: 'svc' })
    h.harbor.consumer({ groupId: 'g', serializer: rawSerializer() })
    const options: TransactionOptions = { serializer: jsonSerializer<{ id: string }>() }
    await h.harbor.transaction(async (tx) => { await tx.send('orders', { value: { id: 'a' } }) }, options)
    await h.harbor.producer().send('notes', { value: 'plain text' })
    assert.equal(h.adapter.messages('orders')[0]?.value?.toString(), '{"id":"a"}')
    assert.equal(h.adapter.messages('notes')[0]?.value?.toString(), 'plain text')
    await h.harbor.shutdown()
  })
})

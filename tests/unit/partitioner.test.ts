import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { ERROR_CODES, murmur2, partitionForKey } from '../../src/index'

describe('murmur2', () => {
  test('matches Kafka\'s reference values', () => {
    // Vectors from an independent port of the Java client's algorithm; the
    // first two are the values the kafkajs suite also asserts.
    const vectors: Array<[Buffer, number]> = [
      [Buffer.from(''), 275646681],
      [Buffer.from('0'), 971027396],
      [Buffer.from('key'), -1079937367],
      [Buffer.from('orders-123'), 216622777],
      [Buffer.from([0xff, 0x00, 0x10]), -1536016136],
      [Buffer.from('ol\u00e1', 'utf8'), -1860657225]
    ]
    for (const [input, expected] of vectors) {
      assert.equal(murmur2(input), expected, input.toString('hex'))
    }
  })

  test('covers every tail length', () => {
    const seen = new Set<number>()
    for (const input of ['', 'a', 'ab', 'abc', 'abcd', 'abcde', 'abcdef', 'abcdefg', 'abcdefgh']) {
      seen.add(murmur2(Buffer.from(input)))
    }
    assert.equal(seen.size, 9)
  })
})

describe('partitionForKey', () => {
  test('is Kafka\'s default: positive murmur2 modulo the partition count', () => {
    assert.equal(partitionForKey('', 12), 9)
    assert.equal(partitionForKey('0', 12), 8)
    assert.equal(partitionForKey('key', 12), 1)
    assert.equal(partitionForKey(Buffer.from('key'), 12), 1)
    assert.equal(partitionForKey('ol\u00e1', 12), 11)
    assert.equal(partitionForKey('anything', 1), 0)
  })

  test('is stable and stays in range', () => {
    for (let i = 0; i < 200; i++) {
      const partition = partitionForKey(`k${i}`, 7)
      assert.ok(partition >= 0 && partition < 7)
      assert.equal(partitionForKey(`k${i}`, 7), partition)
    }
  })

  test('rejects a partition count that is not a positive integer', () => {
    for (const bad of [0, -1, 1.5, Number.NaN]) {
      assert.throws(() => partitionForKey('k', bad), { code: ERROR_CODES.CONFIG_INVALID })
    }
  })
})

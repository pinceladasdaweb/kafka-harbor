import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { ERROR_CODES } from '../../src/index'
import { firstRejection, requireNonEmptyString, requirePositiveInteger } from '../../src/validate'

describe('option validation helpers', () => {
  test('requireNonEmptyString returns the string and names what it got otherwise', () => {
    assert.equal(requireNonEmptyString('orders', 'topic'), 'orders')
    assert.throws(() => requireNonEmptyString('', 'topic'), { code: ERROR_CODES.CONFIG_INVALID, message: 'topic must be a non-empty string; got ""' })
    assert.throws(() => requireNonEmptyString(undefined, 'topic'), { message: 'topic must be a non-empty string; got undefined' })
    assert.throws(() => requireNonEmptyString(7, 'groupId'), { message: 'groupId must be a non-empty string; got number' })
  })

  test('requirePositiveInteger accepts integers from one up and rejects the rest, naming the value', () => {
    assert.equal(requirePositiveInteger(1, 'n'), 1)
    assert.equal(requirePositiveInteger(42, 'n'), 42)
    for (const value of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, '3', undefined, null]) {
      assert.throws(() => requirePositiveInteger(value, 'concurrency'), { code: ERROR_CODES.CONFIG_INVALID, message: `concurrency must be an integer >= 1; got ${String(value)}` })
    }
  })

  test('firstRejection finds the first rejected result, or nothing', async () => {
    const results = await Promise.allSettled([Promise.resolve(1), Promise.reject(new Error('a')), Promise.reject(new Error('b'))])
    assert.equal((firstRejection(results)?.reason as Error).message, 'a')
    assert.equal(firstRejection(await Promise.allSettled([Promise.resolve(1)])), undefined)
    assert.equal(firstRejection([]), undefined)
  })
})

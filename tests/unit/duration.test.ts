import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { ERROR_CODES, MAX_DURATION_MS, parseDuration } from '../../src/index'

describe('parseDuration', () => {
  test('numbers are milliseconds, zero included', () => {
    assert.equal(parseDuration(0, 'x'), 0)
    assert.equal(parseDuration(1500, 'x'), 1500)
  })

  test('strings need a unit', () => {
    assert.equal(parseDuration('250ms', 'x'), 250)
    assert.equal(parseDuration('5s', 'x'), 5_000)
    assert.equal(parseDuration('1m', 'x'), 60_000)
    assert.equal(parseDuration('2h', 'x'), 7_200_000)
    assert.equal(parseDuration('1d', 'x'), 86_400_000)
    assert.equal(parseDuration(' 1.5s ', 'x'), 1_500)
    assert.equal(parseDuration('1.25s', 'x'), 1_250)
    assert.equal(parseDuration('0.5ms', 'x'), 1)
  })

  test('rejects trailing garbage after the unit', () => {
    for (const value of ['5sx', '5s 1', '1m30s']) {
      assert.throws(() => parseDuration(value, 'x'), { code: ERROR_CODES.CONFIG_INVALID }, value)
    }
  })

  test('rejects bare digits, negatives, NaN and unknown units, naming the option', () => {
    for (const value of ['5', '-1s', '1w', '', 'abc', -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      assert.throws(() => parseDuration(value, 'retry.levels[0].delay'), (error: unknown) => {
        assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
        assert.match((error as Error).message, /retry\.levels\[0\]\.delay/)
        return true
      }, `value ${String(value)} should be rejected`)
    }
  })

  test('rejects anything a timer cannot hold, in either form', () => {
    assert.equal(parseDuration(MAX_DURATION_MS, 'x'), MAX_DURATION_MS)
    assert.equal(parseDuration('24d', 'x'), 24 * 86_400_000)
    assert.throws(() => parseDuration(MAX_DURATION_MS + 1, 'shutdown timeout'), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
      assert.match((error as Error).message, /shutdown timeout must not exceed 2147483647ms/)
      return true
    })
    assert.throws(() => parseDuration('25d', 'x'), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => parseDuration(Number.MAX_SAFE_INTEGER, 'x'), { code: ERROR_CODES.CONFIG_INVALID })
  })

  test('rejects non-string non-number input', () => {
    assert.throws(() => parseDuration(null as unknown as number, 'x'), { code: ERROR_CODES.CONFIG_INVALID })
  })
})

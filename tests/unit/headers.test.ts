import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { readRetryInfo, writeRetryInfo } from '../../src/headers'
import { decodeHeaders, headerNames } from '../../src/index'

const names = headerNames()

describe('headerNames', () => {
  test('defaults to the x- prefix shared with the RabbitMQ sibling', () => {
    assert.equal(names.correlationId, 'x-correlation-id')
    assert.equal(names.retryCount, 'x-retry-count')
    assert.equal(names.originalTopic, 'x-original-topic')
    assert.equal(names.firstFailureAt, 'x-first-failure-at')
    assert.equal(names.lastError, 'x-last-error')
    assert.equal(names.deadLetteredAt, 'x-dead-lettered-at')
    assert.equal(names.redrivenFrom, 'x-redriven-from')
    assert.equal(names.redrivenAt, 'x-redriven-at')
    assert.equal(names.prefix, 'x-')
  })

  test('the prefix is configurable, including empty', () => {
    assert.equal(headerNames('acme-').retryCount, 'acme-retry-count')
    assert.equal(headerNames('').producedAt, 'produced-at')
  })
})

describe('decodeHeaders', () => {
  test('decodes Buffers as UTF-8, keeps strings, drops undefined', () => {
    assert.deepEqual(decodeHeaders({ a: Buffer.from('x'), b: 'y', c: undefined }), { a: 'x', b: 'y' })
  })

  test('a repeated key keeps its last value', () => {
    assert.deepEqual(decodeHeaders({ a: ['1', Buffer.from('2'), '3'] }), { a: '3' })
    assert.deepEqual(decodeHeaders({ a: [] }), {})
  })
})

describe('readRetryInfo', () => {
  const valid = {
    [names.retryCount]: '2',
    [names.originalTopic]: 'orders',
    [names.firstFailureAt]: '2026-09-05T10:00:00.000Z',
    [names.lastError]: 'boom'
  }

  test('reads a consistent block', () => {
    assert.deepEqual(readRetryInfo(valid, names), {
      count: 2,
      originalTopic: 'orders',
      firstFailureAt: new Date('2026-09-05T10:00:00.000Z'),
      lastError: 'boom'
    })
  })

  test('a count with surrounding spaces or several digits is read', () => {
    assert.equal(readRetryInfo({ ...valid, [names.retryCount]: ' 2 ' }, names)?.count, 2)
    assert.equal(readRetryInfo({ ...valid, [names.retryCount]: '12' }, names)?.count, 12)
    assert.equal(readRetryInfo({ ...valid, [names.retryCount]: '123456789' }, names)?.count, 123_456_789)
  })

  test('a missing count discards the block', () => {
    const { [names.retryCount]: _dropped, ...withoutCount } = valid
    assert.equal(readRetryInfo(withoutCount, names), undefined)
  })

  test('a blank count is not zero: the block is discarded', () => {
    for (const count of ['', ' ', 'abc', '-1', '1.5', '0', '1e3', '9999999999']) {
      assert.equal(readRetryInfo({ ...valid, [names.retryCount]: count }, names), undefined, `count ${JSON.stringify(count)}`)
    }
  })

  test('a missing or blank original topic discards the block', () => {
    const { [names.originalTopic]: _dropped, ...withoutTopic } = valid
    assert.equal(readRetryInfo(withoutTopic, names), undefined)
    assert.equal(readRetryInfo({ ...valid, [names.originalTopic]: '  ' }, names), undefined)
  })

  test('an unreadable first-failure instant discards the block', () => {
    assert.equal(readRetryInfo({ ...valid, [names.firstFailureAt]: 'yesterday' }, names), undefined)
    assert.equal(readRetryInfo({ ...valid, [names.firstFailureAt]: '' }, names), undefined)
  })

  test('the last error is advisory: absent, the block survives with an empty string', () => {
    const { [names.lastError]: _dropped, ...withoutError } = valid
    assert.equal(readRetryInfo(withoutError, names)?.lastError, '')
  })
})

describe('writeRetryInfo', () => {
  const now = new Date('2026-09-05T12:00:00.000Z')

  test('a first failure starts the count at 1 and stamps the original topic and instant', () => {
    const out = writeRetryInfo({ 'x-correlation-id': 'c1' }, names, { previous: undefined, originalTopic: 'orders', error: 'boom', now })
    assert.deepEqual(out, {
      'x-correlation-id': 'c1',
      [names.retryCount]: '1',
      [names.originalTopic]: 'orders',
      [names.firstFailureAt]: now.toISOString(),
      [names.lastError]: 'boom'
    })
  })

  test('a later failure increments the count and keeps the first instant and topic', () => {
    const first = new Date('2026-09-05T10:00:00.000Z')
    const out = writeRetryInfo({}, names, {
      previous: { count: 2, originalTopic: 'orders', firstFailureAt: first, lastError: 'old' },
      originalTopic: 'orders-retry-2',
      error: 'new',
      now
    })
    assert.equal(out[names.retryCount], '3')
    assert.equal(out[names.originalTopic], 'orders')
    assert.equal(out[names.firstFailureAt], first.toISOString())
    assert.equal(out[names.lastError], 'new')
  })

  test('does not mutate the input headers', () => {
    const input = { a: '1' }
    writeRetryInfo(input, names, { previous: undefined, originalTopic: 't', error: 'e', now })
    assert.deepEqual(input, { a: '1' })
  })
})

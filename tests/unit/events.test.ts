import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { createEmitter, subscribe } from '../../src/events'

describe('createEmitter', () => {
  test('several listeners on one event all fire, in registration order', () => {
    const emitter = createEmitter<{ hit: number }>(() => {})
    const seen: string[] = []
    emitter.on('hit', (n) => { seen.push(`a${n}`) }).on('hit', (n) => { seen.push(`b${n}`) })
    emitter.emit('hit', 1)
    assert.deepEqual(seen, ['a1', 'b1'])
  })

  test('off() before any on() is a no-op, and off() removes only the given listener', () => {
    const emitter = createEmitter<{ hit: number }>(() => {})
    const noop = (): void => {}
    assert.doesNotThrow(() => emitter.off('hit', noop))
    const seen: number[] = []
    const keep = (n: number): void => { seen.push(n) }
    emitter.on('hit', keep).on('hit', noop).off('hit', noop)
    emitter.emit('hit', 2)
    assert.deepEqual(seen, [2])
  })

  test('a throwing listener is reported and the others still run; emitting with no listener is fine', () => {
    const reported: unknown[] = []
    const emitter = createEmitter<{ hit: number, other: string }>((error) => { reported.push(error) })
    const seen: number[] = []
    emitter.on('hit', () => { throw new Error('bug') }).on('hit', (n) => { seen.push(n) })
    emitter.emit('hit', 3)
    emitter.emit('other', 'nobody listens')
    assert.deepEqual(seen, [3])
    assert.equal((reported[0] as Error).message, 'bug')
  })

  test('subscribing during an emit does not affect the event in flight', () => {
    const emitter = createEmitter<{ hit: number }>(() => {})
    let late = 0
    emitter.on('hit', () => { emitter.on('hit', () => { late++ }) })
    emitter.emit('hit', 1)
    assert.equal(late, 0)
    emitter.emit('hit', 2)
    assert.equal(late, 1)
  })
})

describe('subscribe', () => {
  test('registers every listener of the map and the returned function removes exactly those', () => {
    const emitter = createEmitter<{ hit: number, miss: number }>(() => {})
    const seen: string[] = []
    emitter.on('hit', (n) => { seen.push(`kept${n}`) })
    const unsubscribe = subscribe(emitter, { hit: (n) => { seen.push(`hit${n}`) }, miss: (n) => { seen.push(`miss${n}`) } })
    emitter.emit('hit', 1)
    emitter.emit('miss', 2)
    unsubscribe()
    unsubscribe()
    emitter.emit('hit', 3)
    emitter.emit('miss', 4)
    assert.deepEqual(seen, ['kept1', 'hit1', 'miss2', 'kept3'])
  })

  test('a member spelled out as undefined is no listener: emitting that event stays silent', () => {
    const failures: unknown[] = []
    const emitter = createEmitter<{ hit: number, miss: number }>((error) => { failures.push(error) })
    const seen: number[] = []
    subscribe(emitter, { hit: (n: number) => { seen.push(n) }, miss: undefined })
    emitter.emit('miss', 1)
    emitter.emit('hit', 2)
    assert.deepEqual(seen, [2])
    assert.deepEqual(failures, [])
  })
})

import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { systemClock } from '../../src/index'

describe('systemClock', () => {
  test('now() is the wall clock', () => {
    const before = Date.now()
    const now = systemClock.now()
    assert.ok(now >= before && now <= Date.now())
  })

  test('sleep waits about the requested time', async () => {
    const started = Date.now()
    await systemClock.sleep(30)
    assert.ok(Date.now() - started >= 25)
  })

  test('sleep resolves early when the signal aborts, and at once when it already has', async () => {
    const controller = new AbortController()
    const started = Date.now()
    const sleeping = systemClock.sleep(5_000, controller.signal)
    setTimeout(() => controller.abort(), 10)
    await sleeping
    assert.ok(Date.now() - started < 1_000)

    const aborted = new AbortController()
    aborted.abort()
    const again = Date.now()
    await systemClock.sleep(5_000, aborted.signal)
    assert.ok(Date.now() - again < 1_000)
  })

  test('an aborted sleep clears its timer so nothing keeps the process alive', async () => {
    const before = process.getActiveResourcesInfo().filter((r) => r === 'Timeout').length
    const controller = new AbortController()
    const sleeping = systemClock.sleep(60_000, controller.signal)
    assert.equal(process.getActiveResourcesInfo().filter((r) => r === 'Timeout').length, before + 1)
    controller.abort()
    await sleeping
    assert.equal(process.getActiveResourcesInfo().filter((r) => r === 'Timeout').length, before)
  })

  test('a sleep that completes removes its abort listener', async () => {
    const controller = new AbortController()
    let listeners = 0
    const original = controller.signal.addEventListener.bind(controller.signal)
    const originalRemove = controller.signal.removeEventListener.bind(controller.signal)
    controller.signal.addEventListener = ((...args: Parameters<typeof original>) => { listeners++; original(...args) }) as typeof original
    controller.signal.removeEventListener = ((...args: Parameters<typeof originalRemove>) => { listeners--; originalRemove(...args) }) as typeof originalRemove
    await systemClock.sleep(5, controller.signal)
    assert.equal(listeners, 0)
  })
})

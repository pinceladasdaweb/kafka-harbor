import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { circuitBreaker, type BreakerState, type CircuitBreakerPolicy } from 'breakwater'

import { AbortProcessingError, BatchFailedError, ERROR_CODES, defaultFailureIf, isHoldExpiredError, type HoldExpiredError, type HarborEvents } from '../../src/index'
import { captureEvents, harness, text } from '../helpers/harness'
import { settle, until } from '../helpers/manual-clock'

interface StateChange { from: BreakerState, to: BreakerState }

/** A breaker whose circuit the test opens and closes by hand; rejects the way breakwater does, by code. */
const fakeBreaker = () => {
  let open = false
  let disposed = 0
  const executions: number[] = []
  const listeners = new Set<(change: StateChange) => void>()
  const policy = {
    kind: 'circuitBreaker',
    get state () { return open ? 'open' : 'closed' },
    execute: async <T>(fn: (context: unknown) => Promise<T> | T): Promise<T> => {
      if (open) throw Object.assign(new Error('Circuit breaker is open'), { code: 'CIRCUIT_OPEN', retryable: false })
      executions.push(Date.now())
      return await fn({})
    },
    stats: () => ({ state: open ? 'open' : 'closed', successes: 0, failures: 0, totalCalls: executions.length, failureRate: 0 }),
    on: (_event: string, listener: (change: StateChange) => void) => { listeners.add(listener); return policy },
    off: (_event: string, listener: (change: StateChange) => void) => { listeners.delete(listener); return policy },
    dispose: () => { disposed++ }
  }
  const setOpen = (value: boolean): void => {
    const from: BreakerState = open ? 'open' : 'closed'
    open = value
    for (const listener of listeners) listener({ from, to: value ? 'open' : 'closed' })
  }
  return { policy: policy as unknown as CircuitBreakerPolicy, setOpen, executions, listeners, disposed: () => disposed }
}

const changesOf = (events: Array<HarborEvents['circuitStateChanged']>): string[] => events.map((event) => `${event.topic}:${event.from}->${event.to}`)

describe('consumer circuit breaker', () => {
  test('the handler runs through the breaker; while the circuit is open the partition is held, nothing committed and nothing forwarded, and it resumes when the circuit closes', async () => {
    const h = harness()
    const { policy, setOpen, executions } = fakeBreaker()
    const changes = captureEvents(h.harbor, 'circuitStateChanged')
    const failed = captureEvents(h.harbor, 'messageFailed')
    const seen: unknown[] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '1s' }] } })
    consumer.subscribe('orders', (message) => { seen.push(message.value) }, { breaker: { policy } })
    await consumer.start()
    const producer = h.harbor.producer()
    await producer.send('orders', { value: 'a' })
    await until(() => h.adapter.committed('g', 'orders', 0) === '1')
    assert.equal(executions.length, 1, 'the handler ran inside the policy')

    setOpen(true)
    await producer.send('orders', { value: 'b' })
    await producer.send('orders', { value: 'c' })
    await until(() => h.clock.waiting === 1)
    assert.deepEqual(seen, ['a'])
    assert.equal(h.adapter.committed('g', 'orders', 0), '1')
    assert.equal(h.adapter.messages('orders-retry-1').length, 0)
    assert.equal(failed.length, 0)
    // The hold asks again every second, for as long as the circuit stays open.
    h.clock.advance(1_000)
    await until(() => h.clock.sleeps.length === 2 && h.clock.waiting === 1)
    assert.deepEqual(h.clock.sleeps, [1_000, 1_000])
    assert.equal(h.adapter.committed('g', 'orders', 0), '1')

    setOpen(false)
    h.clock.advance(1_000)
    await until(() => h.adapter.committed('g', 'orders', 0) === '3')
    assert.deepEqual(seen, ['a', 'b', 'c'])
    assert.equal(failed.length, 0)
    assert.deepEqual(changesOf(changes), ['orders:closed->open', 'orders:open->closed'])
    assert.deepEqual(changes.map((event) => event.groupId), ['g', 'g'])
    assert.ok(h.logs.some((entry) => entry.level === 'warn' && entry.message.includes('circuit for "orders" closed -> open')))
    assert.ok(h.logs.some((entry) => entry.level === 'info' && entry.message.includes('circuit for "orders" open -> closed')))
    await h.harbor.shutdown()
  })

  test('a hold sleeps in one-second looks and a last shorter one up to the deadline', async () => {
    const h = harness()
    const { policy, setOpen } = fakeBreaker()
    const failed = captureEvents(h.harbor, 'messageFailed')
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, breaker: { policy, hold: '1500ms' } })
    consumer.subscribe('orders', () => {})
    await consumer.start()
    setOpen(true)
    await h.harbor.producer().send('orders', { value: 'a' })
    await until(() => h.clock.waiting === 1)
    h.clock.advance(1_000)
    await until(() => h.clock.sleeps.length === 2)
    h.clock.advance(500)
    await until(() => failed.length === 1)
    assert.deepEqual(h.clock.sleeps, [1_000, 500])
    assert.equal((failed[0]?.error as HoldExpiredError).heldMs, 1_500)
    await h.harbor.shutdown()
  })

  test('a rejection that is not the circuit (the breaker could not decide) is the handler\'s failure, not a hold', async () => {
    const h = harness()
    const failed = captureEvents(h.harbor, 'messageFailed')
    const policy = {
      execute: async () => { throw Object.assign(new Error('state store unreachable'), { code: 'STATE_STORE_DOWN' }) },
      stats: () => ({}),
      on: () => policy,
      off: () => policy,
      dispose: () => {}
    } as unknown as CircuitBreakerPolicy
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, breaker: { policy } })
    consumer.subscribe('orders', () => {})
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 'a' })
    await until(() => failed.length === 1)
    assert.equal(failed[0]?.outcome, 'dead-letter')
    assert.equal((failed[0]?.error as Error).message, 'state store unreachable')
    assert.equal(h.clock.waiting, 0)
    await h.harbor.shutdown()
  })

  test('a hold that runs out fails the message with a retryable HoldExpiredError, so it walks the ladder; the default hold is maxProcessingTime', async () => {
    for (const [options, expectedHeldMs] of [[{ breaker: { hold: '3s' } }, 3_000], [{ maxProcessingTime: '5s', breaker: {} }, 5_000]] as const) {
      const h = harness()
      const { policy, setOpen } = fakeBreaker()
      const failed = captureEvents(h.harbor, 'messageFailed')
      const retried = captureEvents(h.harbor, 'messageRetried')
      const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '4s' }] }, ...(options.maxProcessingTime !== undefined && { maxProcessingTime: options.maxProcessingTime }) })
      consumer.subscribe('orders', () => { throw new Error('never runs') }, { breaker: { ...options.breaker, policy } })
      await consumer.start()
      setOpen(true)
      await h.harbor.producer().send('orders', { value: 'a' })
      await until(() => h.clock.waiting === 1)
      for (let held = 0; held < expectedHeldMs; held += 1_000) {
        h.clock.advance(1_000)
        await settle()
      }
      await until(() => failed.length === 1)
      const error = failed[0]?.error as HoldExpiredError
      assert.equal(failed[0]?.outcome, 'retry')
      assert.ok(isHoldExpiredError(error))
      assert.equal(error.retryable, true)
      assert.equal(error.topic, 'orders')
      assert.equal(error.heldMs, expectedHeldMs)
      assert.equal((error.cause as { code: string }).code, 'CIRCUIT_OPEN')
      assert.equal(retried[0]?.retryTopic, 'orders-retry-1')
      const [hop] = h.adapter.messages('orders-retry-1')
      assert.equal(text(hop?.value ?? null), '"a"')
      assert.match(String(hop?.headers['x-last-error']), new RegExp(`circuit for "orders" stayed open for ${expectedHeldMs}ms`))
      assert.equal(h.adapter.committed('g', 'orders', 0), '1')
      await h.harbor.shutdown()
    }
  })

  test('a hold longer than maxProcessingTime is refused, and so is a hold that is not a duration', () => {
    const h = harness()
    const consumer = h.harbor.consumer({ groupId: 'g', maxProcessingTime: '1m' })
    assert.throws(() => consumer.subscribe('orders', () => {}, { breaker: { hold: '2m' } }), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
      assert.match((error as Error).message, /breaker.hold for "orders" \(120000ms\) exceeds maxProcessingTime \(60000ms\)/)
      return true
    })
    assert.throws(() => consumer.subscribe('orders', () => {}, { breaker: { hold: 'soon' } }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.doesNotThrow(() => consumer.subscribe('orders', () => {}, { breaker: { hold: '1m' } }))
  })

  test('a stop during the hold leaves the message uncommitted and the consumer stops cleanly', async () => {
    const h = harness()
    const { policy, setOpen } = fakeBreaker()
    const failed = captureEvents(h.harbor, 'messageFailed')
    const stopped = captureEvents(h.harbor, 'consumerStopped')
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, breaker: { policy } })
    consumer.subscribe('orders', () => {})
    await consumer.start()
    setOpen(true)
    await h.harbor.producer().send('orders', { value: 'a' })
    await until(() => h.clock.waiting === 1)
    await h.harbor.shutdown()
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    assert.equal(h.adapter.messages('orders-dlq').length, 0)
    assert.equal(failed.length, 0)
    assert.deepEqual(stopped.map((event) => event.reason), ['shutdown'])
    assert.equal(h.clock.waiting, 0, 'the hold was cut, not left sleeping')

    const batched = harness()
    const held = fakeBreaker()
    const batchFailed = captureEvents(batched.harbor, 'messageFailed')
    const batchConsumer = batched.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '1m' }] } })
    batchConsumer.subscribeBatch('orders', () => {}, { size: 2, breaker: { policy: held.policy } })
    await batchConsumer.start()
    held.setOpen(true)
    await batched.harbor.producer().sendBatch('orders', [{ value: 1 }, { value: 2 }])
    await until(() => batched.clock.waiting === 1)
    await batched.harbor.shutdown()
    assert.equal(batched.adapter.committed('g', 'orders', 0), undefined)
    assert.equal(batched.adapter.messages('orders-retry-1').length, 0)
    assert.equal(batchFailed.length, 0)
  })

  test('a batch handler is guarded the same way: held while open, down the ladder when the hold runs out', async () => {
    const h = harness()
    const { policy, setOpen, executions } = fakeBreaker()
    const batches = captureEvents(h.harbor, 'batchProcessed')
    const failed = captureEvents(h.harbor, 'messageFailed')
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '10s' }] } })
    consumer.subscribeBatch('orders', () => {}, { size: 2, breaker: { policy, hold: '2s' } })
    await consumer.start()
    const producer = h.harbor.producer()
    setOpen(true)
    await producer.sendBatch('orders', [{ value: 1 }, { value: 2 }])
    await until(() => h.clock.waiting === 1)
    assert.equal(executions.length, 0)
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    setOpen(false)
    h.clock.advance(1_000)
    await until(() => h.adapter.committed('g', 'orders', 0) === '2')
    assert.deepEqual(batches.map((event) => event.outcome), ['processed'])
    assert.equal(executions.length, 1, 'one execution for the whole batch')

    setOpen(true)
    await producer.sendBatch('orders', [{ value: 3 }, { value: 4 }])
    await until(() => h.clock.waiting === 1)
    h.clock.advance(1_000)
    await settle()
    h.clock.advance(1_000)
    await until(() => failed.length === 2)
    assert.deepEqual(failed.map((event) => [event.offset, event.outcome, (event.error as { code: string }).code, event.batch]), [['2', 'retry', ERROR_CODES.HOLD_EXPIRED, 2], ['3', 'retry', ERROR_CODES.HOLD_EXPIRED, 2]])
    assert.deepEqual(batches.map((event) => event.outcome), ['processed', 'retry'])
    assert.equal(h.adapter.messages('orders-retry-1').length, 2)
    assert.equal(h.adapter.committed('g', 'orders', 0), '4')
    await h.harbor.shutdown()
  })

  test('a breaker of breakwater opens after the configured failures, holds the partition, and closes on a successful probe', async () => {
    const h = harness()
    const changes = captureEvents(h.harbor, 'circuitStateChanged')
    const retried = captureEvents(h.harbor, 'messageRetried')
    // halfOpenAfter is real time inside breakwater; short, so the probe is allowed after a moment.
    const policy = circuitBreaker({ consecutiveFailures: 2, halfOpenAfter: 300, halfOpenCalls: 1, failureIf: defaultFailureIf })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '1m' }] }, breaker: { policy, hold: '1m' } })
    const seen: unknown[] = []
    consumer.subscribe<{ fail: boolean }>('orders', (message) => {
      if (message.value.fail) throw new Error('downstream is down')
      seen.push(message.value)
    })
    await consumer.start()
    const producer = h.harbor.producer<{ fail: boolean }>()
    await producer.send('orders', { value: { fail: true } })
    await producer.send('orders', { value: { fail: true } })
    await until(() => retried.length === 2)
    assert.equal(policy.state, 'open')
    assert.deepEqual(changesOf(changes), ['orders:closed->open'])

    await producer.send('orders', { value: { fail: false } })
    await until(() => h.clock.waiting === 1, 2_000)
    assert.deepEqual(seen, [], 'held, not run')
    await new Promise((resolve) => setTimeout(resolve, 350))
    h.clock.advance(1_000)
    await until(() => seen.length === 1)
    assert.equal(policy.state, 'closed')
    assert.deepEqual(changesOf(changes), ['orders:closed->open', 'orders:open->half-open', 'orders:half-open->closed'])
    assert.equal(h.adapter.committed('g', 'orders', 0), '3')
    await h.harbor.shutdown()
    // A shared policy is left as it is: the consumer only stops listening to it.
    assert.equal(policy.state, 'closed')
  })

  test('a breaker the consumer builds ignores failures the handler declared deterministic, and aborts; a topic may opt out', async () => {
    const h = harness()
    const changes = captureEvents(h.harbor, 'circuitStateChanged')
    const deadLettered = captureEvents(h.harbor, 'messageDeadLettered')
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, breaker: { consecutiveFailures: 2 } })
    consumer.subscribe('orders', () => { throw Object.assign(new Error('bad order'), { retryable: false }) })
    consumer.subscribe('plain', () => { throw Object.assign(new Error('bad too'), { retryable: false }) }, { breaker: false })
    await consumer.start()
    const producer = h.harbor.producer()
    await producer.sendBatch('orders', [{ value: 1 }, { value: 2 }, { value: 3 }])
    await producer.sendBatch('plain', [{ value: 1 }, { value: 2 }, { value: 3 }])
    await until(() => deadLettered.length === 6)
    assert.deepEqual(changes, [], 'deterministic failures never open the circuit')
    assert.equal(h.clock.waiting, 0)
    await h.harbor.shutdown()

    // The same consumer-built breaker does open on transient failures, with the configured count.
    const flaky = harness()
    const flakyChanges = captureEvents(flaky.harbor, 'circuitStateChanged')
    const flakyConsumer = flaky.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '1m' }] }, breaker: { consecutiveFailures: 2 } })
    flakyConsumer.subscribe('orders', () => { throw new Error('downstream is down') })
    await flakyConsumer.start()
    await flaky.harbor.producer().sendBatch('orders', [{ value: 1 }, { value: 2 }, { value: 3 }])
    await until(() => flakyChanges.length === 1)
    assert.deepEqual(changesOf(flakyChanges), ['orders:closed->open'])
    await until(() => flaky.clock.sleeps.includes(1_000), 2_000)
    await flaky.harbor.shutdown()

    assert.equal(defaultFailureIf(new Error('transient')), true)
    assert.equal(defaultFailureIf(Object.assign(new Error('deterministic'), { retryable: false })), false)
    assert.equal(defaultFailureIf(new AbortProcessingError(new Error('stop'))), false)
    assert.equal(defaultFailureIf(new BatchFailedError([], new Error('one straggler'))), false, 'a batch that resolved for the rest reached the dependency')
    assert.equal(defaultFailureIf('a string'), true)
  })

  test('an isolated circuit holds the partition too, and lets it go when unisolated', async () => {
    const h = harness()
    const changes = captureEvents(h.harbor, 'circuitStateChanged')
    const policy = circuitBreaker({ consecutiveFailures: 3 })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, breaker: { policy } })
    const seen: unknown[] = []
    consumer.subscribe('orders', (message) => { seen.push(message.value) })
    await consumer.start()
    await policy.isolate()
    await h.harbor.producer().send('orders', { value: 'a' })
    await until(() => h.clock.waiting === 1)
    assert.deepEqual(seen, [])
    assert.equal(h.adapter.messages('orders-dlq').length, 0, 'isolation is a hold, never a verdict')
    await policy.unisolate()
    h.clock.advance(1_000)
    await until(() => seen.length === 1)
    assert.deepEqual(changesOf(changes), ['orders:closed->isolated', 'orders:isolated->closed'])
    assert.ok(h.logs.some((entry) => entry.level === 'warn' && entry.message.includes('closed -> isolated')))
    await h.harbor.shutdown()
  })

  test('a handler that throws a breaker rejection of its own ran and failed: no hold, its verdict stands', async () => {
    const h = harness()
    const { policy, executions } = fakeBreaker()
    const failed = captureEvents(h.harbor, 'messageFailed')
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '1m' }] }, breaker: { policy } })
    consumer.subscribe('orders', () => { throw Object.assign(new Error('the payment gateway circuit is open'), { code: 'CIRCUIT_OPEN', retryable: false }) })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 'a' })
    await until(() => failed.length === 1)
    assert.equal(executions.length, 1, 'ran once, not every second')
    assert.equal(failed[0]?.outcome, 'dead-letter')
    assert.equal(h.clock.waiting, 0)
    await h.harbor.shutdown()
  })

  test('a rebalance taking the partition away ends the hold with the message uncommitted', async () => {
    const h = harness({}, { partitions: 2 })
    const { policy, setOpen } = fakeBreaker()
    const failed = captureEvents(h.harbor, 'messageFailed')
    let revoke: ((partitions: Array<{ topic: string, partition: number }>) => Promise<void>) | undefined
    const original = h.adapter.consume
    h.adapter.consume = async (options) => {
      revoke = options.onPartitionsRevoked as typeof revoke
      return await original(options)
    }
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, breaker: { policy } })
    consumer.subscribe('orders', () => {})
    await consumer.start()
    setOpen(true)
    await h.harbor.producer().send('orders', { value: 'a', partition: 0 })
    await until(() => h.clock.waiting === 1)
    const revoking = (revoke as NonNullable<typeof revoke>)([{ topic: 'orders', partition: 0 }])
    await settle()
    // The hold notices at its next look, one second at most.
    h.clock.advance(1_000)
    await revoking
    assert.equal(h.adapter.committed('g', 'orders', 0), undefined)
    assert.equal(failed.length, 0)
    await h.harbor.shutdown()
  })

  test('deeper on the ladder the hold is what the delivery tolerates: the retry delay already spent counts against it', async () => {
    const h = harness()
    const { policy, setOpen } = fakeBreaker()
    const failed = captureEvents(h.harbor, 'messageFailed')
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, maxProcessingTime: '5s', retry: { levels: [{ delay: '1s' }, { delay: '4s' }] }, breaker: { policy, hold: '2s' } })
    consumer.subscribe('orders', () => {})
    await consumer.start()
    setOpen(true)
    await h.harbor.producer().send('orders', { value: 'a' })
    for (let step = 0; step < 20 && failed.length < 3; step++) {
      await until(() => h.clock.waiting === 1)
      h.clock.advance(1_000)
      await settle()
    }
    await until(() => failed.length === 3)
    assert.deepEqual(failed.map((event) => [event.topic, event.outcome, (event.error as HoldExpiredError).heldMs]), [
      ['orders', 'retry', 2_000],
      ['orders-retry-1', 'retry', 2_000],
      // Four of the five tolerated seconds went to the delay: one second of hold is what is left.
      ['orders-retry-2', 'dead-letter', 1_000]
    ])
    await h.harbor.shutdown()
  })

  test('a breaker is released when start() fails or when the subscription is refused, not only on a stop', async () => {
    const h = harness({}, { autoCreateTopics: false })
    const { policy, listeners } = fakeBreaker()
    const consumer = h.harbor.consumer({ groupId: 'g', breaker: { policy } })
    consumer.subscribe('orders', () => {})
    assert.equal(listeners.size, 1)
    assert.throws(() => consumer.subscribe('orders', () => {}), { code: ERROR_CODES.CONFIG_INVALID })
    assert.throws(() => consumer.subscribeBatch('orders', () => {}), { code: ERROR_CODES.CONFIG_INVALID })
    assert.equal(listeners.size, 1, 'the refused subscriptions left nothing behind')
    consumer.subscribe('plain', () => {}, { breaker: false })
    assert.throws(() => consumer.subscribe('plain', () => {}, { breaker: false }), { code: ERROR_CODES.CONFIG_INVALID })
    assert.equal(listeners.size, 1, 'a refused topic without a breaker releases nobody else\'s')
    await assert.rejects(consumer.start(), { code: ERROR_CODES.TOPIC_MISSING })
    assert.equal(listeners.size, 0)
    assert.equal(consumer.status, 'stopped')
  })

  test('the consumer stops listening to a shared breaker when it stops, and does not release it', async () => {
    const h = harness()
    const { policy, setOpen, listeners, disposed } = fakeBreaker()
    const changes = captureEvents(h.harbor, 'circuitStateChanged')
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, breaker: { policy } })
    consumer.subscribe('orders', () => {})
    consumer.subscribe('payments', () => {})
    await consumer.start()
    assert.equal(listeners.size, 2, 'one listener per guarded topic')
    setOpen(true)
    assert.deepEqual(changesOf(changes).sort(), ['orders:closed->open', 'payments:closed->open'])
    await h.harbor.shutdown()
    assert.equal(listeners.size, 0)
    assert.equal(disposed(), 0)
    setOpen(false)
    assert.equal(changes.length, 2, 'nothing reported after the stop')
  })
})

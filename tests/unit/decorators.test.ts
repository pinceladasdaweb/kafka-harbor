import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { BatchFailedError, ERROR_CODES, type BatchContext, type HandlerContext, type Message } from '../../src/index'
import { DLQHandler, KafkaBatchListener, KafkaConsumer, KafkaListener, KafkaRetry, bindListeners, consumerOptionsOf, hasListeners, listenersOf } from '../../src/decorators/index'
import { captureEvents, harness } from '../helpers/harness'
import { until } from '../helpers/manual-clock'

interface Order { id: string, total: number }

describe('kafka-harbor/decorators without a framework', () => {
  test('bindListeners() builds the consumer a decorated class declares, calling the methods on the instance', async () => {
    @KafkaConsumer({ groupId: 'orders-service', fromBeginning: true, autoCreateTopics: true })
    @KafkaRetry({ levels: [{ delay: 0 }] })
    class OrdersListener {
      readonly seen: string[] = []
      attempts = 0

      @KafkaListener<Order>('orders')
      onOrder (message: Message<Order>, ctx: HandlerContext): void {
        this.attempts++
        if (message.value.id === 'flaky' && ctx.attempt === 1) throw new Error('first attempt fails')
        this.seen.push(`${message.value.id}@${ctx.attempt}`)
      }

      @KafkaBatchListener<Order>('invoices', { size: 2 })
      onInvoices (messages: Array<Message<Order>>, ctx: BatchContext): void {
        this.seen.push(`invoices[${ctx.partition}]:${messages.map((message) => message.value.id).join(',')}`)
      }
    }

    const listener = new OrdersListener()
    assert.equal(hasListeners(listener), true)
    assert.deepEqual(listenersOf(listener).map((entry) => [entry.kind, entry.kind === 'dlq' ? entry.original : entry.topic, entry.method]).sort(), [['batch', 'invoices', 'onInvoices'], ['each', 'orders', 'onOrder']])
    assert.deepEqual(consumerOptionsOf(listener), { groupId: 'orders-service', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })

    const h = harness()
    const processed = captureEvents(h.harbor, 'messageProcessed')
    const consumer = bindListeners(h.harbor, listener)
    assert.equal(consumer.groupId, 'orders-service')
    assert.equal(consumer.status, 'idle', 'bound, not started')
    await consumer.start()
    const producer = h.harbor.producer<Order>()
    await producer.sendBatch('orders', [{ value: { id: 'a', total: 1 } }, { value: { id: 'flaky', total: 2 } }])
    await producer.sendBatch('invoices', [{ value: { id: 'i1', total: 1 } }, { value: { id: 'i2', total: 1 } }])
    // a, flaky (second attempt, on the retry topic), i1 and i2; the first attempt of flaky failed.
    await until(() => processed.length === 4)
    assert.deepEqual(listener.seen.sort(), ['a@1', 'flaky@2', 'invoices[0]:i1,i2'])
    assert.equal(listener.attempts, 3, 'the retry ladder came from @KafkaRetry')
    await h.harbor.shutdown()
  })

  test('@DLQHandler subscribes to the dead-letter topic of the original, named by the consumer\'s naming', async () => {
    @KafkaConsumer({ groupId: 'dlq-watch', fromBeginning: true, autoCreateTopics: true, dlq: { topicNaming: (topic) => `${topic}.dead` } })
    class DeadLetters {
      readonly seen: string[] = []

      @DLQHandler<Order>('orders')
      onDead (message: Message<Order>): void { this.seen.push(message.value.id) }
    }

    const h = harness()
    h.adapter.createTopic('orders.dead')
    const dead = new DeadLetters()
    const consumer = bindListeners(h.harbor, dead)
    await consumer.start()
    await h.harbor.producer<Order>().send('orders.dead', { value: { id: 'lost', total: 1 } })
    await h.adapter.whenDrained('dlq-watch', 'orders.dead')
    assert.deepEqual(dead.seen, ['lost'])
    await h.harbor.shutdown()

    class Disabled {
      @DLQHandler('orders')
      onDead (): void {}
    }
    assert.throws(() => bindListeners(h.harbor, new Disabled(), { groupId: 'x', dlq: { enabled: false } }), /handles the DLQ of "orders", but the consumer has the DLQ disabled/)
  })

  test('overrides fill in or replace the class options; a subclass inherits and refines its parent\'s listeners', async () => {
    class Base {
      readonly seen: string[] = []

      @KafkaListener('orders')
      onOrder (message: Message): void { this.seen.push(`base:${String(message.value)}`) }
    }
    @KafkaConsumer({ groupId: 'child', fromBeginning: true, autoCreateTopics: true })
    class Child extends Base {
      @KafkaListener('payments')
      onPayment (message: Message): void { this.seen.push(`child:${String(message.value)}`) }
    }

    const h = harness()
    const child = new Child()
    assert.deepEqual(listenersOf(child).map((entry) => entry.method).sort(), ['onOrder', 'onPayment'])
    const consumer = bindListeners(h.harbor, child, { groupId: 'overridden' })
    assert.equal(consumer.groupId, 'overridden')
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await h.harbor.producer().send('payments', { value: 2 })
    await h.adapter.whenDrained('overridden', 'orders')
    await h.adapter.whenDrained('overridden', 'payments')
    assert.deepEqual(child.seen.sort(), ['base:1', 'child:2'])
    await h.harbor.shutdown()
  })

  test('a method a subclass overrides and decorates again is one listener, the nearest declaration winning', async () => {
    class Base {
      readonly seen: string[] = []

      @KafkaListener('orders')
      onOrder (message: Message): void { this.seen.push(`base:${String(message.value)}`) }
    }
    @KafkaConsumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    class Child extends Base {
      @KafkaListener('orders-v2')
      override onOrder (message: Message): void { this.seen.push(`child:${String(message.value)}`) }
    }
    const child = new Child()
    assert.deepEqual(listenersOf(child).map((entry) => [entry.kind === 'dlq' ? entry.original : entry.topic, entry.method]), [['orders-v2', 'onOrder']])
    const h = harness()
    const consumer = bindListeners(h.harbor, child)
    await consumer.start()
    await h.harbor.producer().send('orders-v2', { value: 1 })
    await h.adapter.whenDrained('g', 'orders-v2')
    assert.deepEqual(child.seen, ['child:1'])
    assert.equal(h.adapter.messages('orders').length, 0)
    await h.harbor.shutdown()
  })

  test('a class without a group, without listeners, or with a decorated non-method is refused with a ConfigError', () => {
    const h = harness()
    class Bare {
      @KafkaListener('orders')
      onOrder (): void {}
    }
    assert.throws(() => bindListeners(h.harbor, new Bare()), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.CONFIG_INVALID)
      assert.match((error as Error).message, /Bare has no consumer group/)
      return true
    })
    class Nothing {}
    assert.equal(hasListeners(new Nothing()), false)
    assert.equal(hasListeners(null), false)
    assert.throws(() => bindListeners(h.harbor, new Nothing(), { groupId: 'g' }), /Nothing declares no @KafkaListener/)
    const shadowed = new Bare()
    Object.defineProperty(shadowed, 'onOrder', { value: 'not a function' })
    assert.throws(() => bindListeners(h.harbor, shadowed, { groupId: 'g' }), /Bare.onOrder is decorated but is not a method/)
    // A sealed instance is no obstacle: the standard dialect keeps its metadata beside the instance, not on it.
    class Sealed {
      constructor () { Object.seal(this) }
      @KafkaListener('orders')
      onOrder (): void {}
    }
    assert.equal(listenersOf(new Sealed()).length, 1)
  })

  test('a consumer with the DLQ off binds its ordinary listeners; only a @DLQHandler is refused', () => {
    const h = harness()
    @KafkaConsumer({ groupId: 'g', dlq: { enabled: false } })
    class Quiet {
      @KafkaListener('orders')
      onOrder (): void {}
    }
    assert.doesNotThrow(() => bindListeners(h.harbor, new Quiet()))
    @KafkaConsumer({ groupId: 'g', dlq: { enabled: false } })
    class Dead {
      @DLQHandler('orders')
      onDead (): void {}
    }
    assert.throws(() => bindListeners(h.harbor, new Dead()), /Dead.onDead handles the DLQ of "orders", but the consumer has the DLQ disabled/)
    assert.equal(hasListeners(42), false)
    assert.equal(hasListeners('Quiet'), false)
  })

  test('the decorators also accept the legacy signature NestJS applications compile with', async () => {
    class Legacy {
      readonly seen: unknown[] = []
      onOrder (message: Message): void { this.seen.push(message.value) }
      onBatch (messages: Message[]): void { this.seen.push(messages.length) }
    }
    // What `experimentalDecorators` emits: (prototype, name, descriptor) for methods, (constructor) for classes.
    ;(KafkaListener('orders') as (target: object, key: string, descriptor: PropertyDescriptor) => void)(Legacy.prototype, 'onOrder', Object.getOwnPropertyDescriptor(Legacy.prototype, 'onOrder') as PropertyDescriptor)
    ;(KafkaBatchListener('bulk', { size: 2 }) as (target: object, key: string, descriptor: PropertyDescriptor) => void)(Legacy.prototype, 'onBatch', Object.getOwnPropertyDescriptor(Legacy.prototype, 'onBatch') as PropertyDescriptor)
    ;(KafkaConsumer({ groupId: 'legacy', fromBeginning: true, autoCreateTopics: true }) as (target: unknown) => void)(Legacy)
    ;(KafkaRetry({ levels: [] }) as (target: unknown) => void)(Legacy)

    const h = harness()
    const processed = captureEvents(h.harbor, 'messageProcessed')
    const legacy = new Legacy()
    assert.deepEqual(consumerOptionsOf(legacy), { groupId: 'legacy', fromBeginning: true, autoCreateTopics: true, retry: { levels: [] } })
    const consumer = bindListeners(h.harbor, legacy)
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 'o' })
    await h.harbor.producer().sendBatch('bulk', [{ value: 1 }, { value: 2 }])
    await until(() => processed.length === 3)
    assert.deepEqual(legacy.seen, ['o', 2])
    await h.harbor.shutdown()

    // Standard-decorator misuse is refused where it is declared, in either dialect.
    const fakeContext = { kind: 'field', name: 'x', addInitializer: () => {} }
    assert.throws(() => (KafkaListener('orders') as (t: unknown, c: unknown) => void)(undefined, fakeContext), /@KafkaListener\(\) decorates methods, not a field/)
    assert.throws(() => (KafkaListener('orders') as (t: unknown, c: unknown) => void)(undefined, { kind: 'method', name: 'on', static: true, addInitializer: () => {} }), /@KafkaListener\(\) decorates instance methods; on is static/)
    assert.throws(() => (KafkaBatchListener('orders') as (t: unknown, k: string) => void)(class Static { static on (): void {} }, 'on'), /@KafkaBatchListener\(\) decorates instance methods; on is static/)
    assert.throws(() => (KafkaConsumer({ groupId: 'g' }) as (t: unknown, c: unknown) => void)(class {}, { kind: 'method', name: 'm', addInitializer: () => {} }), /@KafkaConsumer\(\) decorates classes, not a method/)
  })

  test('a batch listener may fail part of its batch with BatchFailedError like any batch handler', async () => {
    @KafkaConsumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    class Partial {
      @KafkaBatchListener<Order>('orders', { size: 2 })
      onOrders (messages: Array<Message<Order>>): void {
        const bad = messages.filter((message) => message.value.id === 'bad')
        if (bad.length > 0) throw new BatchFailedError(bad, Object.assign(new Error('bad'), { retryable: false }))
      }
    }
    const h = harness()
    const failed = captureEvents(h.harbor, 'messageFailed')
    const consumer = bindListeners(h.harbor, new Partial())
    await consumer.start()
    await h.harbor.producer<Order>().sendBatch('orders', [{ value: { id: 'ok', total: 1 } }, { value: { id: 'bad', total: 1 } }])
    await until(() => failed.length === 1)
    assert.equal(failed[0]?.offset, '1')
    assert.equal(h.adapter.messages('orders-dlq').length, 1)
    await h.harbor.shutdown()
  })
})

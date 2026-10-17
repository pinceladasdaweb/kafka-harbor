import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import 'reflect-metadata'
import { Controller, Injectable, Module, Scope, type BeforeApplicationShutdown, type LoggerService, type OnApplicationShutdown } from '@nestjs/common'
import { Test } from '@nestjs/testing'

import { ERROR_CODES, type Harbor, type Message } from '../../src/index'
import { memoryAdapter } from '../../src/testing/index'
import { KafkaConsumer, KafkaListener } from '../../src/decorators/index'
import { KAFKA_HARBOR, KafkaHarborModule, KafkaListenersExplorer } from '../../src/nestjs/index'
import { ManualClock, until } from '../helpers/manual-clock'

const silent = { info: () => {}, warn: () => {}, error: () => {} }

/** Nest's own logger, kept off the console: what it reports is asserted instead. */
const nestLogger = (): { logger: LoggerService, errors: string[] } => {
  const errors: string[] = []
  return { errors, logger: { log: () => {}, warn: () => {}, error: (message: unknown) => { errors.push(String(message)) } } }
}

describe('kafka-harbor/decorators with NestJS', () => {
  test('forRoot() builds the harbor, discovers the decorated providers and controllers, starts their consumers on bootstrap and shuts down with the application', async () => {
    const adapter = memoryAdapter()
    const seen: unknown[] = []

    @Injectable()
    @KafkaConsumer({ groupId: 'orders-service', fromBeginning: true, autoCreateTopics: true })
    class OrdersListener {
      @KafkaListener('orders')
      onOrder (message: Message): void { seen.push(message.value) }
    }

    @Controller()
    @KafkaConsumer({ groupId: 'payments-controller', fromBeginning: true, autoCreateTopics: true })
    class PaymentsController {
      @KafkaListener('payments')
      onPayment (message: Message): void { seen.push(`paid:${String(message.value)}`) }
    }

    @Injectable()
    class Plain {
      nothing (): void {}
    }

    const moduleRef = await Test.createTestingModule({
      imports: [KafkaHarborModule.forRoot({ clientId: 'nest-app', brokers: ['memory:9092'], logger: silent, adapter })],
      providers: [OrdersListener, Plain],
      controllers: [PaymentsController]
    }).compile()
    // An application context, as a worker without HTTP would have: init() runs the bootstrap hooks, close() the shutdown ones.
    const app = await moduleRef.init()

    const harbor = app.get<Harbor>(KAFKA_HARBOR)
    const explorer = app.get(KafkaListenersExplorer)
    assert.deepEqual(explorer.consumers.map((consumer) => [consumer.groupId, consumer.status]), [['orders-service', 'running'], ['payments-controller', 'running']], 'one consumer per decorated class; the plain provider is left alone')
    await harbor.producer().send('orders', { value: 42 })
    await harbor.producer().send('payments', { value: 7 })
    await until(() => seen.length === 2)
    assert.deepEqual(seen.sort(), [42, 'paid:7'])
    assert.equal(harbor.status, 'connected')

    await app.close()
    assert.equal(harbor.status, 'closed', 'the application shutdown closed the harbor')
    assert.deepEqual(explorer.consumers.map((consumer) => consumer.status), ['stopped', 'stopped'])
  })

  test('forRootAsync() takes the configuration from other providers, and the harbor can be injected', async () => {
    const adapter = memoryAdapter()

    @Injectable()
    class Settings {
      readonly clientId = 'from-settings'
    }
    // What the harbor module injects has to reach it through an import, as with any Nest dynamic module.
    @Module({ providers: [Settings], exports: [Settings] })
    class SettingsModule {}

    class Sender {
      constructor (readonly harbor: Harbor) {}
    }

    const moduleRef = await Test.createTestingModule({
      imports: [
        KafkaHarborModule.forRootAsync({
          imports: [SettingsModule],
          inject: [Settings],
          useFactory: ((settings: Settings) => ({ clientId: settings.clientId, brokers: ['memory:9092'], logger: silent, adapter })) as never
        })
      ],
      providers: [{ provide: Sender, useFactory: (harbor: Harbor) => new Sender(harbor), inject: [KAFKA_HARBOR] }]
    }).compile()
    const app = await moduleRef.init()
    const sender = app.get(Sender)
    assert.equal(sender.harbor.config.clientId, 'from-settings')
    assert.equal(app.get(KafkaListenersExplorer).consumers.length, 0, 'nothing decorated, nothing started')
    await app.close()
    assert.equal(sender.harbor.status, 'closed')
  })

  test('a consumer that cannot start fails the bootstrap and shuts the harbor down, so nothing is left consuming', async () => {
    const adapter = memoryAdapter()

    @Injectable()
    @KafkaConsumer({ groupId: 'fine', fromBeginning: true, autoCreateTopics: true })
    class Fine {
      @KafkaListener('orders')
      onOrder (): void {}
    }

    @Injectable()
    @KafkaConsumer({ groupId: 'broken', retry: { levels: [{ delay: 0 }] } })
    class Broken {
      @KafkaListener('payments')
      onPayment (): void {}
    }

    const moduleRef = await Test.createTestingModule({
      imports: [KafkaHarborModule.forRoot({ clientId: 'nest-app', brokers: ['memory:9092'], adapter, logger: { info: () => {}, warn: () => {}, error: () => {} } })],
      providers: [Fine, Broken]
    }).compile()
    await assert.rejects(moduleRef.init(), (error: unknown) => {
      assert.equal((error as { code: string }).code, ERROR_CODES.TOPIC_MISSING, 'the retry topic of "payments" does not exist and autoCreateTopics is off')
      return true
    })
    const harbor = moduleRef.get<Harbor>(KAFKA_HARBOR)
    assert.equal(harbor.status, 'closed')
    assert.deepEqual(moduleRef.get(KafkaListenersExplorer).consumers.map((consumer) => consumer.status), ['stopped', 'stopped'])
  })

  test('the module is global unless told otherwise', () => {
    const adapter = memoryAdapter()
    assert.equal(KafkaHarborModule.forRoot({ clientId: 'x', brokers: ['memory:9092'], adapter }).global, true)
    assert.equal(KafkaHarborModule.forRoot({ clientId: 'x', brokers: ['memory:9092'], adapter, global: false }).global, false)
    assert.equal(KafkaHarborModule.forRootAsync({ useFactory: () => ({ clientId: 'x', brokers: ['memory:9092'], adapter }) }).global, true)
    assert.equal(KafkaHarborModule.forRootAsync({ useFactory: () => ({ clientId: 'x', brokers: ['memory:9092'], adapter }), global: false }).global, false)
  })

  test('providers without listeners are left alone whatever their scope, and a symbol without a description marks nothing', async () => {
    const adapter = memoryAdapter()

    @Injectable({ scope: Scope.TRANSIENT })
    class Transient {
      nothing (): void {}
    }

    // Scoped too, so its class is what gets inspected for listener markers.
    @Injectable({ scope: Scope.REQUEST })
    class Marked {
      nothing (): void {}
    }
    // eslint-disable-next-line symbol-description -- a symbol with no description is the case under test
    Object.defineProperty(Marked.prototype, Symbol(), { value: 1 })

    const moduleRef = await Test.createTestingModule({
      imports: [KafkaHarborModule.forRoot({ clientId: 'nest-app', brokers: ['memory:9092'], logger: silent, adapter })],
      providers: [Transient, Marked]
    }).compile()
    const app = await moduleRef.init()
    assert.deepEqual(app.get(KafkaListenersExplorer).consumers, [])
    await app.close()
  })

  test('a decorated provider that is not a singleton is refused: a consumer needs one instance', async () => {
    const adapter = memoryAdapter()

    @Injectable({ scope: Scope.TRANSIENT })
    class Transient {
      onOrder (): void {}
    }
    // Under the legacy dialect the metadata sits on the prototype, which is what can be checked without an instance.
    ;(KafkaListener('orders') as (target: object, key: string) => void)(Transient.prototype, 'onOrder')
    ;(KafkaConsumer({ groupId: 'g' }) as (target: unknown) => void)(Transient)

    const moduleRef = await Test.createTestingModule({
      imports: [KafkaHarborModule.forRoot({ clientId: 'nest-app', brokers: ['memory:9092'], logger: silent, adapter })],
      providers: [Transient]
    }).compile()
    await assert.rejects(moduleRef.init(), /Transient declares Kafka listeners but is not a singleton/)
    assert.equal(moduleRef.get<Harbor>(KAFKA_HARBOR).status, 'closed')
  })

  test('a scoped provider whose parent class declares the listeners is refused as well', async () => {
    const adapter = memoryAdapter()
    class Base {
      onOrder (): void {}
    }
    ;(KafkaListener('orders') as (target: object, key: string) => void)(Base.prototype, 'onOrder')
    ;(KafkaConsumer({ groupId: 'g' }) as (target: unknown) => void)(Base)

    @Injectable({ scope: Scope.TRANSIENT })
    class Child extends Base {}

    const moduleRef = await Test.createTestingModule({
      imports: [KafkaHarborModule.forRoot({ clientId: 'nest-app', brokers: ['memory:9092'], logger: silent, adapter })],
      providers: [Child]
    }).compile()
    await assert.rejects(moduleRef.init(), /Child declares Kafka listeners but is not a singleton/)
  })

  test('on close the consumers drain first, before other modules close what the handlers use, and the client disconnects last', async () => {
    const adapter = memoryAdapter()
    const log: string[] = []
    const running: { explorer?: KafkaListenersExplorer } = {}
    let release!: () => void
    const held = new Promise<void>((resolve) => { release = resolve })
    let started = false

    @Injectable()
    @KafkaConsumer({ groupId: 'orders-service', fromBeginning: true, autoCreateTopics: true })
    class OrdersListener {
      @KafkaListener('orders')
      async onOrder (): Promise<void> {
        started = true
        await held
        log.push('handler done')
      }
    }

    @Injectable()
    class Database implements BeforeApplicationShutdown, OnApplicationShutdown {
      beforeApplicationShutdown (): void {
        log.push(`database closing: consumer ${String(running.explorer?.consumers[0]?.status)}, client connected ${String(adapter.connected)}`)
      }

      onApplicationShutdown (): void {
        log.push('database closed')
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [KafkaHarborModule.forRoot({ clientId: 'nest-app', brokers: ['memory:9092'], logger: silent, adapter })],
      providers: [OrdersListener, Database]
    }).compile()
    const app = await moduleRef.init()
    running.explorer = app.get(KafkaListenersExplorer)
    const harbor = app.get<Harbor>(KAFKA_HARBOR)
    await harbor.producer().send('orders', { value: 1 })
    await until(() => started)
    const closing = app.close()
    await new Promise((resolve) => setImmediate(resolve))
    release()
    await closing
    assert.deepEqual(log.slice(0, 2), ['handler done', 'database closing: consumer stopped, client connected true'])
    assert.ok(log.includes('database closed'))
    assert.equal(adapter.committed('orders-service', 'orders', 0), '1')
    assert.equal(harbor.status, 'closed')
  })

  test('shutdownTimeout bounds how long the close waits for a running handler, and is not part of the harbor\'s configuration', async () => {
    const adapter = memoryAdapter()
    const clock = new ManualClock()
    const nest = nestLogger()
    let started = false

    @Injectable()
    @KafkaConsumer({ groupId: 'orders-service', fromBeginning: true, autoCreateTopics: true })
    class StuckListener {
      @KafkaListener('orders')
      async onOrder (): Promise<void> {
        started = true
        await new Promise(() => {})
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [KafkaHarborModule.forRoot({ clientId: 'nest-app', brokers: ['memory:9092'], logger: silent, adapter, clock, shutdownTimeout: '2s' })],
      providers: [StuckListener]
    }).setLogger(nest.logger).compile()
    const app = await moduleRef.init()
    const harbor = app.get<Harbor>(KAFKA_HARBOR)
    assert.equal('shutdownTimeout' in harbor.config, false)
    await harbor.producer().send('orders', { value: 1 })
    await until(() => started)
    const closing = app.close()
    await until(() => clock.waiting === 1)
    assert.deepEqual(clock.sleeps.slice(-1), [2_000])
    clock.advance(2_000)
    // The abandoned handler is reported by the harbor's shutdown, which Nest logs; the application still closes.
    await closing
    assert.ok(nest.errors.some((line) => line.includes('1 handler(s) still running after 2000ms')), nest.errors.join('\n'))
    assert.equal(harbor.status, 'closed')
    assert.equal(adapter.connected, false)
    assert.equal(adapter.committed('orders-service', 'orders', 0), undefined)

    await assert.rejects(Test.createTestingModule({
      imports: [KafkaHarborModule.forRoot({ clientId: 'nest-app', brokers: ['memory:9092'], adapter: memoryAdapter(), shutdownTimeout: 'soon' as never })]
    }).compile(), { code: ERROR_CODES.CONFIG_INVALID, message: /shutdownTimeout/ })
  })

  test('a scoped factory provider whose prototype chain ends without Object.prototype is looked at, and left alone', async () => {
    const adapter = memoryAdapter()
    function settings (): { region: string } { return { region: 'eu' } }
    settings.prototype = Object.create(null) as object
    const moduleRef = await Test.createTestingModule({
      imports: [KafkaHarborModule.forRoot({ clientId: 'nest-app', brokers: ['memory:9092'], logger: silent, adapter })],
      providers: [{ provide: 'SETTINGS', useFactory: settings, scope: Scope.TRANSIENT }]
    }).compile()
    const app = await moduleRef.init()
    assert.deepEqual(app.get(KafkaListenersExplorer).consumers, [])
    await app.close()
  })

  test('a configuration factory that returns nothing is refused as a configuration error', async () => {
    for (const nothing of [undefined, null]) {
      await assert.rejects(Test.createTestingModule({
        imports: [KafkaHarborModule.forRootAsync({ useFactory: (() => nothing) as never })]
      }).compile(), { code: ERROR_CODES.CONFIG_INVALID }, String(nothing))
    }
  })

  test('what the drain left of shutdownTimeout goes to the sends in flight, which land before the client goes', async () => {
    const adapter = memoryAdapter()
    const clock = new ManualClock()
    const warnings: string[] = []
    let release!: () => void
    const held = new Promise<void>((resolve) => { release = resolve })
    let auditing = false
    const original = adapter.produce
    adapter.produce = async (records) => {
      if (records.some((record) => record.topic === 'audit')) {
        auditing = true
        await held
      }
      await original(records)
    }
    const moduleRef = await Test.createTestingModule({
      imports: [KafkaHarborModule.forRoot({ clientId: 'nest-app', brokers: ['memory:9092'], adapter, clock, shutdownTimeout: '2s', logger: { ...silent, warn: (message: string) => { warnings.push(message) } } })]
    }).compile()
    const app = await moduleRef.init()
    const harbor = app.get<Harbor>(KAFKA_HARBOR)
    await harbor.connect()
    adapter.createTopic('audit')
    const sending = harbor.producer().send('audit', { value: 'in flight' })
    await until(() => auditing)
    const closing = app.close()
    await until(() => clock.waiting === 1)
    assert.deepEqual(clock.sleeps.slice(-1), [2_000], 'the whole deadline: the drain used none of it')
    release()
    await sending
    await closing
    assert.equal(adapter.messages('audit').length, 1)
    assert.deepEqual(warnings, [])
    assert.equal(harbor.status, 'closed')
  })

  test('shutdownTimeout is one deadline for the whole close: what the drain used is not given again to the sends in flight', async () => {
    const adapter = memoryAdapter()
    const clock = new ManualClock()
    const nest = nestLogger()
    let started = false
    let auditing = false
    const original = adapter.produce
    adapter.produce = async (records) => {
      if (records.some((record) => record.topic === 'audit')) {
        auditing = true
        await new Promise(() => {})
      }
      await original(records)
    }

    @Injectable()
    @KafkaConsumer({ groupId: 'orders-service', fromBeginning: true, autoCreateTopics: true })
    class StuckListener {
      @KafkaListener('orders')
      async onOrder (): Promise<void> {
        started = true
        await new Promise(() => {})
      }
    }

    const moduleRef = await Test.createTestingModule({
      imports: [KafkaHarborModule.forRoot({ clientId: 'nest-app', brokers: ['memory:9092'], adapter, clock, shutdownTimeout: '2s', logger: silent })],
      providers: [StuckListener]
    }).setLogger(nest.logger).compile()
    const app = await moduleRef.init()
    const harbor = app.get<Harbor>(KAFKA_HARBOR)
    await harbor.producer().send('orders', { value: 1 })
    await until(() => started)
    harbor.producer().send('audit', { value: 'never acknowledged' }).catch(() => {})
    await until(() => auditing)
    const closing = app.close()
    await until(() => clock.waiting === 1)
    clock.advance(2_000)
    // Nothing is left of the deadline: the send in flight is not waited for.
    await closing
    assert.ok(nest.errors.some((line) => line.includes('still running')), nest.errors.join('\n'))
    assert.equal(harbor.status, 'closed')
  })
})

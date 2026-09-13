import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import 'reflect-metadata'
import { Controller, Injectable, Module, Scope } from '@nestjs/common'
import { Test } from '@nestjs/testing'

import { ERROR_CODES, type Harbor, type Message } from '../../src/index'
import { memoryAdapter } from '../../src/testing/index'
import { KafkaConsumer, KafkaListener } from '../../src/decorators/index'
import { KAFKA_HARBOR, KafkaHarborModule, KafkaListenersExplorer } from '../../src/nestjs/index'
import { until } from '../helpers/manual-clock'

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
      imports: [KafkaHarborModule.forRoot({ clientId: 'nest-app', brokers: ['memory:9092'], adapter })],
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
          useFactory: ((settings: Settings) => ({ clientId: settings.clientId, brokers: ['memory:9092'], adapter })) as never
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
      imports: [KafkaHarborModule.forRoot({ clientId: 'nest-app', brokers: ['memory:9092'], adapter })],
      providers: [Transient]
    }).compile()
    await assert.rejects(moduleRef.init(), /Transient declares Kafka listeners but is not a singleton/)
    assert.equal(moduleRef.get<Harbor>(KAFKA_HARBOR).status, 'closed')
  })
})

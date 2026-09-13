/**
 * A NestJS application context wired to kafka-harbor through the decorators
 * entry point: the module builds the harbor, discovers the provider whose
 * methods are decorated, starts its consumer when the application
 * bootstraps and shuts the harbor down with it. Runs on the in-memory
 * adapter and asserts its own outcome, so it doubles as executable
 * documentation.
 */
import 'reflect-metadata'
import assert from 'node:assert/strict'

import { Injectable, Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'

import type { BatchContext, Harbor, HandlerContext, Message } from '../src/index'
import { memoryAdapter } from '../src/testing/index'
import { KafkaBatchListener, KafkaConsumer, KafkaListener, KafkaRetry } from '../src/decorators/index'
import { KAFKA_HARBOR, KafkaHarborModule } from '../src/nestjs/index'

interface Order { id: string, total: number }

const seen: string[] = []

@Injectable()
@KafkaConsumer({ groupId: 'orders-service', fromBeginning: true, autoCreateTopics: true })
@KafkaRetry({ levels: [{ delay: 0 }] })
class OrdersListener {
  @KafkaListener<Order>('orders')
  onOrder (message: Message<Order>, ctx: HandlerContext): void {
    if (message.value.total < 0 && ctx.attempt === 1) throw new Error('negative total, try once more')
    seen.push(`order ${message.value.id} on attempt ${ctx.attempt}`)
  }

  @KafkaBatchListener<Order>('invoices', { size: 2, maxWait: '100ms' })
  onInvoices (messages: Array<Message<Order>>, ctx: BatchContext): void {
    seen.push(`${messages.length} invoices from ${ctx.topic}[${ctx.partition}]`)
  }
}

// A NestJS application compiled with `experimentalDecorators` injects the
// harbor with `@Inject(KAFKA_HARBOR)` on the constructor parameter; this
// repository compiles with standard decorators, which have no parameter
// form, so the service is wired through a factory provider instead.
class OrdersService {
  constructor (private readonly harbor: Harbor) {}

  async place (order: Order): Promise<void> {
    await this.harbor.producer<Order>().send('orders', { key: order.id, value: order })
  }
}

const adapter = memoryAdapter()

@Module({
  imports: [KafkaHarborModule.forRoot({ clientId: 'nest-example', brokers: ['memory:9092'], adapter, logger: { info: () => {}, warn: () => {}, error: () => {} } })],
  providers: [OrdersListener, { provide: OrdersService, useFactory: (harbor: Harbor) => new OrdersService(harbor), inject: [KAFKA_HARBOR] }]
})
class AppModule {}

const app = await NestFactory.createApplicationContext(AppModule, { logger: false })
const orders = app.get(OrdersService)
await orders.place({ id: 'a', total: 10 })
await orders.place({ id: 'b', total: -1 })
await app.get<Harbor>(KAFKA_HARBOR).producer<Order>().sendBatch('invoices', [{ value: { id: 'i1', total: 1 } }, { value: { id: 'i2', total: 2 } }])

const deadline = Date.now() + 5_000
while (seen.length < 3 && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 10))
assert.deepEqual(seen.sort(), ['2 invoices from invoices[0]', 'order a on attempt 1', 'order b on attempt 2'])

await app.close()
assert.equal(app.get<Harbor>(KAFKA_HARBOR).status, 'closed')
console.log('nestjs example: decorated listeners discovered, started, and shut down with the application')

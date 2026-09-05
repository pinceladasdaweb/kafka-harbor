/**
 * The handler-signature exercise: five real handlers written against the
 * chosen signature, `(message, context)`, and the same five against the
 * alternative that was considered, a single destructurable object. Runs on
 * the in-memory adapter and asserts its own outcomes, so it doubles as
 * executable documentation.
 *
 * Why `(message, context)` won: the message travels as one coherent object
 * (handlers hand it to other functions, tests build one and pass it), and
 * the runtime utilities (logger, signal, attempt) never mix with the data.
 * The single-object form is a two-line wrapper on top of it, shown below as
 * `fromValue`, and an application that prefers it can adopt that wrapper
 * without the library taking a position.
 */
import assert from 'node:assert/strict'

import { createHarbor, type Handler, type HandlerContext, type Message } from '../src/index'
import { memoryAdapter } from '../src/testing/index'

interface Order { id: string, customerId: string, total: number }
interface Payment { orderId: string, amount: number, method: 'card' | 'pix' }
interface Shipment { orderId: string, trackingCode: string }
interface Audit { entity: string, action: string }

const adapter = memoryAdapter()
const harbor = createHarbor({
  clientId: 'examples',
  brokers: ['memory:9092'],
  adapter,
  logger: { info: () => {}, warn: () => {}, error: () => {} }
})

const processed: string[] = []
const store = new Map<string, Order>()

/* ---------- Option A, the chosen one: (message, context) ---------- */

// 1. The common case: only the value matters.
const onOrderCreated: Handler<Order> = async (message) => {
  store.set(message.value.id, message.value)
  processed.push(`order:${message.value.id}`)
}

// 2. Headers and key matter: tenant routing by header, key as the entity id.
const onPayment: Handler<Payment> = async (message, ctx) => {
  const tenant = message.headers['x-tenant'] ?? 'default'
  ctx.logger.info(`payment for ${message.key ?? message.value.orderId} on tenant ${tenant}`)
  processed.push(`payment:${message.value.orderId}:${tenant}`)
}

// 3. The message is handed to another function whole: it stays one object.
const enrich = (message: Message<Shipment>): Shipment & { partition: number } => ({ ...message.value, partition: message.partition })
const onShipment: Handler<Shipment> = async (message) => {
  const enriched = enrich(message)
  processed.push(`shipment:${enriched.orderId}@${enriched.partition}`)
}

// 4. Retry-aware: the attempt number and the retry trail drive the decision.
const onAudit: Handler<Audit> = async (message, ctx) => {
  if (ctx.attempt === 1 && message.value.action === 'flaky') {
    throw new Error('first attempt fails on purpose')
  }
  processed.push(`audit:${message.value.action}:attempt${ctx.attempt}:retries${message.retry?.count ?? 0}`)
}

// 5. Long-running: observes the shutdown signal instead of ignoring it.
const onReport: Handler<{ pages: number }> = async (message, ctx) => {
  for (let page = 0; page < message.value.pages; page++) {
    if (ctx.signal.aborted) return
    await new Promise((resolve) => setImmediate(resolve))
  }
  processed.push(`report:${message.value.pages}`)
}

/* ---------- Option B, derivable from A: a single destructurable object ---------- */

type Flat<T> = Message<T> & HandlerContext
const fromValue = <T>(handler: (input: Flat<T>) => Promise<void> | void): Handler<T> =>
  async (message, ctx) => await handler({ ...message, ...ctx })

const onOrderCreatedB = fromValue<Order>(async ({ value }) => { processed.push(`b:order:${value.id}`) })
const onPaymentB = fromValue<Payment>(async ({ value, headers, logger }) => {
  logger.info('b payment')
  processed.push(`b:payment:${value.orderId}:${headers['x-tenant'] ?? 'default'}`)
})
const onShipmentB = fromValue<Shipment>(async ({ value, partition }) => { processed.push(`b:shipment:${value.orderId}@${partition}`) })
const onAuditB = fromValue<Audit>(async ({ value, attempt }) => { processed.push(`b:audit:${value.action}:attempt${attempt}`) })
const onReportB = fromValue<{ pages: number }>(async ({ value, signal }) => {
  if (!signal.aborted) processed.push(`b:report:${value.pages}`)
})

async function main (): Promise<void> {
  const consumer = harbor.consumer({ groupId: 'examples', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    .subscribe('orders', onOrderCreated)
    .subscribe('payments', onPayment)
    .subscribe('shipments', onShipment)
    .subscribe('audit', onAudit)
    .subscribe('reports', onReport)
    .subscribe('b-orders', onOrderCreatedB)
    .subscribe('b-payments', onPaymentB)
    .subscribe('b-shipments', onShipmentB)
    .subscribe('b-audit', onAuditB)
    .subscribe('b-reports', onReportB)

  const producer = harbor.producer()
  await producer.send('orders', { key: 'o1', value: { id: 'o1', customerId: 'c1', total: 10 } })
  await producer.send('payments', { key: 'o1', value: { orderId: 'o1', amount: 10, method: 'pix' }, headers: { 'x-tenant': 'acme' } })
  await producer.send('shipments', { value: { orderId: 'o1', trackingCode: 'T1' } })
  await producer.send('audit', { value: { entity: 'order', action: 'flaky' } })
  await producer.send('reports', { value: { pages: 3 } })
  await producer.send('b-orders', { value: { id: 'o2', customerId: 'c2', total: 20 } })
  await producer.send('b-payments', { value: { orderId: 'o2', amount: 20, method: 'card' } })
  await producer.send('b-shipments', { value: { orderId: 'o2', trackingCode: 'T2' } })
  await producer.send('b-audit', { value: { entity: 'order', action: 'ok' } })
  await producer.send('b-reports', { value: { pages: 1 } })

  await consumer.start()
  for (const topic of ['orders', 'payments', 'shipments', 'audit', 'audit-retry-1', 'reports', 'b-orders', 'b-payments', 'b-shipments', 'b-audit', 'b-reports']) {
    await adapter.whenDrained('examples', topic)
  }
  await harbor.shutdown()

  assert.deepEqual(processed.sort(), [
    'audit:flaky:attempt2:retries1',
    'b:audit:ok:attempt1',
    'b:order:o2',
    'b:payment:o2:default',
    'b:report:1',
    'b:shipment:o2@0',
    'order:o1',
    'payment:o1:acme',
    'report:3',
    'shipment:o1@0'
  ])
  assert.equal(store.get('o1')?.total, 10)
  assert.equal(adapter.messages('audit-retry-1').length, 1, 'the flaky audit went through the retry topic once')
  assert.equal(adapter.messages('audit-dlq').length, 0)
  console.log('handler-signatures: ok (10 handlers, 1 retry, 0 dead letters)')
}

main().catch((error) => {
  console.error(error)
  process.exitCode = 1
})

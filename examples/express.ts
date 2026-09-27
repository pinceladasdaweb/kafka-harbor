/**
 * A web application around the harbor: an Express app produces from a
 * route, a consumer handles the orders in the background, `/health`
 * answers the orchestrator's probes from `harbor.health()`, `/metrics`
 * serves the Prometheus registry, and SIGTERM drains the HTTP server and
 * the harbor in that order. Runs on the in-memory adapter, drives itself
 * through HTTP and asserts its own outcome, so it doubles as executable
 * documentation; `npm run examples` executes it.
 */
import assert from 'node:assert/strict'
import { once } from 'node:events'

import express from 'express'

import { createHarbor, type HarborEvents, type Message } from '../src/index'
import { prometheusMetrics } from '../src/prometheus/index'
import { memoryAdapter } from '../src/testing/index'

interface Order { id: string, total: number }

const adapter = memoryAdapter()
const harbor = createHarbor({
  clientId: 'shop-api',
  brokers: ['memory:9092'],
  adapter,
  logger: { info: () => {}, warn: () => {}, error: () => {} }
})
const metrics = prometheusMetrics(harbor)

// The consumer runs in the same process as the HTTP server: one deployable
// unit, the way a small service is usually shipped.
const processed: string[] = []
const deadLettered: Array<HarborEvents['messageDeadLettered']> = []
harbor.on('messageDeadLettered', (event) => { deadLettered.push(event) })
const consumer = harbor.consumer({ groupId: 'orders-workers', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: '1s' }] } })
consumer.subscribe<Order>('orders', (message: Message<Order>) => {
  // A negative total will never become valid: no retry, straight to the DLQ.
  if (message.value.total < 0) throw Object.assign(new Error(`order ${message.value.id} has a negative total`), { retryable: false })
  processed.push(message.value.id)
})
const producer = harbor.producer<Order>()

const app = express()
app.use(express.json())

// Accepts the order and answers as soon as the broker acknowledged it:
// the work itself happens on the consumer side, at its own pace.
app.post('/orders', async (req, res) => {
  const order = req.body as Order
  await producer.send('orders', { key: order.id, value: order })
  res.status(202).json({ accepted: order.id })
})

// What a readiness probe wants: 200 while the harbor and its consumers
// are up, 503 once something stopped on its own or the shutdown began.
app.get('/health', (_req, res) => {
  const health = harbor.health()
  res.status(health.healthy ? 200 : 503).json(health)
})

app.get('/metrics', async (_req, res) => {
  res.type(metrics.registry.contentType).send(await metrics.registry.metrics())
})

await consumer.start()
const server = app.listen(0)
await once(server, 'listening')
const { port } = server.address() as { port: number }
const base = `http://127.0.0.1:${port}`

// Stop taking requests first, then let the handlers in flight finish and
// commit; the harbor waits for them up to the timeout.
const shutdown = async (): Promise<void> => {
  server.close()
  await harbor.shutdown('10s')
}
process.once('SIGTERM', () => {
  shutdown().catch((error: unknown) => { console.error(error); process.exitCode = 1 })
})

// The application drives itself from here on.
const post = async (order: Order): Promise<Response> =>
  await fetch(`${base}/orders`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(order) })

assert.equal((await post({ id: 'a', total: 10 })).status, 202)
assert.equal((await post({ id: 'b', total: -1 })).status, 202)

const deadline = Date.now() + 5_000
while ((processed.length < 1 || deadLettered.length < 1) && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 10))
assert.deepEqual(processed, ['a'])
assert.equal(deadLettered[0]?.dlqTopic, 'orders-dlq')

const health = await (await fetch(`${base}/health`)).json() as { healthy: boolean, consumers: Array<{ groupId: string, status: string }> }
assert.equal(health.healthy, true)
assert.deepEqual(health.consumers, [{ groupId: 'orders-workers', status: 'running' }])

const scraped = await (await fetch(`${base}/metrics`)).text()
assert.match(scraped, /kafka_harbor_messages_processed_total\{group="orders-workers",topic="orders"\} 1/)
assert.match(scraped, /kafka_harbor_messages_dead_lettered_total\{group="orders-workers",topic="orders"\} 1/)

process.kill(process.pid, 'SIGTERM')
while (harbor.status !== 'closed' && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 10))
assert.equal(harbor.status, 'closed')
assert.equal(server.listening, false)
console.log('express example: produced from a route, consumed in the background, probed and scraped over HTTP, drained on SIGTERM')

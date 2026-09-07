// Type-checked against the BUILT declarations (dist/), the way a consumer
// sees the package, never against src/. The unit suite loads src/ through
// tsx, so it cannot notice a declaration bundle that inlines its own copy of
// a class: a class with private members is nominal, and a Harbor declared
// twice (once in index.d.ts, once inside a subpath's d.ts) would make the
// adapters unusable for every consumer while every test passed. `npm run
// check:dist` builds and compiles this file.
import { memoryAdapter } from 'kafka-harbor/testing'
import { prometheusMetrics } from 'kafka-harbor/prometheus'
import { otelMetrics, otelTracing } from 'kafka-harbor/otel'
import { confluentAdapter } from 'kafka-harbor/adapters/confluent'
import { createHarbor, type ClientAdapter, type Message } from 'kafka-harbor'

interface Order { id: string, total: number }

const useMemory: boolean = false
const adapter: ClientAdapter = useMemory ? memoryAdapter() : confluentAdapter()

export const harbor = createHarbor({ clientId: 'consumer-check', brokers: ['localhost:9092'], adapter })

export const consumer = harbor.consumer({ groupId: 'check', retry: { levels: [{ delay: '5s' }] } })
  .subscribe<Order>('orders', async (message: Message<Order>, ctx) => {
    const total: number = message.value.total
    if (total < 0) throw harbor.abort(new Error('negative total'))
    ctx.logger.info(`order ${message.value.id}`, { attempt: ctx.attempt })
  })

// The event map has no index signature: a misspelled event name is a type error.
// @ts-expect-error 'messageProcesed' is not an event
harbor.on('messageProcesed', () => {})
// The exposed configuration never carries the SASL password.
// @ts-expect-error password is not exposed
export const leaked: string | undefined = harbor.config.sasl?.password

// The metrics entry points take the Harbor class from the core declarations:
// a class inlined into a subpath's d.ts would be another nominal type.
export const metrics = prometheusMetrics(harbor, { prefix: 'check_' })
export const otel = otelMetrics(harbor)
export const traced = createHarbor({ clientId: 'consumer-check', brokers: ['localhost:9092'], adapter, instrumentation: otelTracing() })

export const producer = harbor.producer<Order>()
export const send = async (): Promise<void> => await producer.send('orders', { key: '1', value: { id: '1', total: 10 } })

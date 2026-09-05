# kafka-harbor

> **The application layer that Kafka clients don't give you**: retry topics, dead-letter queue, graceful shutdown, serialization and observability for Node.js. Client-agnostic by design.

[![CI](https://github.com/pinceladasdaweb/kafka-harbor/actions/workflows/ci.yml/badge.svg)](https://github.com/pinceladasdaweb/kafka-harbor/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/kafka-harbor.svg)](https://www.npmjs.com/package/kafka-harbor)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Messages cross the sea; the harbor is where they dock safely. Every Kafka client for Node.js stops at the protocol: you get a producer, a consumer, and good luck. Retry with backoff, a dead-letter queue, offsets committed only after your code ran, a shutdown that does not lose or duplicate work: all of it gets rebuilt by hand in every project. kafka-harbor is that layer, done once, on top of the client you already use.

> **Status: pre-release.** The API below is the frozen design for 1.0 and is exercised end to end against a real broker, but the package is not published yet. Nothing here is a promise until `1.0.0` is on npm.

```ts
import { createHarbor } from 'kafka-harbor'
import { confluentAdapter } from 'kafka-harbor/adapters/confluent'

const harbor = createHarbor({
  clientId: 'orders-service',
  brokers: ['kafka-1:9092', 'kafka-2:9092'],
  adapter: confluentAdapter()
})

const consumer = harbor.consumer({
  groupId: 'orders-workers',
  retry: { levels: [{ delay: '5s' }, { delay: '1m' }, { delay: '10m' }] },
  autoCreateTopics: true
})

consumer.subscribe<Order>('orders', async (message, ctx) => {
  await fulfill(message.value)      // throws? -> orders-retry-1, then -retry-2, -retry-3, then orders-dlq
  ctx.logger.info(`order ${message.key} done on attempt ${ctx.attempt}`)
})

await consumer.start()
harbor.enableSignalHandlers()        // SIGTERM -> finish in-flight handlers, commit, leave, disconnect
```

## Table of contents

- [Why another Kafka library?](#why-another-kafka-library)
- [What it does not do](#what-it-does-not-do)
- [Install](#install)
- [Core concepts](#core-concepts)
- [Producer](#producer)
- [Consumer](#consumer)
- [Retry topics and the DLQ](#retry-topics-and-the-dlq)
- [Graceful shutdown](#graceful-shutdown)
- [Health](#health)
- [Serialization](#serialization)
- [Headers](#headers)
- [Events](#events)
- [Errors](#errors)
- [Adapters](#adapters)
- [Testing your handlers](#testing-your-handlers)
- [Development](#development)

## Why another Kafka library?

It is not a client. kafka-harbor runs **on top of** a client through a small `ClientAdapter` interface (connect, produce, consume, commit, pause, resume, admin). The default adapter wraps [`@confluentinc/kafka-javascript`](https://github.com/confluentinc/confluent-kafka-javascript), Confluent's supported client with librdkafka underneath, and the interface is designed so that the core never sees a client type.

Every Node.js team using Kafka ends up writing the same application layer on top of whichever client they picked, because the clients stop at the protocol. The comparison below is against the clients themselves, which is the honest one: kafka-harbor is not a replacement for them, it runs on top of one.

| | kafka-harbor | [@confluentinc/kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript) | [kafkajs](https://github.com/tulios/kafkajs) | [@platformatic/kafka](https://github.com/platformatic/kafka) |
|---|---|---|---|---|
| **Retry topics with a delay per level**, tracking headers, `retryIf` predicate | ✅ `orders-retry-1..N`, delays honored, headers validated as network input | ➖ you build it | ➖ you build it | ➖ you build it |
| **Dead-letter topic** with the original bytes and the failure trail | ✅ automatic after the last level | ➖ | ➖ | ➖ |
| Draining the DLQ back into service | ✅ `harbor.redrive()`, resumable, filterable | ➖ | ➖ | ➖ |
| Offset committed only after the retry/DLQ produce was **acknowledged** | ✅ by construction; a failed produce stops the consumer instead of committing | ➖ your ordering | ➖ your ordering | ➖ your ordering |
| Graceful shutdown: wait for handlers with a deadline, commit, leave, disconnect, **report** what was abandoned | ✅ `ShutdownTimeoutError` names the count | ➖ `disconnect()` waits for the running handler, no deadline, no report | ➖ same | ➖ `close()` |
| `harbor.abort()`: stop without committing when reprocessing is the right call | ✅ | ➖ throw and hope auto-commit is off | ➖ | ➖ |
| Serialization that never silently flattens (`Map`, `undefined`, `NaN` rejected) | ✅ strict JSON default, pluggable per topic | ➖ bytes | ➖ bytes | ✅ pluggable serdes, schema registry |
| In-memory broker for unit tests, same core code, no Docker | ✅ `kafka-harbor/testing` | ❌ | ❌ | ❌ |
| Client-agnostic: swap the client without touching handlers | ✅ `ClientAdapter`, contract suite for authors | n/a | n/a | n/a |
| Typed outcome events (`messageRetried`, `messageDeadLettered`, ...) with correlation id | ✅ | ➖ client events | ➖ instrumentation events | ➖ |
| Health snapshot for probes | ✅ `isHealthy()` / `health()` | ➖ | ➖ | ➖ |
| Idempotent producer and `acks=all` on by default | ✅ set by the adapter, cannot be overridden by accident | ➖ opt-in | ➖ opt-in | ➖ opt-in |
| Runtime dependencies | breakwater + the client you choose | native librdkafka | none (pure JS) | none (pure TS) |

The rows are not a knock on the clients: transactions, exactly-once, schema registry, fetch tuning and wire performance are theirs, and the Confluent client is the one kafka-harbor recommends underneath. The rows are the layer every project rebuilds by hand, done once, with the ordering guarantees tested against a real broker.

The design principle behind every decision: **losing a message is never the default.** Every failure ends in a retry topic, in the DLQ, or in an explicit stop of the consumer. There is no silent path.

### What it does not do

- **At-least-once only.** Duplicates are possible after a crash between handler and commit, a rebalance mid-handler, or an abandoned shutdown; [docs/delivery-semantics.md](docs/delivery-semantics.md) lists every case. Exactly-once effects come from deduplicating in the handler by a business key.
- **One retry ladder per topic.** Three levels times twenty topics is sixty topics. A shared retry topic per service is not in 1.0.
- **A retry delay must fit under the poll interval** (`maxProcessingTime`, default 5 minutes), because the retry consumer waits the delay before the handler runs. Longer ladders need a longer `max.poll.interval.ms` on the client.
- **Retry breaks ordering.** A message that goes through a retry topic is processed after later messages on the original topic. The alternative, blocking the partition until it succeeds, is what `harbor.abort()` gives you.
- **The default adapter has a native dependency.** `@confluentinc/kafka-javascript` ships prebuilt binaries for Node 22 and 24 on Linux (glibc and musl) and macOS; Node 26 compiles librdkafka at install. Any other client can be plugged in through `ClientAdapter`.
- **No transactions, no batch handlers, no metrics exporters.** Observability is the typed event stream; wire it to the collector you use.

## Install

```bash
npm install kafka-harbor @confluentinc/kafka-javascript
```

The Confluent client is a peer dependency: install it when you use `kafka-harbor/adapters/confluent`. It ships prebuilt binaries for Node 18 to 24 on Linux (glibc and musl) and macOS; on Node 26 it compiles librdkafka from source at install time. Node.js >= 22 is required by kafka-harbor itself.

## Core concepts

- **Harbor**: the entry point. Holds the connection, the serializer, the header names and the logger. `createHarbor()` does not connect; the first `send()` or `start()` does, or call `connect()`.
- **Producer**: `harbor.producer()`. Serializes, adds the automatic headers, produces with `acks=all` and an idempotent producer, and retries transient broker failures through [breakwater](https://github.com/pinceladasdaweb/breakwater).
- **Consumer**: `harbor.consumer({ groupId })`. One consumer group, one or more topics, one handler per topic. Owns the retry ladder and the DLQ for each topic it subscribes to.
- **Adapter**: the client behind it all. Explicit in the config so that the core has no dependency on any client.
- **Message**: what the handler receives. Deserialized value, string key, string headers, `Date` timestamp, and `retry` metadata when it came through a retry topic.

## Producer

```ts
const producer = harbor.producer<Order>()

await producer.send('orders', { key: order.id, value: order })

await producer.sendBatch('orders', [
  { key: 'a', value: orderA, headers: { 'x-tenant': 'acme' } },
  { key: 'b', value: orderB, partition: 3 }        // explicit partition, rarely needed
])

// A serializer for this producer only, and a tighter retry ladder.
import { exponential } from 'breakwater'

const events = harbor.producer<OrderEvent>({
  serializer: avroSerializer(schema),
  retry: { attempts: 3, backoff: exponential({ initial: 50, max: 1_000 }) }
})
```

- Every message gets `x-correlation-id` (kept if you set one), `x-produced-at` and `x-producer` (your `clientId`).
- Keyed messages land on the partition Kafka's default partitioner picks (murmur2). `partitionForKey(key, partitions)` computes the same number, for code that needs to know where a key goes: sharding a cache by partition, asserting co-location of related keys, or routing an unkeyed message next to a keyed one.
- A batch is serialized before any byte leaves the process: one unencodable value means nothing is produced.
- `send()` resolves after the broker acknowledged. Transient failures are retried (default: 5 attempts, exponential backoff with full jitter); a failure marked `retryable: false` is not. When the attempts run out you get breakwater's `RETRY_EXHAUSTED` with the last failure as `cause`.

## Consumer

```ts
const consumer = harbor.consumer({
  groupId: 'orders-workers',

  retry: {
    levels: [{ delay: '5s' }, { delay: '1m' }, { delay: '10m' }],
    retryIf: (error) => !(error instanceof ValidationError),  // default: everything unless retryable === false
    topicNaming: (topic, level) => `${topic}-retry-${level}`   // default
  },
  dlq: {
    enabled: true,                                              // default
    topicNaming: (topic) => `${topic}-dlq`                      // default
  },

  concurrency: 4,             // partitions processed at once; order is kept within each. Default: 1
  autoCreateTopics: true,     // create the retry and DLQ topics through the Admin API on start. Default: false
  topicDefaults: { partitions: 3, replicationFactor: 3 },
  fromBeginning: false,       // where a brand-new group starts. Default: false (latest)
  maxProcessingTime: '5m'     // every retry delay must fit under it; see below. Default: '5m'
})

consumer
  .subscribe<Order>('orders', onOrder)
  .subscribe<Payment>('payments', onPayment, { serializer: protobufSerializer(Payment) })

await consumer.start()
```

The handler signature is `(message, context)`:

```ts
consumer.subscribe<Order>('orders', async (message, ctx) => {
  message.topic       // 'orders' or 'orders-retry-2'
  message.partition   // number
  message.offset      // string
  message.key         // string | null
  message.value       // Order, deserialized
  message.headers     // Record<string, string>
  message.timestamp   // Date
  message.retry       // { count, originalTopic, firstFailureAt, lastError } on a retry topic

  ctx.correlationId   // from the headers, if any
  ctx.attempt         // 1 on first delivery, retry count + 1 afterwards
  ctx.logger          // the harbor's logger
  ctx.signal          // aborts when shutdown gave up waiting for this handler
})
```

What happens next depends on how the handler ends, and nothing else. There is no ack callback to forget:

| The handler... | The consumer... |
|---|---|
| returns | commits the offset |
| throws, and `retryIf(error)` is true, and a retry level is left | produces to the next retry topic, then commits |
| throws otherwise | produces to the DLQ, then commits |
| throws `harbor.abort(error)` | stops **without** committing (infrastructure bug: reprocess after restart) |
| throws, with no retry level left and the DLQ disabled | stops **without** committing and emits `error` |

The retry or DLQ produce is acknowledged by the broker **before** the source offset is committed. If it is not acknowledged, the consumer stops and the message stays where it is: it will be redelivered. A commit that fails after the work is safe (a rebalance in progress, for instance) is reported through the `error` event and the consumer carries on; that message is redelivered too.

## Retry topics and the DLQ

A failed message is re-produced, bytes untouched, to `orders-retry-1`. The same consumer group also consumes `orders-retry-1`, waits until the message is `5s` old, and runs the handler again. Fail again: `orders-retry-2`, `1m`. And so on until the ladder is exhausted, then `orders-dlq`.

Each hop rewrites the tracking headers:

| Header | Meaning |
|---|---|
| `x-retry-count` | handler failures so far |
| `x-original-topic` | where the message was first produced |
| `x-first-failure-at` | ISO-8601 instant of the first failure |
| `x-last-error` | description of the latest failure, bounded to 1 KiB |
| `x-dead-lettered-at` | set on the DLQ hop only |
| `x-redriven-from`, `x-redriven-at` | set by `harbor.redrive()` when a message comes back from the DLQ |

These headers come from the network and are validated before use: a blank or corrupt count never turns into `0` or `NaN`; the whole block is discarded and the message counts as a first delivery on that level. A message that fails to deserialize goes straight to the DLQ; retrying would not decode it either.

Things to know:

- **Delays are bounded by `maxProcessingTime`** (default 5 minutes, Kafka's `max.poll.interval.ms`). A retry consumer waits the delay before the handler runs; a wait longer than the poll interval would get it kicked out of the group. A level above the bound is a `ConfigError` at construction naming `retry.levels[i].delay`.
- **Retention must exceed the delay.** A message with a 1h delay on a topic with 30 minutes of retention is a lost message. `topicDefaults` and your own topic configs are yours to set accordingly.
- **Naming uses hyphens** (`orders-retry-1`, `orders-dlq`), the same as Spring Kafka's defaults, because Kafka warns that `.` and `_` collide in metric names. Both naming functions are configurable.
- **One ladder per topic.** Three levels times twenty topics is sixty retry topics. A shared retry topic per service is a possible future mode; it is not in 1.0.
- With no levels configured (the default), a failure goes straight to the DLQ.

### Draining the DLQ back into service

```ts
const result = await harbor.redrive({
  from: 'orders-dlq',
  to: 'orders',               // default: each message's own x-original-topic header
  groupId: 'orders-dlq-redrive', // default: `${from}-redrive`; the offset persists between runs
  max: 500,                    // stop after this many; default: no limit
  idleTimeout: '5s',           // stop once nothing arrived for this long; default
  filter: (message) => message.retry?.lastError !== 'ValidationError: bad sku'  // false skips (committed, not re-injected)
})
result // { from: 'orders-dlq', reprocessed: 498, skipped: 2 }
```

Each message is re-produced with its original key and value; the failed run's tracking headers are removed so it starts a fresh retry ladder, and `x-redriven-from` / `x-redriven-at` record the operation. The DLQ offset is committed only after the broker acknowledged the re-produce, so an interrupted redrive resumes where it stopped. A `messageRedriven` event fires per message. A message without `x-original-topic` fails the run unless `to` is given; a filter that throws, or a re-produce that is not acknowledged, stops the run with that error and leaves the message uncommitted.

## Graceful shutdown

```ts
await harbor.shutdown('30s')   // or harbor.enableSignalHandlers() for SIGTERM/SIGINT
```

1. Every consumer stops taking new messages. Messages waiting for a retry delay are released at once, uncommitted.
2. Handlers already running get up to the timeout to finish. The ones that finish commit their offsets on the way out.
3. Handlers still running when the timeout elapses are abandoned: their `ctx.signal` aborts, their offsets are **not** committed (the messages will be redelivered), and `shutdown()` rejects with `ShutdownTimeoutError` after everything else is done. At-least-once, said out loud.
4. Consumers leave their groups, then the client disconnects.

## Health

```ts
harbor.isHealthy()   // boolean, for a liveness probe
harbor.health()      // { healthy, state, adapter, consumers: [{ groupId, status, stoppedBecause }] }
```

Synchronous and cheap: it reads the state the harbor already tracks and never calls the broker. A harbor is healthy until it shuts down or until a consumer stops on its own (`stoppedBecause` is `'abort'` or `'crash'`); a consumer stopped by `shutdown()` does not count against it. Connection is lazy by design, so a harbor that has not connected yet is healthy.

## Serialization

The default is JSON, strict. `JSON.stringify` turns `Map`, `Set`, typed arrays, `RegExp`, `Error` and `Promise` into `{}` without a word, drops `undefined` inside objects, and encodes `NaN` as `null`. kafka-harbor rejects every one of those shapes with `SerializationError` before a byte is produced, so the handler always gets what the producer meant. `Date` is the single conversion accepted (encoded as ISO-8601, decoded as a string).

```ts
import { createHarbor, jsonSerializer, rawSerializer, stringSerializer, type Serializer } from 'kafka-harbor'
import { confluentAdapter } from 'kafka-harbor/adapters/confluent'

const harbor = createHarbor({ clientId: 'orders-service', brokers, adapter: confluentAdapter(), serializer: jsonSerializer() }) // default
harbor.producer({ serializer: rawSerializer() })                          // Buffer in, Buffer out
consumer.subscribe('logs', handler, { serializer: stringSerializer() })   // UTF-8 text

const msgpack: Serializer<MyType> = {
  serialize: (value, topic) => encode(value),
  deserialize: (bytes, topic) => decode(bytes)
}
```

A serializer applies harbor-wide, per producer, per consumer or per topic, most specific wins.

## Headers

Headers are strings, both ways. The automatic ones use the `x-` prefix (shared with the RabbitMQ sibling library); change it per harbor:

```ts
import { createHarbor } from 'kafka-harbor'
import { confluentAdapter } from 'kafka-harbor/adapters/confluent'

const harbor = createHarbor({
  clientId: 'orders-service',
  brokers,
  adapter: confluentAdapter(),
  headers: {
    prefix: '',                                   // 'correlation-id', 'retry-count', ...
    correlationId: () => asyncLocalStorage.getStore()?.requestId ?? randomUUID()
  }
})
harbor.headerNames.retryCount   // the names in effect, for code that reads them
```

## Events

```ts
harbor
  .on('connected', ({ adapter }) => {})
  .on('disconnected', ({ adapter }) => {})
  .on('messageProcessed', ({ topic, partition, offset, groupId, durationMs, correlationId }) => {})
  .on('messageFailed', ({ topic, offset, error, outcome }) => {})   // outcome: 'retry' | 'dead-letter' | 'abort' | 'crash'
  .on('messageRetried', ({ topic, retryTopic, level, attempt, error }) => {})
  .on('messageDeadLettered', ({ topic, dlqTopic, attempts, error }) => alert(`${topic}: ${attempts} attempts, now in ${dlqTopic}`))
  .on('messageRedriven', ({ from, to, offset }) => {})
  .on('consumerStopped', ({ groupId, reason }) => {})               // reason: 'shutdown' | 'abort' | 'crash'
  .on('error', ({ error, scope, groupId, topic }) => {})
```

`messageDeadLettered` fires after the DLQ produce was acknowledged, never on the attempt. A listener that throws is reported to the logger and does not affect processing.

## Errors

Every error carries a stable `code`; message text is documentation, not contract. Branch on `code` or use the guards, never on `instanceof`: an application can end up with the ESM and CJS builds of this package in one process, and class identity does not cross that line.

| Class | `code` | When |
|---|---|---|
| `ConfigError` | `CONFIG_INVALID` | an option is invalid; thrown at construction, naming the option |
| `SerializationError` | `SERIALIZATION` | a value cannot be encoded or decoded faithfully |
| `AbortProcessingError` | `ABORT_PROCESSING` | built by `harbor.abort()`; stops the consumer without committing |
| `TopicMissingError` | `TOPIC_MISSING` | a retry or DLQ topic does not exist and `autoCreateTopics` is off |
| `AdapterError` | `ADAPTER` | the client reported a failure |
| `ClosedError` | `CLOSED` | the harbor is shutting down or closed |
| `ShutdownTimeoutError` | `SHUTDOWN_TIMEOUT` | handlers were abandoned by shutdown; `inFlight` says how many |

Every error has `retryable`. Throw any error with `retryable: false` from a handler and it goes straight to the DLQ; breakwater's errors and the RabbitMQ sibling's `RetryableError` follow the same convention.

## Adapters

```ts
import { confluentAdapter } from 'kafka-harbor/adapters/confluent'

confluentAdapter({
  global: { 'socket.keepalive.enable': true },       // librdkafka properties, every client
  producer: { 'linger.ms': 5 },
  consumer: { 'fetch.min.bytes': 1024 },
  adminTimeoutMs: 30_000
})
```

The Confluent adapter sets `acks=all` and `enable.idempotence=true` on the producer, `enable.auto.commit=false` on consumers, and loads the client module on first connect, so importing the adapter never touches the native binding. The three properties the offset policy depends on (`enable.auto.commit`, `enable.auto.offset.store`, `auto.offset.reset`) cannot be overridden through the passthrough; `fromBeginning` drives the reset policy. Client failures are retryable unless their code is definitive (authorization, oversized record, invalid argument), regardless of the client's own `retriable` flag, which only describes transactions.

Writing your own adapter means implementing `ClientAdapter` (about 150 lines for the Confluent one) and running `runAdapterContract` from `kafka-harbor/testing` against your backend. The contract is small on purpose: connect, disconnect, produce with acknowledgment, consume with per-partition ordering and a settled-promise gate, commit, stop, optional pause/resume, and two admin calls. Everything else lives in the core.

## Testing your handlers

```ts
import { createHarbor } from 'kafka-harbor'
import { memoryAdapter } from 'kafka-harbor/testing'

const adapter = memoryAdapter()
const harbor = createHarbor({ clientId: 'test', brokers: ['memory'], adapter })

const consumer = harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
consumer.subscribe('orders', onOrder)
await harbor.producer().send('orders', { value: { id: 1 } })
await consumer.start()
await adapter.whenDrained('g', 'orders')

adapter.messages('orders-dlq')        // what landed where
adapter.committed('g', 'orders', 0)   // '1'
adapter.calls                         // every adapter call, in order
```

No Docker, no broker, real pipeline: the same core code that runs in production drives an in-memory broker with topics, partitions, consumer groups and committed offsets. [examples/testing-handlers.ts](examples/testing-handlers.ts) is a complete handler test written this way; [examples/retry-dlq-flow.ts](examples/retry-dlq-flow.ts) runs the whole retry and DLQ flow against the broker from `docker-compose.yml`.

## Development

```bash
npm install
npm run hooks              # once per clone
npm test                   # unit + adapter contract on the memory adapter
npm run test:integration   # the same contract on a real Kafka, plus the retry/DLQ flow (needs Docker)
npm run lint
npm run check:types && npm run check:types:next
npm run check:dist         # build, then compile a consumer against the published declarations
npm run check:docs         # every ```ts block in the docs type-checks against src/; every anchor resolves
npm run api:check          # public API frozen by the report in etc/
npm run test:mutation:changed
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the invariants worth knowing before changing anything, and [docs/](docs/) for delivery semantics and adapter authoring.

## Related

- [breakwater](https://github.com/pinceladasdaweb/breakwater): resilience policies (retry, circuit breaker, bulkhead). kafka-harbor's produce retry runs on it.

## License

MIT

# Writing a client adapter

A `ClientAdapter` connects the core to a Kafka client. It is deliberately
small: the protocol calls and nothing else. Retry topics, the DLQ,
serialization, offset policy and shutdown are the core's job, and an adapter
that starts doing any of them is a bug.

The Confluent adapter (`src/adapters/confluent/index.ts`) is the reference:
about 150 lines, no logic beyond translation.

## The contract

```ts
interface ClientAdapter {
  readonly name: string
  connect (config: BrokerConfig): Promise<void>
  disconnect (): Promise<void>
  produce (records: readonly RawRecord[]): Promise<void>
  consume (options: ConsumeOptions): Promise<ConsumerHandle>
  readonly admin: { createTopics, topicExists }
}
```

Bytes cross the boundary as `Buffer`. Headers arrive as the client exposes
them (`RawHeaders`: Buffer, string, or an array for a repeated key) and go
out as UTF-8 strings. Offsets are decimal strings; the offset to commit is
the **next** one to read (`message.offset + 1`), Kafka's own convention.

### produce

Resolves only after the broker acknowledged every record. The core commits a
source offset right after this resolves; a produce that resolves early is a
lost message. Configure the client for `acks=all` and an idempotent producer.
Reject when any record was not acknowledged.

### consume

`eachMessage` is called once per message, in order within a partition. Two
rules the core relies on:

1. Do not deliver the next message of a partition until the previous
   `eachMessage` promise settled. The core's per-message commit depends on
   it.
2. Never commit on your own. `enable.auto.commit=false`, or the equivalent.

`concurrency` is the number of partitions processed at once; `fromBeginning`
is where a brand-new group starts; `onPartitionsRevoked` (optional) is called
before a rebalance takes partitions away; `onError` reports fetch-loop
errors that belong to no message.

The returned `ConsumerHandle` has `commit`, `stop`, and optionally `pause`
and `resume`. `stop()` leaves the group and releases the client. The core
settles every delivery it abandoned before calling `stop()`, so an adapter
that waits for in-flight `eachMessage` calls does not deadlock.

### admin

`createTopics` treats an already-existing topic as success (the outcome is
what was asked for). `topicExists` tells the truth.

## Errors

Wrap client errors in `AdapterError` from `kafka-harbor`, with the original
as `cause`. Carry the client's retryability hint as `retryable` when it has
one; the producer's retry policy reads it. Import from the package root, never
from a deep path: the build externalizes only the core bundle, and a deep
import would inline a private copy of the error class.

## Loading the client lazily

Load the client module on `connect()`, not at import time, and make it
injectable for tests. The Confluent adapter accepts `client` as a module or a
factory and falls back to `import('@confluentinc/kafka-javascript')`. An
application that never uses the adapter never pays for the client, and the
adapter's own unit tests run without a native binding.

## Proving it

Run the shared contract suite against the real backend:

```ts
import { runAdapterContract } from '../contract/adapter-contract'

runAdapterContract('mine', async () => {
  const adapter = myAdapter()
  await adapter.connect({ clientId: 'contract', brokers })
  return {
    adapter,
    topic: async (label, partitions = 1) => { /* create a unique topic, return its name */ },
    group: (label) => `${label}-${run}`,
    teardown: () => adapter.disconnect()
  }
})
```

The eight invariants are listed in the suite's header. An adapter that
passes them can be dropped into any harbor; an adapter that needs one of them
relaxed has found either a bug in the client or a leak in the contract, and
both are worth an issue.

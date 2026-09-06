# Writing a client adapter

A `ClientAdapter` connects the core to a Kafka client. It is deliberately
small: the protocol calls and nothing else. Retry topics, the DLQ,
serialization, offset policy and shutdown are the core's job, and an adapter
that starts doing any of them is a bug.

The Confluent adapter (`src/adapters/confluent/index.ts`) is the reference:
about 150 lines, no logic beyond translation.

## The contract

```ts
import type { AdminApi, BrokerConfig, ClientAdapter, ConsumeOptions, ConsumerHandle, RawRecord } from 'kafka-harbor'

const shape: ClientAdapter = {
  name: 'mine',
  connect: async (config: BrokerConfig) => {},
  disconnect: async () => {},
  produce: async (records: readonly RawRecord[]) => {},
  consume: async (options: ConsumeOptions): Promise<ConsumerHandle> => ({ commit: async () => {}, stop: async () => {} }),
  admin: { createTopics: async () => {}, topicExists: async () => false } satisfies AdminApi
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
is where a brand-new group starts; `maxProcessingTimeMs` is the longest one
`eachMessage` call may take, retry delay included, for the client setting
that decides how long a member may go without polling (`max.poll.interval.ms`
in librdkafka; ignore it if your client has no such knob); `onError` reports
fetch-loop errors that belong to no message.

`onPartitionsRevoked` (optional) is called when a rebalance is about to take
partitions away, and the adapter must **await it before releasing them**: the
core uses that time to let the handlers still running on those partitions
finish and commit, so the next owner does not repeat their work. The core
bounds the wait by `maxProcessingTimeMs`; an adapter whose client cannot
delay a revocation simply does not call it.

The core may call `consume` more than once for the same group with disjoint
topic sets (one call per retry level), so an adapter must not assume a
single consumer per group.

The returned `ConsumerHandle` has `commit`, `stop`, and optionally `pause`
and `resume`. `stop()` leaves the group and releases the client. The core
settles every delivery it abandoned before calling `stop()`, so an adapter
that waits for in-flight `eachMessage` calls does not deadlock.

### admin

`createTopics` treats an already-existing topic as success (the outcome is
what was asked for) and resolves only once the topics are **visible**: a
broker acknowledges a creation before every replica serves the new metadata,
and a `consume` or `topicExists` issued right after must find the topic.
Poll the metadata until it does, bounded by your admin timeout. `topicExists`
tells the truth.

## Errors

Wrap client errors in `AdapterError` from `kafka-harbor`, with the original
as `cause` (`describeError` from the package root turns any thrown value into
the message text). Carry the client's retryability hint as `retryable` when it has
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

Run the shared contract suite, exported from `kafka-harbor/testing`, against the real backend:

```ts
import { runAdapterContract } from 'kafka-harbor/testing'

runAdapterContract('mine', async () => {
  const adapter = myAdapter()
  await adapter.connect({ clientId: 'contract', brokers })
  return {
    adapter,
    topic: async (label, partitions = 1) => {
      const topic = `${label}-${run}`
      await adapter.admin.createTopics([{ topic, partitions, replicationFactor: 1 }])
      return topic
    },
    group: (label) => `${label}-${run}`,
    teardown: () => adapter.disconnect()
  }
})
```

The suite is a `node:test` suite; run the file with `node --test`. The nine invariants are listed in its header. An adapter that
passes them can be dropped into any harbor; an adapter that needs one of them
relaxed has found either a bug in the client or a leak in the contract, and
both are worth an issue.

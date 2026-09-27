# Delivery semantics

kafka-harbor is **at-least-once**, and says so at every corner where a
library could quietly become at-most-once.

## Where the offset moves

The consumer commits the offset of a message in exactly two situations:

1. The handler returned.
2. The handler threw, and the re-produce to the next retry topic or to the
   DLQ was **acknowledged by the broker**.

Nothing else commits. In particular:

- `harbor.abort(error)` thrown from a handler stops the consumer with the
  offset uncommitted. The message is redelivered after a restart. It is the
  escape hatch for infrastructure bugs where reprocessing is the correct
  outcome and neither a retry topic nor the DLQ is.
- A retry/DLQ produce that fails (after the producer's own retries) stops the
  consumer with the offset uncommitted, and emits `error`.
- A commit that fails (a rebalance in progress, a coordinator timeout) does
  **not** stop the consumer: the work behind it is already safe, so the
  failure is reported through `error` and the message is redelivered. The
  `messageProcessed`, `messageRetried` and `messageDeadLettered` events are
  only emitted for a committed offset. A redrive makes the same call: a DLQ
  offset whose commit fails after the re-produce is reported and the run
  goes on; that dead letter is re-injected again on the next redrive.
- A handler abandoned by shutdown (still running after the timeout) does not
  commit, whatever it returns or throws afterwards.
- With no retry level left and the DLQ disabled, a failure stops the
  consumer with the offset uncommitted.

The consumer stopping is the loud failure. It is never the default; it is
what remains when every safe destination is unavailable.

## Batches

`subscribeBatch` moves the offset once per batch, after the last message
of the batch, and only once the batch resolved or every failed message of
it was forwarded and acknowledged. Nothing in a batch is committed before
the batch is done, so a crash mid-batch redelivers the whole batch: at-
least-once at the batch's grain. A batch still collecting when the
consumer stops was never committed and comes back to the next member; one
collecting when a partition is taken away runs before the partition is
released, the way a running handler does.

## Stopping on purpose

`harbor.abort()` and the no-destination-left case stop the consumer. With
`concurrency > 1`, handlers running on the other partitions at that moment
are not the reason for the stop, so they get the grace a shutdown gives them
(30 seconds): they finish, commit, and only then does the consumer leave the
group. A handler still running after that is abandoned the way a shutdown
timeout abandons it: its `ctx.signal` aborts, its offset is not committed,
and its message is redelivered. Nothing is lost; effects it had already
produced may run twice.

## Coming back from the DLQ

`harbor.redrive()` reads a dead-letter topic with its own consumer group and
re-produces each message, bytes untouched, to its original topic. It commits
the DLQ offset only after the re-produce was acknowledged: the same rule as
everywhere else, so a redrive killed halfway resumes from the last committed
message and never drops one. The redriven message starts a fresh ladder (the
old tracking headers are removed); if it fails again it walks the retry
topics again and lands in the DLQ again, with `x-redriven-from` still on it.

## Duplicates you can expect

At-least-once means these can happen and your handler should tolerate them:

- **Crash between handler and commit.** The handler ran; the process died
  before the commit reached the broker. The next member reprocesses the
  message.
- **Rebalance during processing.** When the client announces that a
  partition is being taken away, the consumer lets the handler running on it
  finish and commit before the partition is released (bounded by
  `maxProcessingTime`), so the new owner usually starts after that message.
  A handler that does not finish in time is the exception: the new owner
  starts from the last committed offset and repeats it.
- **Shutdown timeout.** An abandoned handler may have completed its side
  effects; the message is redelivered anyway.
- **Retry produce acknowledged, commit failed.** The message exists on the
  retry topic *and* gets redelivered from the source, so the handler sees it
  twice (once per topic) and both copies walk the ladder.

For exactly-once *effects* on top of at-least-once *delivery*, run the
handler through an idempotency engine (`consumer({ idempotency })`, see the
README). The default key, `groupId:topic:partition:offset`, collapses a
redelivery whose first run completed; one that arrives while the first run
is still executing is a conflict the engine's policy decides (quayside's
`onConflict: 'wait'` waits and replays). The last case is two deliveries
with two keys; a business key collapses that one too, and the duplicates
the producer sent.

## Transactions

`harbor.transaction()` and `ctx.transaction()` run on a transactional
producer (`transactionalId` on the harbor). What `tx.send()` produced is
written to the brokers as the function runs, marked as part of an open
transaction; a consumer reading committed data (the default in both
clients) sees none of it until the commit, and never sees it after an
abort. Inside a handler, the consumed offset (the batch's last, for a batch
handler) is added to the transaction before the commit, so the offset
commit and the produces land together: a crash between them cannot leave
the offset committed with nothing produced, nor the records produced with
the offset uncommitted. That is exactly-once from the consumed topic to the
produced ones, and only that: anything the function did outside Kafka ran
once per attempt and is not rolled back by an abort.

What the transaction does not cover: the consumer's own commit after the
handler (a repeat of an offset already committed, harmless); the retry and
DLQ hops, produced outside any transaction, which is why `ctx.transaction`
must be the last thing a handler does: a handler that throws after its
transaction committed walks the ladder with the plain producer, and a crash
between the transactional commit and that hop loses the message, offset
committed and nothing forwarded. A batch message that does not decode is
dead-lettered before the handler runs for the same reason, and a batch
handler must not throw `BatchFailedError` after `ctx.transaction`. A
shutdown mid-transaction disconnects the producer and aborts it, leaving
the message uncommitted for the next member. Transactions run one at a
time per harbor, and one begun from inside another is refused. A
transaction the broker times out (a minute by default) has its commit
refused; a transaction that could be neither committed nor aborted has its
producer reopened by the adapter for the next one.

## Retry delays

A message on `orders-retry-N` becomes due `levels[N-1].delay` after its
broker timestamp, so the delay is observed even when the retry topic is
otherwise idle. The wait is never longer than the level's delay: a broker
or producer clock ahead of the consumer's does not stretch it, and a
message on the original topic never waits at all.

A message that arrives before it is due is **parked**: the consumer pauses
its partition through the adapter, keeps the message in memory with a
timer, and reports the delivery settled, so the client keeps polling and
the group never evicts the member for a long delay. When the timer fires
the message goes through the pipeline as a delivery of its own, and the
partition resumes once it is done; the next message on it, produced later
and so due later, is delivered then and parks for what is left of its own
delay. Nothing is committed while a message is parked: a crash, a stop or a
rebalance leaves it on the retry topic for the next member, and a stop or a
revocation cancels the timer and resumes the partition so the pause never
outlives the assignment. Parking holds one message per retry partition in
memory; a paused partition delivers nothing else meanwhile. A parked message
comes back outside the adapter's delivery gate, so parked messages take
`concurrency` slots of their own per level: at most that many run at a
time among themselves, on top of what the adapter delivers on the
partitions that are not paused.

Three consequences:

- A delay may be as long as a timer holds, whatever `maxProcessingTime`
  says, because the delivery does not wait. Only an adapter without
  `pause`/`resume` makes the delivery itself sleep, and then every delay
  must fit under `maxProcessingTime`, the client's poll interval:
  `start()` rejects with a `ConfigError` naming the adapter otherwise. The
  three in-tree adapters pause.
- Retention on each retry topic must exceed that level's delay, or the
  message expires before it is due.
- Each level is a group member of its own. A consumer with N levels joins
  its group N+1 times: once for the original topics, once per level. A
  message parked on `orders-retry-2` therefore holds no worker that
  `orders` or `orders-retry-1` is waiting for, whatever `concurrency` is;
  the original partition keeps flowing while retries wait.

## Holding a partition

With `breaker` on a consumer or a subscription, a topic whose circuit is
open holds its partitions: the message in front of each one is neither
committed nor forwarded, and nothing behind it is delivered until the
circuit lets a probe through. Nothing is lost by a hold; the offset does
not move. A hold ends in one of three ways. The probe succeeds and the
circuit closes: the held message runs, commits, and the partition flows
again. `hold` runs out (default: what is left of `maxProcessingTime`): the
message fails with `HoldExpiredError`, retryable, so it takes the next hop
of the ladder like any transient failure, and its retry copy meets the same
circuit later. The consumer stops, or a rebalance takes the partition away: the message
stays uncommitted and the next member gets it. `hold` is counted from the
moment the handler would have run, and never reaches past what the client
tolerates for the delivery as a whole (`maxProcessingTime` from when the
message was delivered). A parked retry is a fresh delivery, so the hold
starts over on every level; only with an adapter that cannot pause does the
delay already spent count against it. The probe itself is the next message of any held
partition of that topic; a probe that fails walks the ladder with its own
error and reopens the circuit.

## Shared retry topics

A retry topic serves every subscription whose naming function produced its
name. With the default naming (`orders-retry-1`) that is one; with a naming
function that returns the same name for every topic of a level
(`svc-retry-1`) the topic is shared, and the `x-original-topic` header
decides which handler a message belongs to. That header is network input:
a message on a shared topic whose header names no subscription of the
consumer is dead-lettered to the owners' DLQ when they share one, tracking
headers untouched, so someone can look. When the owners have different
DLQs there is no honest destination and the consumer stops with the offset
uncommitted. An unshared retry topic never consults the header for routing:
its single owner is the answer, and a corrupt tracking block there is a
first delivery, as before.

Levels never mix. A name that is level 1 of one topic and level 2 of another
is refused at `subscribe()`, because the level decides the delay a message
waits and a message must not wait one ladder's delay on another's.

## Ordering

Order is preserved within a partition on the original topic. A message that
goes through a retry topic leaves that order: it is re-produced with the same
key, so it lands on a deterministic partition of the retry topic, but
messages produced after it on the original topic will be processed before it
retries. This is the standard trade-off of non-blocking retry (the
alternative, blocking the partition until the message succeeds, is what
`harbor.abort()` gives you for the cases that need it).

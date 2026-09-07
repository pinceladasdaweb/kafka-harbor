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

For exactly-once *effects* on top of at-least-once *delivery*, deduplicate
in the handler by a business key, or by `topic:partition:offset` when the
message has no natural one.

## Retry delays

A message on `orders-retry-N` becomes due `levels[N-1].delay` after its
broker timestamp. The retry consumer sleeps until then, so the delay is
observed even when the retry topic is otherwise idle, and it is bounded:
every delay must fit under `maxProcessingTime` (default 5 minutes), or the
construction fails naming the level. The wait is never longer than the
level's delay: a broker or producer clock ahead of the consumer's does not
stretch it, and a message on the original topic never waits at all.

Three consequences:

- A long ladder (`10m`, `1h`) needs a longer `maxProcessingTime`. The
  adapter receives that number as `maxProcessingTimeMs` and the Confluent
  adapter sets the client's `max.poll.interval.ms` from it, so the client
  tolerates every delay the core accepted. A `consumer` passthrough that
  pins `max.poll.interval.ms` wins; keep it above the longest delay plus the
  handler's own time, or the group evicts the sleeping consumer.
- Retention on each retry topic must exceed that level's delay, or the
  message expires before it is due.
- Each level is a group member of its own. A consumer with N levels joins
  its group N+1 times: once for the original topics, once per level. A
  message sleeping on `orders-retry-2` therefore holds no worker that
  `orders` or `orders-retry-1` is waiting for, whatever `concurrency` is;
  the original partition keeps flowing while retries wait.

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

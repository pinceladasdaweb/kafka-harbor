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
  only emitted for a committed offset.
- A handler abandoned by shutdown (still running after the timeout) does not
  commit, whatever it returns or throws afterwards.
- With no retry level left and the DLQ disabled, a failure stops the
  consumer with the offset uncommitted.

The consumer stopping is the loud failure. It is never the default; it is
what remains when every safe destination is unavailable.

## Stopping on purpose

`harbor.abort()` and the no-destination-left case stop the consumer at once.
With `concurrency > 1`, handlers running on the other partitions at that
moment are abandoned the same way a shutdown timeout abandons them: their
`ctx.signal` aborts, their offsets are not committed, and their messages
are redelivered. Nothing is lost; effects they had already produced may run
twice.

## Duplicates you can expect

At-least-once means these can happen and your handler should tolerate them:

- **Crash between handler and commit.** The handler ran; the process died
  before the commit reached the broker. The next member reprocesses the
  message.
- **Rebalance during processing.** A partition revoked while its handler
  runs is reassigned; the new owner starts from the last committed offset.
- **Shutdown timeout.** An abandoned handler may have completed its side
  effects; the message is redelivered anyway.
- **Retry produce acknowledged, commit failed.** The message exists on the
  retry topic *and* gets redelivered from the source, so the handler sees it
  twice (once per topic) and both copies walk the ladder.

For exactly-once *effects* on top of at-least-once *delivery*, deduplicate
in the handler by a business key: the planned `kafka-harbor` idempotency
integration is [quayside](https://github.com/pinceladasdaweb/quayside) keyed
by `topic:partition:offset` or by a message id of your own.

## Retry delays

A message on `orders-retry-N` becomes due `levels[N-1].delay` after its
broker timestamp. The retry consumer sleeps until then, so the delay is
observed even when the retry topic is otherwise idle, and it is bounded:
every delay must fit under `maxProcessingTime` (default 5 minutes,
`max.poll.interval.ms`), or the construction fails naming the level.

Two consequences:

- A long ladder (`10m`, `1h`) needs a longer `maxProcessingTime` **and** a
  client configured with a matching `max.poll.interval.ms`. Otherwise the
  group evicts the sleeping consumer.
- Retention on each retry topic must exceed that level's delay, or the
  message expires before it is due.

A more precise mechanism (pause the partition, resume on a timer, no
sleeping consumer) is planned for a later version; the semantics above will
not change.

## Ordering

Order is preserved within a partition on the original topic. A message that
goes through a retry topic leaves that order: it is re-produced with the same
key, so it lands on a deterministic partition of the retry topic, but
messages produced after it on the original topic will be processed before it
retries. This is the standard trade-off of non-blocking retry (the
alternative, blocking the partition until the message succeeds, is what
`harbor.abort()` gives you for the cases that need it).

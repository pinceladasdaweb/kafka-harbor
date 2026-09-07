/**
 * The ClientAdapter contract, as a `node:test` suite every adapter must pass
 * unmodified against its real backend. Exported from `kafka-harbor/testing`
 * so an adapter written outside this repository can prove itself the same
 * way the in-tree ones do. Named invariants:
 *
 *  1. Bytes are faithful: key, value and headers arrive as produced.
 *  2. Order holds within a partition.
 *  3. A committed offset survives a stop: the next consumption of the group
 *     starts there.
 *  4. Nothing is committed implicitly: without a commit, a stop redelivers.
 *  5. eachMessage gates the partition: the next message of a partition is
 *     not delivered before the previous promise settled.
 *  6. Partitions run concurrently up to `concurrency`, when there is more
 *     than one.
 *  7. Admin: createTopics is idempotent and topicExists tells the truth.
 *  8. pause/resume, when implemented, stop and restart delivery, and a
 *     pause does not outlive the consumption that set it.
 *  9. A tombstone (null value) arrives as null, not as empty bytes.
 * 10. Offsets, when implemented: the high watermark is the next offset to
 *     be produced, the low one the first still held (a partition without a
 *     leader yet may be left out until it has one), and a committed offset
 *     reads back as committed, while a partition never committed on reads
 *     back as null or is left out.
 *
 * In this repository the suite runs against the in-memory adapter in the
 * unit run and against the Confluent adapter on a Testcontainers broker in
 * the integration run. The second adapter is what proves the contract did
 * not leak the client.
 */
import assert from 'node:assert/strict'
import { after, before, describe, test } from 'node:test'

import { decodeHeaders, type ClientAdapter, type CommittedOffset, type RawMessage } from '../index'

export interface AdapterContractSetup {
  /** A connected adapter, fresh for the whole suite. */
  adapter: ClientAdapter
  /** A topic name unique to this run, created with `partitions` partitions. */
  topic: (label: string, partitions?: number) => Promise<string>
  /** A group id unique to this run. */
  group: (label: string) => string
  /** How long to wait for deliveries, ms. */
  timeoutMs?: number
  teardown?: () => Promise<void>
}

const waitFor = async (condition: () => boolean, timeoutMs: number, what: string): Promise<void> => {
  const deadline = Date.now() + timeoutMs
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}

const bytes = (value: string): Buffer => Buffer.from(value, 'utf8')
const valueOf = (message: RawMessage): string => message.value?.toString('utf8') ?? ''

export function runAdapterContract (name: string, setup: () => Promise<AdapterContractSetup>): void {
  describe(`ClientAdapter contract: ${name}`, () => {
    let ctx: AdapterContractSetup
    let timeoutMs: number

    before(async () => {
      ctx = await setup()
      timeoutMs = ctx.timeoutMs ?? 15_000
    })

    after(async () => {
      await ctx.teardown?.()
    })

    const consumeAll = async (topic: string, groupId: string, count: number, options: { fromBeginning?: boolean, concurrency?: number, commitEach?: boolean, hold?: (message: RawMessage) => Promise<void> } = {}) => {
      const received: RawMessage[] = []
      const handle = await ctx.adapter.consume({
        groupId,
        topics: [topic],
        fromBeginning: options.fromBeginning ?? true,
        ...(options.concurrency !== undefined && { concurrency: options.concurrency }),
        eachMessage: async (message) => {
          await options.hold?.(message)
          received.push(message)
          if (options.commitEach === true) {
            await handle.commit([{ topic, partition: message.partition, offset: (BigInt(message.offset) + 1n).toString() }])
          }
        }
      })
      await waitFor(() => received.length >= count, timeoutMs, `${count} message(s) on ${topic}`)
      return { received, handle }
    }

    test('1. delivers key, value and headers byte for byte, with offsets and partition', async () => {
      const topic = await ctx.topic('faithful')
      await ctx.adapter.produce([
        { topic, key: bytes('k1'), value: bytes('{"n":1}'), headers: { 'x-a': 'one', 'x-b': 'two' } },
        { topic, key: null, value: bytes(''), headers: {} },
        { topic, key: bytes('k3'), value: bytes('éà utf8'), headers: { 'x-utf': 'ç' } }
      ])
      const { received, handle } = await consumeAll(topic, ctx.group('faithful'), 3)
      await handle.stop()
      assert.equal(received.length, 3)
      const [first, second, third] = received as [RawMessage, RawMessage, RawMessage]
      assert.equal(first.topic, topic)
      assert.equal(first.partition, 0)
      assert.equal(first.offset, '0')
      assert.deepEqual(first.key, bytes('k1'))
      assert.deepEqual(first.value, bytes('{"n":1}'))
      assert.deepEqual(decodeHeaders(first.headers), { 'x-a': 'one', 'x-b': 'two' })
      assert.ok(Number.isFinite(first.timestamp) && first.timestamp > 0)
      assert.equal(second.key, null)
      assert.equal(second.offset, '1')
      assert.deepEqual(second.value, bytes(''))
      assert.deepEqual(third.value, bytes('éà utf8'))
      assert.deepEqual(decodeHeaders(third.headers), { 'x-utf': 'ç' })
    })

    test('2. preserves order within a partition', async () => {
      const topic = await ctx.topic('order')
      const values = Array.from({ length: 25 }, (_, i) => `m${i}`)
      await ctx.adapter.produce(values.map((value) => ({ topic, key: bytes('same-key'), value: bytes(value), headers: {} })))
      const { received, handle } = await consumeAll(topic, ctx.group('order'), values.length)
      await handle.stop()
      assert.deepEqual(received.map(valueOf), values)
      assert.deepEqual(received.map((m) => m.offset), values.map((_, i) => String(i)))
    })

    test('3. a committed offset survives stop: the next consumption starts there', async () => {
      const topic = await ctx.topic('commit')
      const groupId = ctx.group('commit')
      await ctx.adapter.produce(['a', 'b', 'c', 'd'].map((value) => ({ topic, key: null, value: bytes(value), headers: {} })))
      const first = await consumeAll(topic, groupId, 4)
      await first.handle.commit([{ topic, partition: 0, offset: '2' }])
      await first.handle.stop()

      const second = await consumeAll(topic, groupId, 2)
      await second.handle.stop()
      assert.deepEqual(second.received.map(valueOf), ['c', 'd'])
    })

    test('4. without a commit, a stop redelivers everything', async () => {
      const topic = await ctx.topic('redeliver')
      const groupId = ctx.group('redeliver')
      await ctx.adapter.produce(['a', 'b'].map((value) => ({ topic, key: null, value: bytes(value), headers: {} })))
      const first = await consumeAll(topic, groupId, 2)
      await first.handle.stop()
      const second = await consumeAll(topic, groupId, 2)
      await second.handle.stop()
      assert.deepEqual(second.received.map(valueOf), ['a', 'b'])
    })

    test('5. the next message of a partition waits for the previous eachMessage to settle', async () => {
      const topic = await ctx.topic('gate')
      await ctx.adapter.produce(['a', 'b', 'c'].map((value) => ({ topic, key: null, value: bytes(value), headers: {} })))
      let inFlight = 0
      let overlap = false
      const { received, handle } = await consumeAll(topic, ctx.group('gate'), 3, {
        hold: async () => {
          inFlight++
          if (inFlight > 1) overlap = true
          await new Promise((resolve) => setTimeout(resolve, 30))
          inFlight--
        }
      })
      await handle.stop()
      assert.equal(received.length, 3)
      assert.equal(overlap, false)
    })

    test('6. partitions are processed concurrently up to the concurrency option', async () => {
      const topic = await ctx.topic('concurrent', 2)
      await ctx.adapter.produce([
        ...['a', 'b', 'c'].map((value) => ({ topic, key: null, value: bytes(value), headers: {}, partition: 0 })),
        ...['x', 'y', 'z'].map((value) => ({ topic, key: null, value: bytes(value), headers: {}, partition: 1 }))
      ])
      let inFlight = 0
      let maxInFlight = 0
      const { received, handle } = await consumeAll(topic, ctx.group('concurrent'), 6, {
        concurrency: 2,
        hold: async () => {
          inFlight++
          maxInFlight = Math.max(maxInFlight, inFlight)
          await new Promise((resolve) => setTimeout(resolve, 60))
          inFlight--
        }
      })
      await handle.stop()
      assert.equal(received.length, 6)
      assert.equal(maxInFlight, 2)
      assert.deepEqual(received.filter((m) => m.partition === 0).map(valueOf), ['a', 'b', 'c'])
      assert.deepEqual(received.filter((m) => m.partition === 1).map(valueOf), ['x', 'y', 'z'])
    })

    test('7. createTopics is idempotent and topicExists tells the truth', async () => {
      const topic = await ctx.topic('admin')
      assert.equal(await ctx.adapter.admin.topicExists(topic), true)
      assert.equal(await ctx.adapter.admin.topicExists(`${topic}-never-created`), false)
      await ctx.adapter.admin.createTopics([{ topic, partitions: 1, replicationFactor: 1 }])
      await ctx.adapter.admin.createTopics([{ topic, partitions: 1, replicationFactor: 1 }, { topic: `${topic}-b`, partitions: 1, replicationFactor: 1 }])
      assert.equal(await ctx.adapter.admin.topicExists(`${topic}-b`), true)
    })

    test('8. pause stops delivery on the partition and resume restarts it', async (t) => {
      const topic = await ctx.topic('pause')
      const groupId = ctx.group('pause')
      const received: RawMessage[] = []
      const handle = await ctx.adapter.consume({
        groupId,
        topics: [topic],
        fromBeginning: true,
        eachMessage: async (message) => { received.push(message) }
      })
      if (handle.pause === undefined || handle.resume === undefined) {
        await handle.stop()
        t.skip('adapter does not implement pause/resume')
        return
      }
      await ctx.adapter.produce([{ topic, key: null, value: bytes('a'), headers: {} }])
      await waitFor(() => received.length === 1, timeoutMs, 'first message before pause')
      handle.pause([{ topic, partition: 0 }])
      await ctx.adapter.produce([{ topic, key: null, value: bytes('b'), headers: {} }])
      await new Promise((resolve) => setTimeout(resolve, 500))
      assert.equal(received.length, 1, 'nothing may arrive while paused')
      handle.resume([{ topic, partition: 0 }])
      await waitFor(() => received.length === 2, timeoutMs, 'message after resume')
      // A pause left in place must not outlive the consumption that set it.
      handle.pause([{ topic, partition: 0 }])
      await handle.stop()
      assert.equal(valueOf(received[1] as RawMessage), 'b')
      await ctx.adapter.produce([{ topic, key: null, value: bytes('c'), headers: {} }])
      const next = await consumeAll(topic, groupId, 3)
      await next.handle.stop()
      assert.equal(valueOf(next.received[2] as RawMessage), 'c', 'the next consumption of the group is not paused')
    })

    test('9. a tombstone arrives as a null value, not as empty bytes', async () => {
      const topic = await ctx.topic('tombstone')
      await ctx.adapter.produce([
        { topic, key: bytes('k'), value: bytes('{"n":1}'), headers: {} },
        { topic, key: bytes('k'), value: null, headers: {} },
        { topic, key: bytes('k'), value: bytes(''), headers: {} }
      ])
      const { received, handle } = await consumeAll(topic, ctx.group('tombstone'), 3)
      await handle.stop()
      assert.deepEqual(received[0]?.value, bytes('{"n":1}'))
      assert.equal(received[1]?.value, null, 'a tombstone is null')
      assert.deepEqual(received[2]?.value, bytes(''), 'an empty value is empty bytes, not null')
    })

    test('10. watermarks and committed offsets read back as produced and committed', async (t) => {
      const { admin } = ctx.adapter
      if (admin.fetchTopicOffsets === undefined || admin.fetchCommittedOffsets === undefined) {
        t.skip('adapter does not report offsets')
        return
      }
      const topic = await ctx.topic('offsets', 2)
      const groupId = ctx.group('offsets')
      // Right after creation a partition may have no leader yet and be left
      // out; both are there once the broker has elected one.
      const fetchTopicOffsets = admin.fetchTopicOffsets.bind(admin)
      const watermarks = async (): Promise<Array<[number, string, string]>> => {
        for (let attempt = 0; ; attempt++) {
          const entries = await fetchTopicOffsets([topic])
          if (entries.length === 2 || attempt === 100) return entries.map((entry): [number, string, string] => [entry.partition, entry.low, entry.high]).sort()
          await new Promise((resolve) => setTimeout(resolve, 50))
        }
      }
      assert.deepEqual(await watermarks(), [[0, '0', '0'], [1, '0', '0']])
      await ctx.adapter.produce([
        ...['a', 'b', 'c'].map((value) => ({ topic, key: null, value: bytes(value), headers: {}, partition: 0 })),
        { topic, key: null, value: bytes('x'), headers: {}, partition: 1 }
      ])
      assert.deepEqual(await watermarks(), [[0, '0', '3'], [1, '0', '1']])

      // A partition never committed on comes back as null or not at all.
      const committedOn = (reported: readonly CommittedOffset[]): Array<[number, string | null]> => {
        const byPartition = new Map(reported.map((entry) => [entry.partition, entry.offset]))
        return [0, 1].map((partition) => [partition, byPartition.get(partition) ?? null])
      }
      assert.deepEqual(committedOn(await admin.fetchCommittedOffsets(groupId, [topic])), [[0, null], [1, null]], 'nothing committed yet')
      const { handle } = await consumeAll(topic, groupId, 4)
      await handle.commit([{ topic, partition: 0, offset: '2' }])
      await handle.stop()
      assert.deepEqual(committedOn(await admin.fetchCommittedOffsets(groupId, [topic])), [[0, '2'], [1, null]])
    })
  })
}

import assert from 'node:assert/strict'
import { describe, test } from 'node:test'

import { type Instrumentation, type MessageHeaders } from '../../src/index'
import { wrapped } from '../../src/instrumentation'
import { harness, silentLogger } from '../helpers/harness'
import { until } from '../helpers/manual-clock'

const capture = () => {
  const lines: string[] = []
  return { lines, logger: { ...silentLogger, error: (message: string) => { lines.push(message) } } }
}

describe('wrapped: instrumentation never changes an outcome', () => {
  test('a well-behaved wrapper sees the work run once and its result come back', async () => {
    const { logger, lines } = capture()
    const order: string[] = []
    const result = await wrapped(async (run) => {
      order.push('before')
      const value = await run()
      order.push('after')
      return value
    }, async () => { order.push('work'); return 42 }, logger)
    assert.equal(result, 42)
    assert.deepEqual(order, ['before', 'work', 'after'])
    assert.deepEqual(lines, [])
  })

  test('without a wrapper the work simply runs, and nothing is logged', async () => {
    const { logger, lines } = capture()
    assert.equal(await wrapped(undefined, async () => 'plain', logger), 'plain')
    assert.deepEqual(lines, [])
  })

  test('a wrapper that parks the outcome while awaiting something else leaves no rejection unobserved', async () => {
    const { logger } = capture()
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown): void => { unhandled.push(reason) }
    process.on('unhandledRejection', onUnhandled)
    try {
      await assert.rejects(wrapped(async (run) => {
        const outcome = run()
        // A macrotask later: long enough for Node to report a rejection nobody handles.
        await new Promise((resolve) => setImmediate(resolve))
        return await outcome
      }, async () => { throw new Error('handler bug') }, logger), /handler bug/)
      await new Promise((resolve) => setImmediate(resolve))
    } finally {
      process.off('unhandledRejection', onUnhandled)
    }
    assert.deepEqual(unhandled, [])
  })

  test('a wrapper that throws before running the work is logged and the work runs unwrapped', async () => {
    const { logger, lines } = capture()
    let runs = 0
    const result = await wrapped(async () => { throw new Error('tracer down') }, async () => { runs++; return 'ok' }, logger)
    assert.equal(result, 'ok')
    assert.equal(runs, 1)
    assert.match(lines[0] ?? '', /threw before running the work: tracer down; running it unwrapped/)
  })

  test('a wrapper that never calls run is logged and the work runs anyway', async () => {
    const { logger, lines } = capture()
    let runs = 0
    const result = await wrapped(async () => 'not the work' as never, async () => { runs++; return 'ok' }, logger)
    assert.equal(result, 'ok')
    assert.equal(runs, 1)
    assert.match(lines[0] ?? '', /did not run the work it wrapped/)
  })

  test('a wrapper that fails after the work succeeded is logged and the result stands', async () => {
    const { logger, lines } = capture()
    const result = await wrapped(async (run) => { await run(); throw new Error('span.end exploded') }, async () => 'ok', logger)
    assert.equal(result, 'ok')
    assert.match(lines[0] ?? '', /threw after the work completed: span.end exploded/)
  })

  test('a failing work fails the same way through a wrapper, and the wrapper is not blamed', async () => {
    const { logger, lines } = capture()
    await assert.rejects(wrapped(async (run) => await run(), async () => { throw new Error('handler bug') }, logger), /handler bug/)
    assert.deepEqual(lines, [])
  })

  test('a wrapper that runs the work twice still runs it once', async () => {
    const { logger } = capture()
    let runs = 0
    const result = await wrapped(async (run) => { await run(); return await run() }, async () => { runs++; return runs }, logger)
    assert.equal(result, 1)
    assert.equal(runs, 1)
  })

  test('a wrapper that keeps run() and calls it after returning finds the work already done, not started again', async () => {
    const { logger } = capture()
    let runs = 0
    let late!: () => Promise<number>
    const result = await wrapped(async (run) => { late = run; return 0 }, async () => { runs++; return runs }, logger)
    assert.equal(result, 1, 'the work ran unwrapped')
    assert.equal(await late(), 1, 'the late call sees that outcome')
    assert.equal(runs, 1)
  })
})

describe('instrumentation hooks in the pipelines', () => {
  const tracing = (): { instrumentation: Instrumentation, calls: string[] } => {
    const calls: string[] = []
    const instrumentation: Instrumentation = {
      async wrapProduce (batch, run) {
        calls.push(`produce ${batch.topic} x${batch.records} ${batch.kind}${batch.origin === undefined ? '' : ` from ${batch.origin.topic}@${batch.origin.offset}`}`)
        return await run()
      },
      onProduce (record) {
        calls.push(`record ${record.topic} corr=${record.headers['x-correlation-id']}`)
        return { traceparent: '00-abc-def-01' }
      },
      async wrapHandler (message, context, run) {
        calls.push(`handle ${message.topic}@${message.offset} group=${context.groupId} attempt=${context.attempt} traceparent=${message.headers.traceparent}`)
        return await run()
      }
    }
    return { instrumentation, calls }
  }

  test('without instrumentation the pipeline runs as before and logs nothing', async () => {
    const h = harness()
    let handled = 0
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => { handled++ })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(handled, 1)
    assert.deepEqual(h.logs.filter((log) => log.level !== 'info'), [])
    await h.harbor.shutdown()
  })

  test('produce wraps the batch, stamps every record, and the consumer wraps the handler with the same headers', async () => {
    const { instrumentation, calls } = tracing()
    const h = harness({ instrumentation })
    const handled: MessageHeaders[] = []
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', (message) => { handled.push(message.headers) })
    await consumer.start()
    await h.harbor.producer().sendBatch('orders', [{ value: 1 }, { value: 2 }])
    await h.adapter.whenDrained('g', 'orders')
    assert.deepEqual(calls, [
      'produce orders x2 send',
      'record orders corr=corr-fixed',
      'record orders corr=corr-fixed',
      'handle orders@0 group=g attempt=1 traceparent=00-abc-def-01',
      'handle orders@1 group=g attempt=1 traceparent=00-abc-def-01'
    ])
    assert.equal(handled[0]?.traceparent, '00-abc-def-01')
    assert.equal(h.adapter.messages('orders')[0]?.headers.traceparent, '00-abc-def-01')
    await h.harbor.shutdown()
  })

  test('the trace context travels through the retry ladder, the DLQ and the redrive untouched; every hop is a batch with its origin', async () => {
    const { instrumentation, calls } = tracing()
    const h = harness({ instrumentation })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    consumer.subscribe('orders', () => { throw new Error('always') })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1, headers: { traceparent: '00-original-01' } })
    await until(() => h.adapter.messages('orders-dlq').length === 1)
    await h.adapter.whenDrained('g', 'orders-retry-1')
    await consumer.stop()
    await h.harbor.redrive({ from: 'orders-dlq', idleTimeout: 0 })
    // The header the record carries wins over the hook's, on the first send and on every hop.
    for (const topic of ['orders', 'orders-retry-1', 'orders-dlq']) assert.equal(h.adapter.messages(topic)[0]?.headers.traceparent, '00-original-01', topic)
    assert.equal(h.adapter.messages('orders')[1]?.headers.traceparent, '00-original-01', 'redriven')
    assert.deepEqual(calls.filter((call) => call.startsWith('produce')), [
      'produce orders x1 send',
      'produce orders-retry-1 x1 retry from orders@0',
      'produce orders-dlq x1 dead-letter from orders-retry-1@0',
      'produce orders x1 redrive from orders-dlq@0'
    ])
    await h.harbor.shutdown()
  })

  test('a header set by the application wins over the one the hook injects', async () => {
    const h = harness({ instrumentation: { onProduce: () => ({ traceparent: 'hook', 'x-correlation-id': 'hook' }) } })
    await h.harbor.producer().send('orders', { value: 1, headers: { traceparent: 'mine' } })
    const [record] = h.adapter.messages('orders')
    assert.equal(record?.headers.traceparent, 'mine')
    assert.equal(record?.headers['x-correlation-id'], 'corr-fixed', 'the automatic headers are the record\'s too')
    await h.harbor.shutdown()
  })

  test('hooks that throw are logged and change nothing: the message is produced, handled and committed', async () => {
    const h = harness({
      instrumentation: {
        wrapProduce: async () => { throw new Error('produce hook down') },
        onProduce: () => { throw new Error('inject down') },
        wrapHandler: async () => { throw new Error('handler hook down') }
      }
    })
    let handled = 0
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true })
    consumer.subscribe('orders', () => { handled++ })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await h.adapter.whenDrained('g', 'orders')
    assert.equal(handled, 1)
    assert.equal(h.adapter.committed('g', 'orders', 0), '1')
    const errors = h.logs.filter((log) => log.level === 'error').map((log) => log.message)
    assert.equal(errors.filter((line) => line.includes('produce hook down')).length, 1)
    assert.equal(errors.filter((line) => line.includes('handler hook down')).length, 1)
    // The per-record hook is part of the work, so it still ran (unwrapped) and its failure was contained too.
    assert.equal(errors.filter((line) => line.includes('inject down')).length, 1)
    assert.equal(h.adapter.messages('orders')[0]?.headers['x-producer'], 'test-app', 'the record left with its headers intact')
    await h.harbor.shutdown()
  })

  test('a handler failure inside a wrapper still follows the retry ladder', async () => {
    const { instrumentation } = tracing()
    const h = harness({ instrumentation })
    const consumer = h.harbor.consumer({ groupId: 'g', fromBeginning: true, autoCreateTopics: true, retry: { levels: [{ delay: 0 }] } })
    let attempts = 0
    consumer.subscribe('orders', () => { attempts++; if (attempts === 1) throw new Error('first fails') })
    await consumer.start()
    await h.harbor.producer().send('orders', { value: 1 })
    await until(() => attempts === 2)
    await h.adapter.whenDrained('g', 'orders-retry-1')
    assert.equal(h.adapter.messages('orders-dlq').length, 0)
    assert.equal(h.adapter.committed('g', 'orders-retry-1', 0), '1')
    await h.harbor.shutdown()
  })
})

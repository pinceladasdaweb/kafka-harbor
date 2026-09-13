/**
 * Decorators for classes that hold Kafka handlers. They keep their own
 * metadata (no `reflect-metadata`), work under TypeScript's standard
 * decorators as well as the legacy `experimentalDecorators` NestJS
 * applications use, and `bindListeners()` turns a decorated instance into a
 * consumer without any framework: this entry point has no dependency beyond
 * the core. `kafka-harbor/nestjs` discovers decorated providers in a NestJS
 * application.
 */
import {
  ConfigError,
  defaultDlqTopicNaming,
  type BatchHandler,
  type Consumer,
  type ConsumerOptions,
  type ConsumerRetryOptions,
  type Handler,
  type Harbor,
  type SubscribeBatchOptions,
  type SubscribeOptions
} from '../index'

// Registered symbols: this package ships dual CJS and ESM builds, and an
// application that loads both must have the decorator write under the key
// the binder reads. The legacy dialect records on prototypes under them;
// the standard dialect records per instance, in a side table, so a sealed
// or frozen instance is no obstacle.
const LISTENERS = Symbol.for('kafka-harbor:listeners')
const CONSUMER = Symbol.for('kafka-harbor:consumer')
const instanceListeners = new WeakMap<object, ListenerMetadata[]>()

/** One decorated method, as `bindListeners()` reads it. */
export type ListenerMetadata =
  | { readonly kind: 'each', readonly topic: string, readonly method: string | symbol, readonly options: SubscribeOptions }
  | { readonly kind: 'batch', readonly topic: string, readonly method: string | symbol, readonly options: SubscribeBatchOptions }
  | { readonly kind: 'dlq', readonly original: string, readonly method: string | symbol, readonly options: SubscribeOptions }

/** `Omit` applied to each member of a union rather than to the union as a whole. */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never

/** A listener entry before the decorated method's name is known. */
type Declared = DistributiveOmit<ListenerMetadata, 'method'>

// TypeScript's standard decorators receive (value, context); the legacy
// ones NestJS applications compile with receive (target, key, descriptor)
// for methods and (constructor) for classes. Both are told apart by the
// second argument.
interface DecoratorContext {
  readonly kind: string
  readonly name: string | symbol
  readonly static?: boolean
  readonly addInitializer: (initializer: (this: unknown) => void) => void
}

const isContext = (value: unknown): value is DecoratorContext =>
  typeof value === 'object' && value !== null && typeof (value as { kind?: unknown }).kind === 'string'

/** The value stored under `key` on `holder` itself, not inherited. */
const own = <T>(holder: object, key: symbol): T | undefined => Object.getOwnPropertyDescriptor(holder, key)?.value as T | undefined

/** The prototypes of an instance, nearest first, `Object.prototype` excluded. */
function * prototypesOf (instance: object): Generator<object> {
  for (let holder: object | null = Object.getPrototypeOf(instance); holder !== null && holder !== Object.prototype; holder = Object.getPrototypeOf(holder)) yield holder
}

const recordOnPrototype = (prototype: object, entry: ListenerMetadata): void => {
  const entries = own<ListenerMetadata[]>(prototype, LISTENERS)
  if (entries !== undefined) entries.push(entry)
  else Object.defineProperty(prototype, LISTENERS, { value: [entry], enumerable: false, configurable: true, writable: false })
}

/**
 * A method decorator in both dialects. Under standard decorators the entry
 * is recorded per instance as it is constructed (a parent's initializer
 * runs before a child's); under the legacy ones on the prototype.
 * `bindListeners()` reads both.
 */
const methodDecorator = (name: string, entry: Declared) =>
  (target: unknown, keyOrContext: string | symbol | DecoratorContext): void => {
    if (isContext(keyOrContext)) {
      if (keyOrContext.kind !== 'method') throw new ConfigError(`@${name}() decorates methods, not a ${keyOrContext.kind}`)
      if (keyOrContext.static === true) throw new ConfigError(`@${name}() decorates instance methods; ${String(keyOrContext.name)} is static`)
      const method = keyOrContext.name
      keyOrContext.addInitializer(function (this: unknown) {
        const instance = this as object
        const entries = instanceListeners.get(instance)
        if (entries !== undefined) entries.push({ ...entry, method } as ListenerMetadata)
        else instanceListeners.set(instance, [{ ...entry, method } as ListenerMetadata])
      })
      return
    }
    if (typeof target === 'function') throw new ConfigError(`@${name}() decorates instance methods; ${String(keyOrContext)} is static`)
    recordOnPrototype(target as object, { ...entry, method: keyOrContext } as ListenerMetadata)
  }

// Decorators apply bottom-up, so of two class decorators the upper one
// writes last and wins where they overlap.
const classDecorator = (name: string, options: Partial<ConsumerOptions>) =>
  (target: unknown, context?: unknown): void => {
    if (context !== undefined && isContext(context) && context.kind !== 'class') throw new ConfigError(`@${name}() decorates classes, not a ${context.kind}`)
    const prototype = (target as { prototype: object }).prototype
    const previous = own<Partial<ConsumerOptions>>(prototype, CONSUMER)
    Object.defineProperty(prototype, CONSUMER, { value: { ...previous, ...options }, enumerable: false, configurable: true, writable: false })
  }

/** The consumer the class's handlers run in: the group and any other `ConsumerOptions`. */
export function KafkaConsumer (options: ConsumerOptions): (target: unknown, context?: unknown) => void {
  return classDecorator('KafkaConsumer', options)
}

/** The retry ladder of the class's consumer, the same as `ConsumerOptions.retry`. */
export function KafkaRetry (retry: ConsumerRetryOptions): (target: unknown, context?: unknown) => void {
  return classDecorator('KafkaRetry', { retry })
}

/** The method handles every message of `topic`, as `consumer.subscribe()` would. */
export function KafkaListener<T = unknown> (topic: string, options: SubscribeOptions<T> = {}): (target: unknown, keyOrContext: string | symbol | DecoratorContext) => void {
  return methodDecorator('KafkaListener', { kind: 'each', topic, options: options as SubscribeOptions })
}

/** The method handles batches of `topic`, as `consumer.subscribeBatch()` would. */
export function KafkaBatchListener<T = unknown> (topic: string, options: SubscribeBatchOptions<T> = {}): (target: unknown, keyOrContext: string | symbol | DecoratorContext) => void {
  return methodDecorator('KafkaBatchListener', { kind: 'batch', topic, options: options as SubscribeBatchOptions })
}

/**
 * The method handles the dead letters of `originalTopic`: it subscribes to
 * that topic's DLQ, named by the consumer's `dlq.topicNaming`. A consumer
 * never consumes the DLQ it fills, so this belongs in a class of its own,
 * with its own group.
 */
export function DLQHandler<T = unknown> (originalTopic: string, options: SubscribeOptions<T> = {}): (target: unknown, keyOrContext: string | symbol | DecoratorContext) => void {
  return methodDecorator('DLQHandler', { kind: 'dlq', original: originalTopic, options: options as SubscribeOptions })
}

/**
 * Every listener declared for an instance, one per method: a method a
 * subclass overrides and decorates again is listed once, with the nearest
 * declaration, whichever dialect either class used.
 */
export function listenersOf (instance: object): ListenerMetadata[] {
  // Standard-dialect entries were recorded parent first; the last one for
  // a method is the nearest. Legacy entries follow, prototype by prototype.
  const nearestFirst = [...(instanceListeners.get(instance) ?? [])].reverse()
  for (const prototype of prototypesOf(instance)) nearestFirst.push(...(own<ListenerMetadata[]>(prototype, LISTENERS) ?? []))
  const byMethod = new Map<string | symbol, ListenerMetadata>()
  for (const entry of nearestFirst) if (!byMethod.has(entry.method)) byMethod.set(entry.method, entry)
  return [...byMethod.values()]
}

/** The `@KafkaConsumer()` / `@KafkaRetry()` options of an instance's class, the nearest class winning. */
export function consumerOptionsOf (instance: object): Partial<ConsumerOptions> {
  let options: Partial<ConsumerOptions> = {}
  for (const prototype of prototypesOf(instance)) options = { ...own<Partial<ConsumerOptions>>(prototype, CONSUMER), ...options }
  return options
}

/** Whether an object carries any decorated handler. */
export const hasListeners = (instance: unknown): instance is object =>
  typeof instance === 'object' && instance !== null && listenersOf(instance).length > 0

/**
 * Builds the consumer a decorated instance declares: one `subscribe()` or
 * `subscribeBatch()` per decorated method, calling it on the instance, with
 * the class's `@KafkaConsumer()` options under `overrides`. Not started: the
 * caller decides when. Rejects with a ConfigError when nothing is decorated
 * or no group id is known.
 */
export function bindListeners (harbor: Harbor, instance: object, overrides: Partial<ConsumerOptions> = {}): Consumer {
  const listeners = listenersOf(instance)
  const name = instance.constructor.name
  if (listeners.length === 0) throw new ConfigError(`${name} declares no @KafkaListener, @KafkaBatchListener or @DLQHandler method`)
  const options = { ...consumerOptionsOf(instance), ...overrides }
  if (options.groupId === undefined) throw new ConfigError(`${name} has no consumer group: decorate the class with @KafkaConsumer({ groupId }) or pass groupId to bindListeners()`)
  const consumer = harbor.consumer(options as ConsumerOptions)
  const dlqNaming = (options.dlq?.enabled ?? true) ? (options.dlq?.topicNaming ?? defaultDlqTopicNaming) : undefined
  for (const listener of listeners) {
    const method = (instance as Record<string | symbol, unknown>)[listener.method]
    if (typeof method !== 'function') throw new ConfigError(`${name}.${String(listener.method)} is decorated but is not a method`)
    if (listener.kind === 'batch') {
      consumer.subscribeBatch(listener.topic, method.bind(instance) as BatchHandler, listener.options)
      continue
    }
    if (listener.kind === 'dlq' && dlqNaming === undefined) {
      throw new ConfigError(`${name}.${String(listener.method)} handles the DLQ of "${listener.original}", but the consumer has the DLQ disabled`)
    }
    const topic = listener.kind === 'dlq' ? (dlqNaming as (topic: string) => string)(listener.original) : listener.topic
    consumer.subscribe(topic, method.bind(instance) as Handler, listener.options)
  }
  return consumer
}

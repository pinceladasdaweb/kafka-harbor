/**
 * The NestJS module for `kafka-harbor/decorators`: builds the harbor,
 * discovers the providers and controllers whose methods are decorated,
 * starts one consumer per class when the application bootstraps and shuts
 * the harbor down with it. Needs `@nestjs/common` and `@nestjs/core`
 * (optional peer dependencies of the package); the core never imports this
 * module.
 */
import { DiscoveryModule, DiscoveryService } from '@nestjs/core'
import { Module, type DynamicModule, type OnApplicationBootstrap, type OnApplicationShutdown, type OnModuleDestroy } from '@nestjs/common'

import { ConfigError, createHarbor, parseDuration, type Consumer, type Duration, type Harbor, type HarborConfig } from '../index'
import { bindListeners, hasListeners } from '../decorators/index'

/** Injection token of the `Harbor` the module builds. */
export const KAFKA_HARBOR = 'KAFKA_HARBOR'
/** Injection token of the module options. */
export const KAFKA_HARBOR_OPTIONS = 'KAFKA_HARBOR_OPTIONS'

/** What `forRoot()` takes: the harbor's configuration, and how long the application's shutdown waits for running handlers. */
export interface KafkaHarborModuleOptions extends HarborConfig {
  /**
   * How long the consumers wait for running handlers when the application
   * closes; the ones still running then are abandoned, uncommitted.
   * Default: 30s.
   */
  shutdownTimeout?: Duration
}

/** What `forRootAsync()` takes: the configuration built from other providers. */
export interface KafkaHarborModuleAsyncOptions {
  imports?: DynamicModule['imports']
  inject?: unknown[]
  useFactory: (...args: never[]) => KafkaHarborModuleOptions | Promise<KafkaHarborModuleOptions>
  /** Register the module globally. Default: true, matching forRoot. */
  global?: boolean
}

/**
 * Finds every provider and controller with decorated handlers once the
 * application has bootstrapped, binds and starts a consumer per class, and
 * shuts the harbor down with the application. A class whose instance Nest
 * does not keep (a request- or transient-scoped provider) cannot host a
 * consumer and is refused. If one consumer fails to start, the ones already
 * running are shut down with the harbor before the failure propagates, so a
 * failed bootstrap leaves nothing consuming. Constructed by the module
 * through a factory, so it needs no decorator of its own.
 *
 * On close the consumers stop first, in `onModuleDestroy`: the running
 * handlers finish and commit while what they use is still there. That is
 * before every module's `beforeApplicationShutdown` and
 * `onApplicationShutdown`, where most libraries close their clients, and
 * before the `onModuleDestroy` of every module that is not global; a global
 * module that closes a client in `onModuleDestroy` closes it first when it
 * is imported after this one. The client disconnects in
 * `onApplicationShutdown`. `shutdownTimeout` is one deadline for the whole
 * close, not one per hook. Only the consumers this explorer started drain
 * early; one created by hand on the injected harbor stops with the harbor.
 *
 * A scoped class is recognized by `@KafkaConsumer()` (or `@KafkaRetry()`)
 * on it or a class it extends, or by legacy method decorators; standard
 * method decorators record per instance, which a scoped class does not
 * have here.
 */
export class KafkaListenersExplorer implements OnApplicationBootstrap, OnModuleDestroy, OnApplicationShutdown {
  private readonly harbor: Harbor
  private readonly discovery: DiscoveryService
  private readonly timeoutMs: number
  /** When the close must be over: set as it begins, so the hooks share one timeout. */
  private closingBy = Infinity
  private readonly bound: Consumer[] = []

  constructor (harbor: Harbor, discovery: DiscoveryService, shutdownTimeout: Duration = '30s') {
    this.harbor = harbor
    this.discovery = discovery
    this.timeoutMs = parseDuration(shutdownTimeout, 'shutdownTimeout')
  }

  async onApplicationBootstrap (): Promise<void> {
    try {
      for (const wrapper of [...this.discovery.getProviders(), ...this.discovery.getControllers()]) {
        // A scoped provider has no instance of its own to bind (Nest keeps
        // a placeholder that shares the prototype, so the prototype's
        // legacy metadata would read as a listener): its class is what can
        // be checked, and a decorated one is a configuration error.
        if (!wrapper.isDependencyTreeStatic() || wrapper.isTransient) {
          if (isDecoratedClass(wrapper.metatype)) {
            throw new ConfigError(`${(wrapper.metatype as { name: string }).name} declares Kafka listeners but is not a singleton; a consumer needs one instance for the life of the application`)
          }
          continue
        }
        const instance: unknown = wrapper.instance
        if (!hasListeners(instance)) continue
        const consumer = bindListeners(this.harbor, instance)
        this.bound.push(consumer)
        await consumer.start()
      }
    } catch (error) {
      await this.harbor.shutdown(this.timeoutMs)
      throw error
    }
  }

  /**
   * Stops every consumer: no new message, running handlers finish and
   * commit. Each gets `shutdownTimeout`, the close's one deadline, whatever
   * its own `stopTimeout`. A handler that outlives it is abandoned here and
   * reported by the harbor's shutdown in `onApplicationShutdown`, so the
   * rest of the application still closes.
   */
  async onModuleDestroy (): Promise<void> {
    this.closingBy = this.now() + this.timeoutMs
    await Promise.allSettled(this.bound.map(async (consumer) => { await consumer.stop(this.timeoutMs) }))
  }

  /** What is left of the timeout goes to the sends and transactions in flight, then the client disconnects. */
  async onApplicationShutdown (): Promise<void> {
    await this.harbor.shutdown(Math.max(0, Math.min(this.timeoutMs, this.closingBy - this.now())))
  }

  private now (): number {
    return this.harbor.config.clock?.now() ?? Date.now()
  }

  /** The consumers bound so far, started in order. */
  get consumers (): readonly Consumer[] {
    return this.bound
  }
}

/**
 * Whether a class, or a class it extends, carries a decorator of this
 * package. A scoped provider's metatype is its class or, for a factory
 * provider, the factory, which may be an arrow function with no prototype.
 */
const isDecoratedClass = (metatype: unknown): boolean => {
  // Object() so a metatype Nest left null reads as one without a prototype.
  for (let prototype = (Object(metatype) as { prototype?: object | null }).prototype; prototype !== undefined && prototype !== null && prototype !== Object.prototype; prototype = Object.getPrototypeOf(prototype) as object | null) {
    if (Object.getOwnPropertySymbols(prototype).some((symbol) => symbol.description?.startsWith('kafka-harbor:') === true)) return true
  }
  return false
}

/** The harbor's configuration without the module's own options; what is not an object goes to `createHarbor` as it is, to be refused there. */
const harborConfigOf = (options: KafkaHarborModuleOptions): HarborConfig => {
  if (typeof options !== 'object' || options === null) return options
  const { shutdownTimeout, ...config } = options
  return config
}

type OptionsProvider =
  | { provide: string, useValue: KafkaHarborModuleOptions }
  | { provide: string, useFactory: KafkaHarborModuleAsyncOptions['useFactory'], inject: never[] }

/**
 * The NestJS module: `forRoot(config)` builds a `Harbor` from a
 * `HarborConfig` (available under `KAFKA_HARBOR`), discovers the providers
 * and controllers whose methods are decorated, starts their consumers when
 * the application bootstraps and shuts the harbor down with it. Registered
 * globally by default. One per application: the discovery covers every
 * module, so a second one would bind the same classes to a second harbor.
 */
export class KafkaHarborModule {
  /** The module with a configuration known up front. */
  static forRoot (options: KafkaHarborModuleOptions & { global?: boolean }): DynamicModule {
    const { global, ...config } = options
    return KafkaHarborModule.assemble({ provide: KAFKA_HARBOR_OPTIONS, useValue: config }, { global })
  }

  /** The module with a configuration built from other providers, which `imports` must make available. */
  static forRootAsync (options: KafkaHarborModuleAsyncOptions): DynamicModule {
    return KafkaHarborModule.assemble(
      { provide: KAFKA_HARBOR_OPTIONS, useFactory: options.useFactory, inject: (options.inject ?? []) as never[] },
      { global: options.global, imports: options.imports }
    )
  }

  private static assemble (optionsProvider: OptionsProvider, context: { global?: boolean, imports?: DynamicModule['imports'] }): DynamicModule {
    return {
      module: KafkaHarborModule,
      global: context.global ?? true,
      imports: [DiscoveryModule, ...(context.imports ?? [])],
      providers: [
        optionsProvider,
        { provide: KAFKA_HARBOR, useFactory: (options: KafkaHarborModuleOptions) => createHarbor(harborConfigOf(options)), inject: [KAFKA_HARBOR_OPTIONS] },
        {
          provide: KafkaListenersExplorer,
          useFactory: (harbor: Harbor, discovery: DiscoveryService, options: KafkaHarborModuleOptions) => new KafkaListenersExplorer(harbor, discovery, options.shutdownTimeout),
          inject: [KAFKA_HARBOR, DiscoveryService, KAFKA_HARBOR_OPTIONS]
        }
      ],
      exports: [KAFKA_HARBOR, KAFKA_HARBOR_OPTIONS, KafkaListenersExplorer]
    }
  }
}
// Module metadata as a plain call: the same as `@Module({})` on the class,
// without needing either decorator dialect in this file.
Module({})(KafkaHarborModule)

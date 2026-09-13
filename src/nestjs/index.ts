/**
 * The NestJS module for `kafka-harbor/decorators`: builds the harbor,
 * discovers the providers and controllers whose methods are decorated,
 * starts one consumer per class when the application bootstraps and shuts
 * the harbor down with it. Needs `@nestjs/common` and `@nestjs/core`
 * (optional peer dependencies of the package); the core never imports this
 * module.
 */
import { DiscoveryModule, DiscoveryService } from '@nestjs/core'
import { Module, type DynamicModule, type OnApplicationBootstrap, type OnApplicationShutdown } from '@nestjs/common'

import { ConfigError, createHarbor, type Consumer, type Harbor, type HarborConfig } from '../index'
import { bindListeners, hasListeners } from '../decorators/index'

/** Injection token of the `Harbor` the module builds. */
export const KAFKA_HARBOR = 'KAFKA_HARBOR'
/** Injection token of the module options. */
export const KAFKA_HARBOR_OPTIONS = 'KAFKA_HARBOR_OPTIONS'

/** What `forRoot()` takes: the harbor's configuration. */
export type KafkaHarborModuleOptions = HarborConfig

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
 */
export class KafkaListenersExplorer implements OnApplicationBootstrap, OnApplicationShutdown {
  private readonly harbor: Harbor
  private readonly discovery: DiscoveryService
  private readonly bound: Consumer[] = []

  constructor (harbor: Harbor, discovery: DiscoveryService) {
    this.harbor = harbor
    this.discovery = discovery
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
      await this.harbor.shutdown()
      throw error
    }
  }

  async onApplicationShutdown (): Promise<void> {
    await this.harbor.shutdown()
  }

  /** The consumers bound so far, started in order. */
  get consumers (): readonly Consumer[] {
    return this.bound
  }
}

const isDecoratedClass = (metatype: unknown): boolean =>
  typeof metatype === 'function' && Object.getOwnPropertySymbols((metatype as { prototype: object }).prototype ?? {}).some((symbol) => symbol.description?.startsWith('kafka-harbor:') === true)

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
        { provide: KAFKA_HARBOR, useFactory: (config: KafkaHarborModuleOptions) => createHarbor(config), inject: [KAFKA_HARBOR_OPTIONS] },
        { provide: KafkaListenersExplorer, useFactory: (harbor: Harbor, discovery: DiscoveryService) => new KafkaListenersExplorer(harbor, discovery), inject: [KAFKA_HARBOR, DiscoveryService] }
      ],
      exports: [KAFKA_HARBOR, KAFKA_HARBOR_OPTIONS, KafkaListenersExplorer]
    }
  }
}
// Module metadata as a plain call: the same as `@Module({})` on the class,
// without needing either decorator dialect in this file.
Module({})(KafkaHarborModule)

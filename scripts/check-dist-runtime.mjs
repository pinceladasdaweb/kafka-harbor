/**
 * Loads the shipped bundles the way an application does and checks what
 * only shows across entry points: kafka-harbor/nestjs finds the listeners
 * kafka-harbor/decorators recorded under the standard decorators, from the
 * ESM builds, from the CJS builds, and from one of each (an application
 * whose dependencies load the package both ways). Run after the build.
 */
import assert from 'node:assert/strict'
import { readFileSync, readdirSync } from 'node:fs'
import { createRequire } from 'node:module'

// Structure first: the NestJS bundle imports the decorators bundle, and the
// decorators' registry lives in that bundle alone. The runtime checks below
// pass with a copy as well (the registry is shared through the global
// object), so only this catches a bundle that inlines another entry point.
const dist = new URL('../dist/', import.meta.url)
assert.match(readFileSync(new URL('nestjs.mjs', dist), 'utf8'), /from '\.\/decorators\.mjs'/, 'nestjs.mjs imports the decorators bundle')
assert.match(readFileSync(new URL('nestjs.cjs', dist), 'utf8'), /require\('\.\/decorators\.cjs'\)/, 'nestjs.cjs requires the decorators bundle')
const bundles = readdirSync(dist, { recursive: true }).filter((file) => /\.(?:mjs|cjs)$/.test(file))
const holders = bundles.filter((file) => readFileSync(new URL(file, dist), 'utf8').includes('kafka-harbor:instance-listeners'))
assert.deepEqual(holders.sort(), ['decorators.cjs', 'decorators.mjs'], 'the registry is in the decorators bundles only')

const require = createRequire(import.meta.url)
const esm = {
  core: await import('../dist/index.mjs'),
  decorators: await import('../dist/decorators.mjs'),
  nestjs: await import('../dist/nestjs.mjs'),
  testing: await import('../dist/testing.mjs')
}
const cjs = {
  core: require('../dist/index.cjs'),
  decorators: require('../dist/decorators.cjs'),
  nestjs: require('../dist/nestjs.cjs'),
  testing: require('../dist/testing.cjs')
}

/** A decorated instance, decorated by hand the way TypeScript's standard decorators call them. */
const decorated = (decorators) => {
  class Orders {
    onOrder () {}
  }
  const initializers = []
  decorators.KafkaListener('orders')(Orders.prototype.onOrder, { kind: 'method', name: 'onOrder', static: false, addInitializer: (initializer) => { initializers.push(initializer) } })
  decorators.KafkaConsumer({ groupId: 'dist-check', autoCreateTopics: true })(Orders, { kind: 'class', name: 'Orders', addInitializer: () => {} })
  const instance = new Orders()
  for (const initializer of initializers) initializer.call(instance)
  return instance
}

const discovered = async (application, decorators, label) => {
  const harbor = application.core.createHarbor({ clientId: 'dist-check', brokers: ['memory:9092'], adapter: application.testing.memoryAdapter() })
  const instance = decorated(decorators)
  const provider = { instance, metatype: instance.constructor, isTransient: false, isDependencyTreeStatic: () => true }
  const explorer = new application.nestjs.KafkaListenersExplorer(harbor, { getProviders: () => [provider], getControllers: () => [] })
  await explorer.onApplicationBootstrap()
  assert.equal(explorer.consumers.length, 1, `${label}: kafka-harbor/nestjs finds the listener kafka-harbor/decorators recorded`)
  await harbor.shutdown()
}

await discovered(esm, esm.decorators, 'ESM')
await discovered(cjs, cjs.decorators, 'CJS')
await discovered(cjs, esm.decorators, 'decorators from ESM, nestjs from CJS')
console.log('dist runtime: the decorators and the NestJS module agree in ESM, in CJS and across both')

import { dts } from 'rollup-plugin-dts'
import typescript from '@rollup/plugin-typescript'

// A library build must never inline dependencies: everything that is not
// the library's own source (deps, node builtins) stays external.
const external = (id) => !id.startsWith('.') && !id.startsWith('/')

// A subpath entry that uses the core (the adapters throw the core error
// classes and build core Message objects; the testing entry returns a
// ClientAdapter the core drives) must import the shipped core bundle, never
// carry a private copy: instanceof checks on the error taxonomy have to
// hold across entry points at runtime, and the SAME rule holds for the
// declaration bundles - a class with private members is nominal in
// TypeScript, so an inlined `declare class Harbor` in a subpath's d.ts is a
// different type from the one in index.d.ts. Interfaces have no identity and
// may inline freely; classes may not.
//
// Subpath modules import the core through '../index' (one level deep) or
// '../../index' (two levels, e.g. src/adapters/confluent); both spellings
// resolve to src/index and are mapped to the shipped entry here.
//
// Ids reach these checks as written ('../index') or resolved to absolute
// paths, whose separators depend on the platform and whose directories may
// be called anything (a checkout under ~/src included): they are compared
// by their ending only.
const normalized = (id) => id.replaceAll('\\', '/')
const isCoreId = (id) => {
  const path = normalized(id)
  return /^(\.\.\/)+index$/.test(path) || path.endsWith('/src/index') || path.endsWith('/src/index.ts')
}

// One entry per public entry point: [source, name in dist/].
const ENTRIES = [
  ['src/index.ts', 'index'],
  ['src/adapters/confluent/index.ts', 'adapters/confluent'],
  ['src/adapters/platformatic/index.ts', 'adapters/platformatic'],
  ['src/testing/index.ts', 'testing'],
  ['src/prometheus/index.ts', 'prometheus'],
  ['src/otel/index.ts', 'otel'],
  ['src/schema-registry/index.ts', 'schema-registry'],
  ['src/decorators/index.ts', 'decorators'],
  ['src/nestjs/index.ts', 'nestjs']
]

// The same holds between subpaths: kafka-harbor/nestjs reads what
// kafka-harbor/decorators records, so it must import that bundle, not a copy
// of it (a copy has its own registry, and finds nothing the application's
// decorators wrote).
const SUBPATHS = ENTRIES.map(([, name]) => name).filter((name) => name !== 'index')
const siblingOf = (id) => {
  const path = normalized(id)
  return SUBPATHS.find((name) => path.endsWith(`../${name}/index`) || path.endsWith(`/src/${name}/index`) || path.endsWith(`/src/${name}/index.ts`))
}
const isShippedId = (id) => isCoreId(id) || siblingOf(id) !== undefined

// The shipped bundles live at dist/<name>.<ext>; a name with a slash
// (adapters/confluent) lands in a subdirectory, so the relative path back to
// the core entry depends on that depth.
const toCore = (name) => {
  const depth = name.split('/').length - 1
  return depth === 0 ? './' : '../'.repeat(depth)
}
const shipped = (id) => isCoreId(id) ? 'index' : siblingOf(id)
const corePaths = (name, format) => (id) =>
  isShippedId(id) ? `${toCore(name)}${shipped(id)}${format === 'es' ? '.mjs' : '.cjs'}` : id
// The declaration bundles point at the core declarations the way TypeScript
// resolves them: `index.js` is looked up as index.d.ts (the ESM entry) and
// `index.cjs` as index.d.cts, so each module kind lands on its own copy.
const coreTypePaths = (name, kind) => (id) =>
  isShippedId(id) ? `${toCore(name)}${shipped(id)}${kind === 'cts' ? '.cjs' : '.js'}` : id

// One pair of configs per public entry point. Each subpath bundles its own
// tree; `core: true` is the exception above: the code bundle then imports
// the core entry instead of duplicating it.
const entry = (input, name, { core = false } = {}) => [
  {
    input,
    output: [
      { file: `dist/${name}.cjs`, format: 'cjs', exports: 'named', ...(core && { paths: corePaths(name, 'cjs') }) },
      { file: `dist/${name}.mjs`, format: 'es', exports: 'named', ...(core && { paths: corePaths(name, 'es') }) }
    ],
    plugins: [typescript({ include: ['src/**/*.ts'] })],
    external: core ? (id) => external(id) || isShippedId(id) : external
  },
  {
    input,
    // The two declaration files differ only in how they name the core
    // declarations they import (see coreTypePaths); emitting both here keeps
    // the build script a plain `rollup -c` however many entry points exist.
    output: [
      { file: `dist/${name}.d.ts`, format: 'es', ...(core && { paths: coreTypePaths(name, 'ts') }) },
      { file: `dist/${name}.d.cts`, format: 'es', ...(core && { paths: coreTypePaths(name, 'cts') }) }
    ],
    // Members tagged @internal (the pipelines' constructors) are the core's, not the API.
    plugins: [dts({ compilerOptions: { stripInternal: true } })],
    external: core ? (id) => external(id) || isShippedId(id) : external
  }
]

export default ENTRIES.flatMap(([input, name]) => entry(input, name, { core: name !== 'index' }))

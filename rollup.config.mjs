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
const isCoreId = (id) => /^(\.\.\/)+index$/.test(id) || id.endsWith('/src/index')

// The shipped bundles live at dist/<name>.<ext>; a name with a slash
// (adapters/confluent) lands in a subdirectory, so the relative path back to
// the core entry depends on that depth.
const toCore = (name) => {
  const depth = name.split('/').length - 1
  return depth === 0 ? './' : '../'.repeat(depth)
}
const corePaths = (name, format) => (id) =>
  isCoreId(id) ? `${toCore(name)}${format === 'es' ? 'index.mjs' : 'index.cjs'}` : id
// The declaration bundles point at the core declarations the way TypeScript
// resolves them: `index.js` is looked up as index.d.ts (the ESM entry) and
// `index.cjs` as index.d.cts, so each module kind lands on its own copy.
const coreTypePaths = (name, kind) => (id) =>
  isCoreId(id) ? `${toCore(name)}${kind === 'cts' ? 'index.cjs' : 'index.js'}` : id

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
    external: core ? (id) => external(id) || isCoreId(id) : external
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
    plugins: [dts()],
    external: core ? (id) => external(id) || isCoreId(id) : external
  }
]

export default [
  ...entry('src/index.ts', 'index'),
  ...entry('src/adapters/confluent/index.ts', 'adapters/confluent', { core: true }),
  ...entry('src/testing/index.ts', 'testing', { core: true })
]

#!/usr/bin/env node
// Runs the mutation gate on the source files this branch changed, and only
// those. A full run over src/ takes long enough to be skipped "just this
// once"; a run scoped to the diff takes a few minutes and is what the
// delivery ritual asks for on every change.
//
// The base is the merge base with origin/development (or the argument given
// as --base=<ref>); uncommitted changes count as well, so the gate can run
// before the commit that carries them.
import { spawnSync } from 'node:child_process'
import { execFileSync } from 'node:child_process'
import { existsSync, readFileSync } from 'node:fs'

const git = (...args) => execFileSync('git', args, { encoding: 'utf8' }).trim()

const baseArg = process.argv.find((arg) => arg.startsWith('--base='))
const base = baseArg ? baseArg.slice('--base='.length) : resolveBase()

function resolveBase () {
  for (const candidate of ['origin/development', 'development', 'origin/main', 'main']) {
    try {
      return git('merge-base', 'HEAD', candidate)
    } catch {}
  }
  // A fresh repository with a single commit has nothing to diff against.
  return git('rev-list', '--max-parents=0', 'HEAD')
}

const committed = git('diff', '--name-only', base, 'HEAD').split('\n')
const unstaged = git('diff', '--name-only').split('\n')
const staged = git('diff', '--name-only', '--cached').split('\n')
const untracked = git('ls-files', '--others', '--exclude-standard').split('\n')

// Mirrors the "mutate" patterns in stryker.config.json: the entry barrel has
// no logic to mutate, and the Confluent adapter is only exercised by the
// integration suite, which the hermetic mutation run cannot reach.
const config = JSON.parse(readFileSync(new URL('../stryker.config.json', import.meta.url), 'utf8'))
const excluded = config.mutate
  .filter((pattern) => pattern.startsWith('!'))
  .map((pattern) => pattern.slice(1).replace(/\/\*\*$/, '/'))

const files = [...new Set([...committed, ...unstaged, ...staged, ...untracked])]
  .filter((file) => file.startsWith('src/') && file.endsWith('.ts'))
  .filter((file) => !excluded.some((prefix) => file === prefix || file.startsWith(prefix)))
  .filter((file) => existsSync(file))

if (files.length === 0) {
  console.log(`mutation: no mutable source changed since ${base.slice(0, 12)}; nothing to run`)
  process.exit(0)
}

console.log(`mutation: ${files.length} changed file(s) since ${base.slice(0, 12)}:\n  ${files.join('\n  ')}`)

const result = spawnSync('npx', ['stryker', 'run', '--mutate', files.join(',')], {
  stdio: 'inherit',
  shell: process.platform === 'win32'
})
process.exit(result.status ?? 1)

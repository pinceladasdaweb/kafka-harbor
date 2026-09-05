#!/usr/bin/env node
// Type-checks every ```ts block in the public documentation against src/,
// and checks that every in-page link in those documents points at a heading
// that exists. Documentation that does not compile is documentation that
// drifted from the code; this makes the drift a build failure.
//
// Each block becomes one module under temp/doc-snippets/, with the package
// names (kafka-harbor, kafka-harbor/testing, ...) mapped to the sources.
// Identifiers the prose leaves undefined on purpose (Order, fulfill, harbor,
// ...) are declared once in scripts/doc-snippets.globals.d.ts.
import { execFileSync } from 'node:child_process'
import { mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs'
import { dirname, join, relative } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = dirname(dirname(fileURLToPath(import.meta.url)))
const documents = ['README.md', 'CONTRIBUTING.md', ...readdirSync(join(root, 'docs')).filter((f) => f.endsWith('.md')).map((f) => `docs/${f}`)]
const outDir = join(root, 'temp', 'doc-snippets')
rmSync(outDir, { recursive: true, force: true })
mkdirSync(outDir, { recursive: true })

const slug = (heading) => heading.toLowerCase().replace(/[`*_]/g, '').replace(/[^a-z0-9 -]/g, '').trim().replace(/\s+/g, '-')

let snippets = 0
let problems = 0
const origins = new Map()

for (const document of documents) {
  const text = readFileSync(join(root, document), 'utf8')
  const lines = text.split('\n')

  // Anchors: every [text](#anchor) must match a heading in the same document.
  const headings = new Set(lines.filter((line) => /^#{1,6}\s/.test(line)).map((line) => slug(line.replace(/^#+\s*/, ''))))
  for (const [index, line] of lines.entries()) {
    for (const match of line.matchAll(/\]\(#([^)]+)\)/g)) {
      if (!headings.has(match[1])) {
        problems++
        console.error(`${document}:${index + 1}: link to #${match[1]} has no matching heading`)
      }
    }
  }

  // Snippets: ```ts ... ``` blocks. ```typescript is accepted too.
  let index = 0
  let block = null
  for (const [lineNumber, line] of lines.entries()) {
    if (block === null) {
      if (/^```(ts|typescript)\s*$/.test(line)) block = { start: lineNumber + 2, body: [] }
      continue
    }
    if (/^```\s*$/.test(line)) {
      const name = `${document.replace(/[^a-z0-9]+/gi, '-')}-${index++}.ts`
      // `export {}` last, so the snippet's lines keep their numbers.
      writeFileSync(join(outDir, name), `${block.body.join('\n')}\nexport {}\n`)
      origins.set(name, { document, start: block.start })
      snippets++
      block = null
      continue
    }
    block.body.push(line)
  }
}

writeFileSync(join(outDir, 'tsconfig.json'), JSON.stringify({
  compilerOptions: {
    target: 'ES2022',
    module: 'ESNext',
    moduleResolution: 'Bundler',
    lib: ['ES2024'],
    types: ['node'],
    typeRoots: [join(root, 'node_modules', '@types')],
    strict: true,
    noEmit: true,
    skipLibCheck: true,
    // paths resolve relative to this tsconfig (temp/doc-snippets/).
    paths: {
      'kafka-harbor': ['../../src/index.ts'],
      'kafka-harbor/*': ['../../src/*/index.ts']
    }
  },
  files: [join(root, 'scripts', 'doc-snippets.globals.d.ts'), ...[...origins.keys()].map((name) => join(outDir, name))]
}, null, 2))

let output = ''
try {
  execFileSync(join(root, 'node_modules', '.bin', 'tsc'), ['-p', join(outDir, 'tsconfig.json'), '--pretty', 'false'], { cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] })
} catch (error) {
  output = `${error.stdout ?? ''}${error.stderr ?? ''}`
}

// tsc reports temp/doc-snippets/<name>(line,col); point at the document instead.
for (const line of output.split('\n').filter((entry) => entry.trim() !== '')) {
  const match = /^(.*?)\((\d+),(\d+)\): (.*)$/.exec(line)
  if (match === null) {
    problems++
    console.error(line)
    continue
  }
  const name = relative(outDir, join(root, match[1]))
  const origin = origins.get(name)
  problems++
  if (origin === undefined) {
    console.error(line)
  } else {
    console.error(`${origin.document}:${origin.start + Number(match[2]) - 1}:${match[3]}: ${match[4]}`)
  }
}

if (problems > 0) {
  console.error(`\ndocs: ${problems} problem(s) across ${snippets} snippet(s) in ${documents.length} document(s)`)
  process.exit(1)
}
console.log(`docs: ${snippets} snippet(s) in ${documents.length} document(s) type-check against src/, all anchors resolve`)

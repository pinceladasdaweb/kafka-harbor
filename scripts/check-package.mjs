/**
 * The package as a consumer installs it: packed by npm (the `files` list,
 * the `exports` map), unpacked as a dependency of two scratch projects under
 * temp/, one an ES module and one CommonJS, and compiled in each with the
 * resolution Node itself uses (`nodenext`) and every declaration checked
 * (`skipLibCheck: false`; an error in another dependency's declarations is
 * not this package's), then loaded at runtime: every entry point
 * imported, and required, and the package.json read through the map. The
 * peer dependencies resolve from the repository's own node_modules, a few
 * levels up. Run after the build.
 */
import { execFileSync } from 'node:child_process'
import { cpSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('..', import.meta.url))
const work = join(root, 'temp', 'package-check')
rmSync(work, { recursive: true, force: true })
mkdirSync(join(work, 'node_modules', 'kafka-harbor'), { recursive: true })

const [{ filename }] = JSON.parse(execFileSync('npm', ['pack', '--json', '--ignore-scripts', '--pack-destination', work], { cwd: root, encoding: 'utf8' }))
execFileSync('tar', ['-xzf', join(work, filename), '-C', join(work, 'node_modules', 'kafka-harbor'), '--strip-components=1'])
rmSync(join(work, filename))

const manifest = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'))
const entryPoints = Object.keys(manifest.exports)
  .filter((key) => key !== './package.json')
  .map((key) => key === '.' ? 'kafka-harbor' : `kafka-harbor/${key.slice(2)}`)

const tsc = join(root, 'node_modules', 'typescript', 'bin', 'tsc')
for (const type of ['module', 'commonjs']) {
  const project = join(work, type)
  mkdirSync(project)
  writeFileSync(join(project, 'package.json'), JSON.stringify({ name: `package-check-${type}`, private: true, type }))
  cpSync(join(root, 'tests', 'consumer', 'consumer.ts'), join(project, 'consumer.ts'))
  writeFileSync(join(project, 'tsconfig.json'), JSON.stringify({
    compilerOptions: { strict: true, noEmit: true, module: 'nodenext', moduleResolution: 'nodenext', target: 'es2022', skipLibCheck: false, types: ['node'] },
    files: ['consumer.ts']
  }))
  // Every declaration is checked, but only an error in this package (or in
  // the file that uses it) is this package's to fix: other dependencies ship
  // declarations with errors of their own under these settings.
  let output = ''
  try {
    execFileSync(process.execPath, [tsc, '-p', join(project, 'tsconfig.json'), '--pretty', 'false'], { cwd: root, encoding: 'utf8' })
  } catch (failure) {
    // 2 is tsc reporting errors; anything else (a crash, a spawn failure) is a failure of the check itself.
    if (failure.status !== 2 || typeof failure.stdout !== 'string') throw failure
    output = failure.stdout
  }
  // A diagnostic with no file (a missing type library, a bad option) starts the line and counts too.
  const ours = output.split('\n').filter((line) => /(^| )error TS\d+:/.test(line) && (/^error TS/.test(line) || !line.includes('node_modules') || line.includes('node_modules/kafka-harbor/')))
  if (ours.length > 0) {
    console.error(ours.join('\n'))
    throw new Error(`${ours.length} type error(s) in kafka-harbor as a ${type} project sees it`)
  }
}

const imports = [...entryPoints.map((entry) => `await import(${JSON.stringify(entry)})`), 'await import("kafka-harbor/package.json", { with: { type: "json" } })'].join('\n')
execFileSync(process.execPath, ['--input-type=module', '-e', imports], { cwd: join(work, 'module'), stdio: 'inherit' })
const requires = [...entryPoints.map((entry) => `require(${JSON.stringify(entry)})`), 'require("kafka-harbor/package.json")'].join('\n')
execFileSync(process.execPath, ['-e', requires], { cwd: join(work, 'commonjs'), stdio: 'inherit' })

rmSync(work, { recursive: true, force: true })
console.log(`package: ${entryPoints.length} entry points compile under nodenext as ESM and CJS with every declaration checked, and load both ways`)

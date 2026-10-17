/**
 * The API reports in etc/: one per public entry point, the root's at
 * etc/kafka-harbor.api.md and each subpath's next to it
 * (etc/kafka-harbor.<subpath>.api.md). `update` rewrites them; `check`
 * fails when the built declarations drifted from the committed reports.
 * Run after the build.
 */
import { Extractor, ExtractorConfig } from '@microsoft/api-extractor'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { join } from 'node:path'

const mode = process.argv[2]
if (mode !== 'update' && mode !== 'check') throw new Error('usage: node scripts/api-report.mjs update|check')
const root = fileURLToPath(new URL('..', import.meta.url))
// API Extractor resolves the compiler's globals (Buffer, NodeJS) from the working directory.
process.chdir(root)
const manifest = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'))
const base = ExtractorConfig.loadFile(join(root, 'api-extractor.json'))

let failed = false
for (const key of Object.keys(manifest.exports).filter((entry) => entry !== './package.json')) {
  const subpath = key === '.' ? undefined : key.slice(2)
  const config = ExtractorConfig.prepare({
    configObject: {
      ...base,
      mainEntryPointFilePath: join(root, 'dist', `${subpath ?? 'index'}.d.ts`),
      apiReport: { ...base.apiReport, reportFileName: subpath === undefined ? '<unscopedPackageName>.api.md' : `<unscopedPackageName>.${subpath.replaceAll('/', '-')}.api.md` }
    },
    configObjectFullPath: join(root, 'api-extractor.json'),
    packageJsonFullPath: join(root, 'package.json')
  })
  const result = Extractor.invoke(config, { localBuild: mode === 'update', showVerboseMessages: false })
  if (!result.succeeded) {
    failed = true
    console.error(`API report of ${subpath ?? 'the root'}: ${result.apiReportChanged ? 'changed; run npm run api:update and commit it' : `${result.errorCount} error(s), ${result.warningCount} warning(s)`}`)
  }
}
if (failed) process.exit(1)
console.log('api: every entry point matches its report')

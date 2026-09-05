# Contributing

Thanks for taking the time. This library guards a few invariants that are
easy to break by accident, so this document is mostly about those, not about
formatting, which the tooling handles for you.

## Getting set up

```bash
npm install
npm run hooks   # once per clone: enables lint and commit-message hooks
```

Node.js >= 22 is required. The integration suite manages its own Kafka
through Testcontainers, so the only external requirement is a running Docker.
`@confluentinc/kafka-javascript` has prebuilt binaries for Node 22 and 24; on
Node 26 `npm install` compiles librdkafka from source (a few minutes, needs a
C++ toolchain). `npm install --ignore-scripts` skips that when you only need
the unit suite.

## The checks

```bash
npm test                      # unit tests + adapter contract on the memory adapter
npm run test:coverage         # the same, with a coverage report
npm run test:integration      # adapter contract on a real Kafka + the retry/DLQ flow (Docker)
npm run test:mutation:changed # the mutation gate on the files this branch changed
npm run lint                  # neostandard + the project rules
npm run check:types           # TypeScript 6
npm run check:types:next      # the same surface under TypeScript 7
npm run check:dist            # build, then compile tests/consumer against dist/
npm run api:check             # build + public API report comparison
npm run examples              # the handler-signature example asserts its own outcome
```

CI runs all of it on Node 22, 24 and 26; the integration job runs on Node 22
with `KAFKA_REQUIRED=1`, so a broker that fails to start fails the build
instead of skipping the suite.

## Invariants worth knowing before you change code

**No offset is committed before the work it stands for is safe.** A
successful handler commits. A failed handler commits only after the retry or
DLQ produce was acknowledged by the broker. A produce that is not
acknowledged stops the consumer with the offset uncommitted. Never reorder
those two lines.

**Every failure has an explicit destination.** Retry topic, DLQ, or a stop of
the consumer with an `error` event. If you find yourself writing a branch
that logs and moves on, the design is being violated.

**Headers are network input.** `x-retry-count`, `x-first-failure-at` and
`x-original-topic` are validated as strings before any coercion: `Number('')`
is `0`, and `0` is finite. A corrupt block is discarded whole, never partly
believed. A corrupt advisory field (`x-last-error`) is dropped, the block
survives.

**Events report outcomes, not attempts.** `messageDeadLettered` fires after
the DLQ produce was acknowledged. `messageRetried` after the retry produce.
Counting attempts inflates the rescue metric during the exact incident it is
supposed to measure.

**The adapter is dumb on purpose.** Retry, DLQ, serialization, offset policy
and shutdown live in the core. If a feature needs the adapter to know about
any of them, the feature is in the wrong place. `ClientAdapter` is
implemented by users: adding a required member is a **major**. New
capabilities are optional members, and the core works when they are absent.

**Errors carry a stable `code`; message text is not contract.** Never branch
on a message and never assert one in a test. Guards check `code`, not the
prototype chain: ESM and CJS copies of this package can coexist in one
process.

**Serialization is never magic.** A value JSON would flatten or drop
(`Map`, `Set`, typed arrays, `undefined` in an object, `NaN`) raises
`SerializationError` before a byte is produced. `Date` is the only conversion
accepted, and it is documented.

**Zero is a legitimate value.** Use `??`, never `||`, for defaults.

**One clock sample per operation.** Time comes from the injected `Clock`,
sampled once; two `Date.now()` calls in the same statement are two truths.
Tests use `ManualClock` and never race wall time.

**Timers a caller awaits are never `unref()`'d.** A retry wait or a shutdown
deadline keeps the process alive on purpose.

**The core knows nothing about clients.** Top-level `src/` modules import
only their siblings; the linter enforces it. Subpath entry points import the
core through `'../index'`, never deep paths, so the build externalizes the
core bundle and `instanceof` holds across entry points.

**The public API is frozen by the report in `etc/`.** `npm run api:check`
fails when exports drift; if the change is deliberate, run `npm run
api:update` and commit the report. The diff is part of the review.

## Tests

Every change needs a test that fails without it. Beyond that:

- **New adapter?** Run `runAdapterContract` from
  `tests/contract/adapter-contract.ts` against the real backend via
  Testcontainers. The invariants are numbered in the file header; add to the
  list rather than around it.
- **Fixed a bug?** Name the test after the behavior and mark it with a
  `// Regression:` comment explaining what used to happen.
- **Observability changes** are asserted through the typed events, not
  through log output.
- **Mutation gate.** `npm run test:mutation:changed` (Stryker) grades whether
  the tests assert what their names promise, on the files the branch
  touched. The Confluent adapter is excluded (only a broker can exercise it)
  and answers to the integration contract suite instead. A surviving mutant
  is either a missing assertion or an equivalent mutant; equivalent ones are
  removed by simplifying the code, never with a `// Stryker disable`.

## Commits and branches

Commit messages follow Conventional Commits and are checked by commitlint on
commit. Branch names start with a type prefix (`feat/`, `fix/`, `chore/`,
...) and are checked on push. Identifiers and comments are English-only and
`enum` is banned; the linter enforces both.

`package-lock.json` is versioned and CI installs with `npm ci`, so a build
reproduces from the lockfile. Dependabot keeps it moving.

Open pull requests against `development`, not `main`.

## How a release happens

Merging to `main` publishes. The workflow reads the version already on the
registry, bumps it from the merge commit message, publishes to npm through
OIDC trusted publishing (no token secret, provenance attached), then writes
`CHANGELOG.md`, tags `vX.Y.Z` and opens the GitHub release.

The bump follows the commit subject: `BREAKING CHANGE` or `major` for a
major, `minor` for a minor, anything else for a patch. Commit subjects are
the release notes, so write them for the person reading them later.

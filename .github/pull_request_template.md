## What changed

<!-- What behavior is different after this, and why. -->

## How it was verified

<!-- Which checks you ran locally. CI runs lint, both type checks, unit and
     contract tests, the consumer type check and the API report on Node 22,
     24 and 26, plus the integration suite against a Kafka started by
     Testcontainers. -->

- [ ] `npm test` (and `npm run test:integration` if the behavior touches the broker path; needs Docker)
- [ ] `npm run check:types` and `npm run check:types:next`
- [ ] `npm run api:check`, or `npm run api:update` with the report committed when the public API moved on purpose
- [ ] `npm run check:docs` when README or docs/ changed
- [ ] `npm run test:mutation:changed`, surviving mutants explained below, if any

## Checklist

- [ ] A test fails without this change
- [ ] New client adapter? It passes `runAdapterContract` from `kafka-harbor/testing` against a real broker via Testcontainers
- [ ] Errors carry a stable `code`; no test asserts message text
- [ ] No offset is committed before the retry/DLQ produce is acknowledged by the broker
- [ ] Headers read from the wire are validated before use (blank or garbage never becomes `0` or `NaN`)
- [ ] Defaults use `??` so `0` survives
- [ ] Nothing client-specific leaks into the core
- [ ] README and docs updated if the public surface moved

# A4.1 dispatch UI acceptance

Base: main `d400757b357e63511518a77e1d67620f52b23b6c`, including #93/#108/#107.
Implementation: backend commit `d5c099c` plus the frontend source hashes in
`source-sha256.json`. This evidence is goal-scoped; it does not replace the
official 18-report course gate under #105.

## Red-first evidence

- `red-browser.txt`: the old embedded placeholder had no dispatch heading.
- `red-events.txt`: the heartbeat-only SSE stub never requested a refresh on connection.
- `red-routing.txt`: an initial UI subscription on a primary workflow exchange
  masked an absent durable work queue. The implementation uses a separate
  disposable projection exchange/connection, offered only after the primary
  routed positive confirm. The same real-broker regression now passes.

Browser OS-library setup failures were resolved before collecting the behavior
failure. They are not counted as a red-first feature test.

## Final branch checks

| Command / observation | Result | Evidence |
| --- | --- | --- |
| `make check` | PASS: lint 0 issues; default and chaos race tests pass | `check.txt` |
| `make gen-check` | PASS: no generated contract diff | `gen.txt` |
| `make verify-A4.1` | PASS: 4 model, 9 Chromium, 3 live-broker regressions; Go regressions | `verify.txt`, `browser-results.json` |
| `go test -race -count=3 ./ui ./internal/api ./cmd/api ./internal/mq` | PASS | `race-repeat.txt` |
| App images built and loaded into local five-node kind | PASS: nonroot, broker PVC preserved, three replicas on distinct data workers per shard | image JSONs, image IDs, `broker-pvc.txt`, `placement.log` |
| Production API/outbox/relay/RabbitMQ/API-SSE/browser chain | PASS | `kind-results.json`, `kind-probe.mjs` |
| Real reciprocal assignment displayed; network off reload; cancel completes while offline; reconnect shows CANCELLED | PASS | `kind-results.json`, `kind-desktop.png` |

The nine deterministic browser cases run against the actual Go UI server,
embedded production build and service worker, with a contract-shaped HTTP/SSE
fixture. They cover real EventSource duplicate IDs, misleading event payloads,
partial reads, offline records/shell, mobile overflow, cancellation cleanup, and
unknown write outcomes. `browser-desktop.png` and `browser-mobile.png` use that
small fixture. `kind-desktop.png` uses real deployed records and broker counts.

For the additional production kind probe, deploy the branch with
`APP_IMAGE_TAG=<unique-tag> make app-deploy` on the local standalone-storage kind
profile. With the pinned UI dependencies installed, run:

```sh
node docs/evidence/A4.1/kind-probe.mjs
```

The optional `A41_KIND_ORIGIN` must be a loopback origin; default is port 8080.
The probe creates synthetic MEDICINE records in the local development cluster.
Its early authoring corrections used the actual `activeClaim` response shape,
scoped the request-card selector, and used the contract's `/status` route. No
production code or workflow assertion was weakened to obtain the final pass.

## Pre-PR review

Verdict: **READY for owner review**.

1. Workflow invariants: primary durable routing, confirms, operation IDs and
   handoffs are unchanged. Temporary UI consumers never consume work queues.
2. Idempotency: bounded browser event-ID dedupe; reconnect/missed-hint recovery
   rereads authoritative state. Events and cache never drive terminal transitions.
3. Concurrency: projection connections have bounded setup/call lifetimes; API
   root cancellation closes active SSE and joins the worker. Slow clients are
   disconnected. Three repeated affected-package race runs pass.
4. Errors: partial reads retain a stale view and disable writes; unknown write
   outcomes remain unconfirmed even after a successful unchanged reread. Failed
   telemetry is unknown, never fabricated as zero.
5. Tests: behavior failures retained; real broker safety negative and complete
   browser cases pass. Frozen contracts are unchanged. No assertion was loosened.
6. Scope: A-owned app/UI/MQ code and shared documentation only. S-owned workflow
   CI changes are requested in #111; the separate tagged schedule CI gap is #110.
7. Hygiene: pinned dependencies, local-only runtime assets with licenses,
   source/output hash guards, clean generation and whitespace, human-authored
   commits with no attribution trailers. No local progress/plan files staged.

Limitations: local map is a coordinate schematic without online tiles. Lists are
explicitly limited to 100 per entity type; paging is A4.4. History is observed,
not an audit journal. SSE is a best-effort hint stream; default local deployment
has no STREAM_TOKEN, and protected SSE requires a same-origin authenticated
proxy that injects the credential server-side. Shard availability reflects the
last complete reads, not pod readiness. Queue counts cover matching requests and
dead letters, not the full A4.2 metric suite. Native offline support requires a
secure context (localhost works). Browser CI wiring remains with S in #111.

## Integration after #100 merged

Rebased onto main dd1ea8e (merged #100). Full make check, make gen-check and
make verify-A4.1 pass again; see after-100-*.txt. Goal source hashes are unchanged.
The earlier live-kind run remains the recorded run on the stated image IDs.

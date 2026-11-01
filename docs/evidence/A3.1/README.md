# A3.1 — application demos

## Behaviour and acceptance

Demos 2, 3, 5, 8 and 9 exercise the real public API through Traefik, Kubernetes
workloads and the pinned RabbitMQ broker. Recorded seed/frozen inputs and every
HTTP outcome are saved per demo. Unknown create outcomes reuse identical keys and
payloads. Strong current claims prove reciprocal ownership; incomplete/truncated
scans fail closed, and absent broker counters remain unknown.

Demo 2 covers resource validation, listings, updates, exact managed-status 409
refusals with unchanged stored rows, and original create replay. Demo 3 covers
async/explicit matching, incompatible resources and competition. Demo 5 submits
500 compatible requests against 50 resources with the fixed 120-second settling
budget. It pauses admission, finishes committed handoffs through the production
routed-confirm relay, audits, then restores workers. No queue purge or premature
acknowledgement is used; queued hints may remain durable after the final drain.
Demo 8 records pod replacement UIDs, progress, retry/replay and interruption counts.
Demo 9 scales API 2 -> 6 -> 2 without HPA and observes all six pods via the Service.
App eviction tolerations are 30 seconds, separate from measured recovery time.

## Actual verification

Two clean deployments passed all five demos: 10 green reports and 8 strict
invcheck audits. Both bursts reached 50 MATCHED / 450 WAITING with no duplicate
resource and current reciprocal claims. Both scale runs observed all six pods.
A separate controlled check seeds one event with workers stopped and verifies
Read=Published=Marked=1, empty strong outbox pages and a clean invcheck; see
drain-control/. This covers the non-empty drain path directly.
See ../A3.1-20261101-resume1/round-1/ and round-2/, live-two-rounds.log and
live-summary.json for results, seeds, timings, pod identities and exact code hashes.
Verified source: 19ad0cbb58d2435781998f6ed98af71e133250d5; base main: e2d845f8ddab045abe60b8bc3a35025f63d4f9da.
The final evidence commit changes no executable source.

`make check` passed (both lint runs: 0 issues, ordinary/chaos race suites).
`make verify-A3.1-static`, `make gen-check` and the focused integration-tagged
`go test -race -count=3` run passed. Exact output is retained in final-*.log.
Initial failing tests catch the five stubs, missing serving-pod metadata,
missing eviction tolerations and an unrouted-topology preflight before reset.
The client tests exercise lost responses, strict scans, reciprocity, unknown
broker counts and restoration after audit failure.

## Failed runs and corrections

Earlier evidence is retained without relabelling it as success. Attempt 1 used an
outdated 400 expectation; the authoritative API requires an exact 409 for managed
statuses, and the corrected demo additionally checks the entire stored row/version.
It also exposed a typed-nil publisher startup failure, fixed separately in #65.
Attempt 2 correctly failed unpublished resource.state_changed events; merged #81
adds their missing exact durable binding. Attempt 3 predates merged #74 and stalled
on illegal WAITING -> WAITING. Attempt 4 reached 50/450, then caught the final
handoff race when stopping matcher also stopped its embedded relay. The new
bounded relay-only helper resolves that race after admission stops, while the
strict zero-backlog assertion stays intact. Its test-first red/green and failed
live output are in c13-handoff-drain/. No settling budget or invariant was weakened.

## Pre-PR review

Verdict: READY for ownership review.

1. Invariants: current strong reads/complete scans and strict invcheck; no intent
   clearing, historical ownership proof or workflow/consumer acknowledgement changes.
2. Idempotency: lost-response regression and identical keyed create replay.
3. Concurrency: fixed deadlines, bounded worker pools and joined observers;
   restoration on failure; full race checks and focused race count=3 pass.
4. Errors: unknown outcomes retained; partial scans/missing counters fail closed;
   relay failures prevent audit success and preserve the committed handoff.
5. Tests: red-first regressions plus two clean real-cluster rounds; no skips,
   weaker invariant or raised settling budget. Broad crash schedules are A2.3.
6. Scope: A/shared scripts, API response metadata, app tolerations, Makefile and
   evidence only. No S-owned or frozen-contract edits.
7. Hygiene: Parth author without attribution; no local-only staging, credentials,
   kubeconfig artifacts or binaries; whitespace/lint/generation clean.

## Limits and next work

This branch is based on main; #81 and all earlier A2.x dependencies are merged.
Demos 2/3/5/8/9 never cancel or fulfil a request. A2.4/#79 remains necessary for
periodic/wakeup recovery and cancel/fulfil liveness before the joint 4 November
gate. A3.2 graceful drain/PDB and A2.3 schedules remain separate. A3.3's joint
all-nine gate has not been run. A zero observed interruption count is reported
honestly when redundant pods prevent a visible failed call.

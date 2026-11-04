# A2.3 pre-PR review

Reviewed against main dba92c5 after the full 50-seed run and clean rebase.

1. Invariants: real M8/production coordinator-client operations retain exact
   canonical versions, reciprocal ownership and explicit partial-read failures.
   No production protocol, ownership, admission or ack invariant was changed.
2. Idempotency: lost committed responses retain operation IDs; pre-apply loss
   remains unknown; historical success cannot reacquire ownership. Duplicate
   terminal commands and stale/current-version confirmations remain asserted.
3. Concurrency: remote calls have bounded contexts; forwarding children are
   joined. Full matrix ran under -race; new M8 drivers and shell rollback
   regressions also passed -race -count=3. Fixture teardown uses a separate
   bounded context after assertions, including when the test context expires.
4. Errors: missing/expired seam observations fail. Required SIGKILL errors fail
   without a pod-delete fallback. Failed cleanup makes the verifier fail, while
   independent rollback steps still execute. Unknown outcomes remain distinct.
5. Tests: SCH01 pins committed-log-before-apply; SCH12/13 pin actual partial
   snapshot boundaries. All 50 seeds passed: 850 M8 + 900 kind executions, with
   150 physical SIGKILL recoveries. SCH13a is explicitly inapplicable to M8
   because M8 has no transfer. Ordinary workflows retain 30s; kind snapshot
   cases allow 3m based on measured owner runs. Aggregate 150m covers the full
   physical matrix, not an individual recovery. Final row/index assertions
   are retained; fixture recycling happens afterwards, never as fault recovery.
6. Scope: A/shared tests, scripts, Makefile and compact evidence only. No
   S-owned production source, frozen contracts, dependencies or protocols.
7. Hygiene: full make check, generation, schedules-tag vet/lint and diff checks
   passed. Source hashes match the successful run after rebase. Explicit staging
   excludes raw runs, local progress/playbooks/PDF, credentials and attribution.

Verdict: **READY for owner review**. Full local verification passed; this is
not approval, merge completion or an official joint gate result. The old draft
checkpoint evidence remains clearly labelled historical.

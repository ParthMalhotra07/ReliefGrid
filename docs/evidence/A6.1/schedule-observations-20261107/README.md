# Durable schedule observations — A6.1 / #109

SCH04 proves the exact attemptA closure before and after retargeting, with late
claims rejected at both boundaries. Every applicable production handoff now has
its event type, immutable envelope, server-filled metadata and operation identity
checked against strong durable reads. Replaying the original command must leave
current state and pending rows unchanged. Final fixture versions/claims remain
unchanged; production confirmation replaces direct synthetic confirmation steps.

## Verification

- [Result and source hashes](result.json).
- [Full M8 summary](m8-summary.log): 50 seeds ×17 applicable cases =850 PASS;
  all eight then-present tagged tests passed, none skipped. SCH13a has no M8
  transfer transport and remains explicitly not applicable, as before.
- [Kind summary](kind-summary.log): five seeds ×18 physical cases =90 PASS.
  [One complete seed](kind-one-seed.log) includes real process kills and the
  transfer/restore fault observations. This follow-up does not claim another
  50-seed kind acceptance; that earlier goal acceptance is already on main.
- [Regression controls](regressions.log): missing/duplicate/corrupted event data,
  wrong operation/version/time, missing command event and missing/foreign closure
  are rejected (`-race -count=3`). A real M8 terminal transition with its event
  deliberately omitted passes the old entity fixture but fails the durable-row
  assertion. This control is forced to M8 even with a cluster environment.
- [Validator red](validator-red.log) and [preflight red](preflight-red.log) are
  retained. [Preflight/rollback regressions](preflight-green.log) now pass.
- [Full check](check.log), [generation](gen.log), [tagged lint](tagged-lint.log),
  [tagged vet](tagged-vet.log): PASS.

To stay inside the public outbox's 100-row bound, each kind fixture confirms its
verified events into a disposable vhost before marking them. A durable audit
queue provides routing for relief.events. Foreign pending work is rejected,
including before non-relay subsets; no event is marked before a routed confirm.
The original storage image and matcher replicas were restored on exit. No PVC
was reset by this follow-up run (`SCHEDULE_RESET=0`).

SCH13a/b restore an already-matched initial fixture without a new workflow
handoff; their existing complete-row snapshot equivalence checks are preserved.
No production FSM, contract, CI workflow or invariant changed.

Pre-PR review: READY. Fault decorators only change test observations/commands in
isolated M8; all physical faults use the existing owner-provided seams. Tests
retain current-state checks independently of historical operation proofs.

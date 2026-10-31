# A2.1 — fenced cancellation and completion

Parth explicitly authorized starting Phase 2 while Phase 1 ownership reviews
remain pending. This goal is a separate branch stacked on verified #66 f6c2c8d.
The formal Phase 1 gate remains RED until its merges/main checks; this exception
does not waive ownership review, CI, invariants or post-merge goal verification.

## Behaviour and safety

Cleanup(ctx, requestID), also reached by Match for CANCELLING/FULFILLING, resumes
the API's durable cleanup intent. It preserves the already-chosen disposition,
closes only its bound attempt, reads the exact authoritative closure, then CASes
the original request version/state/attempt to CANCELLED/FULFILLED. The terminal
transition and one event commit atomically. A historical OK alone is not proof.

For the same committed cleanup version, command IDs, timestamps and payloads
are stable across duplicate helpers, clock changes and restarts. A concurrent
RetryWorkflow can advance the request version: the old final CAS loses safely,
and the next helper inspects the durable closure before doing anything else.
The first closure remains authoritative; cleanup never recalculates disposition.

Notifications use the frozen request.state_changed schema, newStatus terminal,
and request.status.cancelled/fulfilled routes, matching the existing public API.
No frozen contract, Store/FSM type or S-owned source was edited.

## Verification

Dependencies: fresh A1.2 Verify and A0.2 Verify (21/21) passed before work began.
The new cleanup tests first failed because Matcher.Cleanup was absent; see
test-first.log. Routine implementation was authorized without another checkpoint.

Fresh make verify-A2.1 passed 11 cleanup tests plus fault/barrier/disposition
subcases, all API/MQ/matcher package tests, and actual terminal-event validation
against the frozen schema with format assertions. API integration covers both
cancel and fulfill, protected assigned-resource updates and repeated status calls.

GITHUB_ACTIONS=true make check passed full ordinary/chaos race tests, both lint
runs (0 issues), pinned-broker A1.3/A1.4 Verify and the new A2.1 Verify. Tagged
vet/lint, generation, module and whitespace checks passed. See make-check.log,
verify.log and terminal-events.json. CI runs the goal verifier through the shared
Makefile, with no new Python/package or S-owned workflow dependency.

Mapping: SCH05/SCH06 cancellation fixture final states are checked directly;
late claims receive CLOSED, including the SCH07 fence property. SCH09 historical
SUCCESS replay cannot revive ownership. SCH10/SCH11 duplicate/repeated work
cannot duplicate the terminal event. Full RETRYING and all-schedule cluster
execution remain A2.2/A2.3, not claimed here.

Lost pre/post-commit close responses retain intermediate intent and retry an
identical command or use the exact committed closure. Lost terminal responses,
two helpers forced to read the same version, a winning retry reschedule, a later
different resource owner and failed authoritative reads are also exercised.

Two temporary source mutation controls proved the tests detect skipping closure
and omitting the terminal outbox event. Both failed for behavioural assertions;
the original source was immediately restored. Logs: mutations/.

Self-correction: one lint-only helper selector correction (QF1008) after the
goal verifier passed. Exact failure retained in lint-attempt1.log. No test was
weakened/skipped, no sleep added and no timeout raised.

## Remaining scope

This is bounded cleanup protocol work. It does not add periodic reconciliation
or the matching.wakeups handler (A2.4), the complete retry table (A2.2), or claim
real-cluster adversarial execution (A2.3). The public API keeps its durable
intermediate response; a later delivery/helper/reconciler resumes cleanup.
Dependency merges, S review, CI and fresh main Verify remain before ticking A2.1.

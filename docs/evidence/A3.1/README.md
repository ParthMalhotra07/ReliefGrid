# A3.1 — app demonstrations, work in progress

Implemented demo-2/3/5/8/9 through the existing assertion/evidence framework.
The driver retains frozen create inputs and every HTTP outcome, uses strong
current records for reciprocal-claim checks, rejects incomplete listings,
submits the fixed 500/50 burst, measures pod replacement, counts/retries
interrupted calls, and observes all six scaled API pods through response
metadata. App pods get explicit 30-second NoExecute tolerations; those are
an eviction delay, not a claimed recovery time.

The burst audit waits for outbox drain, pauses matcher/reconciler admission,
checks zero backlog/invariants, and restores replicas on every exit path.
The live verifier runs two rounds with a clean demo-reset -> app-deploy before
each round. It now checks the resource-event binding before a destructive reset.

## Verified so far

- Initial red regressions caught all five stubs, the missing tolerations and
  absent serving-pod metadata. Transport tests prove identical create retry
  inputs after a lost response, strict complete scans and current reciprocity.
- Admission pause/restore and the fail-before-reset topology guard have red and
  green regressions. Full make check, static A3.1 Verify and generation pass.
- Clean attempt 1 deployed successfully (205 s reset). Resource validation
  passed until the driver expected an outdated 400 for a managed status. The
  updated prompts/API require a definite 409; the driver now requires exactly
  that and asserts the entire stored row/version is unchanged after refusals.
- Attempt 1 also exposed the relay's typed-nil constructor panic, fixed and
  tested separately in #65. No workflow/ack invariant was weakened.
- Clean attempt 2 deployed successfully (158 s reset), with no matcher restarts.
  Every resource/create/update/refusal/replay assertion passed. The zero-backlog
  audit correctly failed: two resource.state_changed events were unpublished
  because no broker binding matched them. Raw invcheck and HTTP evidence are
  retained under docs/evidence/A3.1-20261101-attempt2/round-1/.

## Exact remaining prerequisite / pre-PR review

Verdict: NOT READY for a non-draft A3.1 PR. #81 adds the missing exact-key binding
in both Helm and runtime topology and has its own red/green/live broker evidence.
It must be reviewed/merged (Hrishabh), then the branch refreshed, before the
clean two-round A3.1 verifier is rerun. No success is claimed for live demo-3/5/8/9
or either complete round. The zero-backlog assertion remains strict.

Source branch is a/A3.1-demos, directly on #66 because this goal needs its app
chart and build/deploy code; it does not extend the legacy #70/#74 stack.
The current local implementation/evidence is not marked merged or complete.
A3.3 is a separate joint gate and has not been run.

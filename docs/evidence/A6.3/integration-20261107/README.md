# Combined pending-PR audit — A6.3 preparation

This is a local integration audit, not an enhanced gate, owner approval, merged
release or completed final package. Fourteen pending A-side PR snapshots were
combined on main `0fa3a63cbddb283a8ddb5f73b100a3afa967202f` in a detached worktree.
No shared PR branch or main was modified by assembling the candidate.

[Immutable input heads, tree hash and resolutions](candidate.json) identify the
exact candidate. Source PRs:121,146,123,140,128,147,148,139,134,136,137,144,145,149.
Child146 uses121 as its diff base; child136 uses134, avoiding duplicated parents.

## Verified

- [Full race/vet/lint check](check.log) and [regeneration without drift](gen.log).
- [All schedules-tagged M8 tests](m8.log):9/9 top-level tests, none skipped;
  50 seeds ×17 applicable cases =850 schedule cases. Includes exact restart,
  closure and deliberately omitted durable-event controls.
- [Combined chart checks](chart.log): application/storage ownership, UI monitor
  identity, metrics isolation, autoscaling and PS route coverage.
- [Complete metrics static Verify](a42.log): real pinned Prometheus validation
  and five PromQL fixtures, fault envelopes, noisy-forward and scratch recovery.
- [Complete scaling static Verify](a43.log), [scaling judge](scaling.log),
  [offline archive/network safety](offline.log), [scratch recovery](recovery.log).
- UI [format](ui-format.log), [Node tests](ui-node.log), [build](ui-build.log),
  and [12 browser cases](ui-browser.log), with offline/reconnect/reentry checks.

## Merge-resolution notes

1. 121+123 both append Makefile targets. Keep both metrics and autoscaling blocks.
2. In app values, keep `app.metrics` and `app.autoscaling` as separate sibling
   mappings. Keep the existing manual demo defaults and autoscaling opt-in.
3. In tools.mk keep monitoring image pins and controller pins. Preserve one
   canonical K6_VERSION/archive checksum pair from main.
4. 128's PS alias Verify targets are additional to both earlier target blocks.
5. 121+136 overlap updateRequest: retain the metrics Observer calls, then use
   `committedUpdate(r, result)` for conditional-update error handling. Retaining
   the older `committed(result)` here would lose the new If-Match/412 semantics.

## Remaining acceptance

The audit does not replace CI against each actual rebased merge head. Owner
review and required checks still apply; no aggregate candidate was pushed.
A4.4 still needs the S-owned transactional-aging interface (#135) and internal
auth interface (#29). Contract134 needs both owners before136 can merge.
The joint enhanced gate/experiment choice, S load-report evidence, three real
swapped-presenter rehearsals, final recording and submission remain pending.
Draft rehearsal144 is included only as preparation, never as executed rehearsals.
Offline139's nine-demo physical proof and149's five-seed kind proof are separate
immutable runs; no combined physical gate is claimed here.

Publication-branch checks: [full check](publication-check.log) and [generation](publication-gen.log) also passed after rebasing the README branch onto current main.

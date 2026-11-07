# ADR004 — Durable attempt closure and intermediate workflow states

Status: implemented core decision; report text awaiting owner review.

## Context and decision

A request and resource can belong to different shards, so assignment is not one
cross-shard transaction. Commit a request-side bound MATCHING intent before the
resource TryClaim, then confirm only the current request version/attempt. Before
abandoning or retargeting an attempt, commit its durable closure. Resource-side
closure rows survive later reuse and fence delayed claims.

Use RETRYING for abandonment and CANCELLING/FULFILLING for cleanup, retaining the
intent until closure and the required disposition are durable. A historical
deduplicated SUCCESS never proves current ownership. Unknown outcome keeps the
intent and same operationId; it is not a rejection.

## Alternatives and cost

An optimistic claim followed by best-effort undo leaves a delayed claim able to
reappear. Clearing the intent before a known rejection loses recovery information.
A cross-shard distributed transaction would introduce a different protocol and
failure model. Durable closure adds records, round trips and reconciliation work,
but makes late messages/crashes recoverable without dropping the ownership fence.

Assumptions: per-shard committed transactions and a retained closure history;
recovery can become unavailable without a majority. Tests cannot justify deleting
closure rows before all delayed attempts are impossible.

## Falsification and evidence

[Adversarial fixtures](../../tests/workflow/fixtures.go) and
[drivers](../../tests/workflow) exercise delayed claims, lost replies, confirmation
versus retarget, cancellation and restart boundaries. A request with two live claims,
a terminal request owning a claim, or a delayed claim accepted after closure would
falsify the decision's intended safety. See [workflow](../../internal/workflow),
[FSM rules](../../internal/fsm) and [schedule evidence](../evidence/A2.3/README.md).

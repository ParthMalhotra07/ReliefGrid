# Remaining A1.1 review follow-ups (issue 63, items 2-4)

Item 1 was implemented and checked first; see ../63-known-refusals/. While A1.3
awaited S review, completed the remaining independent A1.1 maintenance work on
the same branch/PR, preserving the one-goal review boundary.

## Deterministic intent-write race

The new test uses the real three-shard memstore plus Before barriers. The first
create pauses at its intent CAS until the second reaches its own intent CAS,
which proves both initial reads found nothing. The second CAS waits until the
first reaches domain creation, proving the first intent is committed. The
second then deterministically loses CAS and must reread the winning intent.

The same payload must replay byte-identical original 201/Location even though
the second handler proposes a different timestamp. A changed payload must
return 409. Both paths must leave exactly one request and perform two intent
write attempts. There are no timing sleeps or probabilistic repeated runs.

Negative control: temporarily removed only the losing writer's reread in the
local production source, then ran this test. Both subcases failed with 404
instead of 201/409. Restored the original source immediately in a finally
block. The reread is present in the committed code; see the mutation log.

## Keyed and unkeyed collisions

New tests use real committed entity/operation collisions before the public
CreateEntity apply, for both request and resource routes. Eight cases cover
with/without an Idempotency-Key and EXISTS/OPERATION_ID_REUSED. Before the code
fix, all four unkeyed cases wrongly reported CONFLICT_IDEMPOTENCY_KEY.

Now only keyed creates translate these outcomes into the public idempotency
conflict. Unkeyed creates use ordinary DOMAIN_CONFLICT/OPERATION_ID_REUSED.
The existing frozen public codes/statuses are retained, and all eight cases pass.

Negative control: disabled only the keyed conflict translation in a temporary
local source mutation. All four keyed collision cases failed with ordinary
domain/operation codes instead of CONFLICT_IDEMPOTENCY_KEY. Restored source
immediately. This proves the missing EXISTS translation coverage is now present.

## Retention documentation

The comment next to reserve explicitly says api-create: records are immutable
idempotency metadata, must not be deleted/independently expired, and preserve
the exact command/original response. A tombstone prevents create-if-absent reuse;
deletion needs a deliberate migration. No tombstone-repair behavior is claimed.

## Actual validation

```text
go test -race -count=1 ./internal/api -run 'TestAPICreationIntentCASLoserRereadsWinnerDeterministically|TestAPICreateIdentityConflictsOnlyUseIdempotencyCodeWithAKey' -v
--- PASS: TestAPICreationIntentCASLoserRereadsWinnerDeterministically (0.23s)
--- PASS: TestAPICreateIdentityConflictsOnlyUseIdempotencyCodeWithAKey (0.35s)
ok github.com/ParthMalhotra07/ReliefGrid/internal/api 1.656s

make check
0 issues.
0 issues.
ok github.com/ParthMalhotra07/ReliefGrid/internal/api 4.476s

bash scripts/verify-A1.1.sh
ok github.com/ParthMalhotra07/ReliefGrid/cmd/api 1.108s
ok github.com/ParthMalhotra07/ReliefGrid/internal/api 4.444s
A1.1 VERIFY: ALL PASS
```

Full repository race and chaos checks passed; all four API outbox schemas
validated; generation/tidy left no changes. New replay/collision tests add ten
subcases beside the initial 18 refusal cases. No assertion was weakened,
skipped or given a larger timeout. Both mutation controls are expected failures
with intentionally changed production code, not flaky runs of the final source.

Pre-PR review: READY. Only A-owned create code/tests and evidence changed. The
existing intent reread, operation dedupe, durable handoff and original-response
replay remain intact. No contract, module, S-owned source or local-only file
changed. Issue 63's four requested follow-ups are covered by this branch; human
review/merge and fresh main verification are still required.

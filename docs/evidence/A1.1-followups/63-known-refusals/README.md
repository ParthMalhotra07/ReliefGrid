# A1.1 known mutation refusals (issue 63, item 1)

A typed coordinator-client error describes whether a write was sent or refused
before proposal. The public API previously discarded that distinction when a
mutation had no committed Result, returning retryable OUTCOME_UNKNOWN for a
known INVALID or other refusal. This could tell clients to retry invalid work.

The API now preserves the existing public error mapping for INVALID, NOT_FOUND,
DOMAIN_CONFLICT, VERSION_CONFLICT and OPERATION_ID_REUSED. A not-sent transport
error remains retryable STORAGE_UNAVAILABLE with outcomeUnknown=false. An
unclassified upstream refusal is STORAGE_UNAVAILABLE without claiming a write
might have committed; its retryability comes from the typed client. Internal
statuses are not forwarded blindly (VERSION_CONFLICT remains public 409).

Unknown outcomes and unclassified mutation transport failures retain retryable
OUTCOME_UNKNOWN. Durable creation intent, operation identity, payload hashes,
response replay and all committed storage behavior are unchanged.

## Test first

Added TestAPIMutationErrorsPreserveKnownRefusals before the fix: nine error shapes
at both domain creation and creation-intent CAS boundaries, 18 subcases. Known
refusals failed with the old 503 OUTCOME_UNKNOWN; the unknown controls already
passed. The tests use real three-shard SQLite memstore with fault wrappers and
the public handler, assert the frozen error schema and Retry-After semantics,
and check that a refused fixture has no request written.

Actual red output (see refusal-red.log):

```text
HTTP 503: {"code":"OUTCOME_UNKNOWN", ... "outcomeUnknown":true,"retryable":true}
; want 400
--- FAIL: TestAPIMutationErrorsPreserveKnownRefusals
```

## Validation after the fix

```text
go test -race -count=1 ./internal/api -run TestAPIMutationErrorsPreserveKnownRefusals -v
--- PASS: TestAPIMutationErrorsPreserveKnownRefusals (0.93s)
ok github.com/ParthMalhotra07/ReliefGrid/internal/api 1.979s

make check
0 issues.
0 issues.
go test -race -tags chaos ./cmd/invcheck/...
ok github.com/ParthMalhotra07/ReliefGrid/cmd/invcheck 1.996s

bash scripts/verify-A1.1.sh
ok github.com/ParthMalhotra07/ReliefGrid/cmd/api 1.089s
ok github.com/ParthMalhotra07/ReliefGrid/internal/api 3.675s
A1.1 VERIFY: ALL PASS
```

All repository race tests passed. The existing real committed-create/lost-response
and keyed-replay test also passed. All four actual API outbox envelopes validated
against their frozen schemas. make gen-check and go mod tidy -diff left no changes.
Raw successful commands are in refusal-green.log, make-check.log and verify.log.

## Review and limits

Only the A-owned public error mapper and its regression test change. No contract,
module, generated, FSM or coordinator-client source change. The new branch is a
maintenance follow-up to merged A1.1, preserving the approved goal's behavior.
No tests were weakened, skipped or given larger timeouts.

Pre-PR review: READY for this scoped fix. It neither grants ownership nor changes
acknowledgement or retry handoffs. Unknown outcomes remain distinct from refusals.
Issue 63 items 2-4 (deterministic intent race coverage, EXISTS mapping/coverage and
metadata tombstone documentation) remain open and are not claimed resolved.

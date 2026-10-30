# A1.1 public API verification — 30 October 2026

Base: main a57054e. Branch: a/A1.1-public-api; existing PR #59.
Goal checks ran against the implementation and tests in this change before
the evidence commit. No contract, generated, module or S-owned code changed.

## Results

```text
go test -race ./cmd/api/... ./internal/api/...
ok  github.com/ParthMalhotra07/ReliefGrid/cmd/api 1.079s
ok  github.com/ParthMalhotra07/ReliefGrid/internal/api 3.370s

make check
0 issues.
0 issues.
go test -race -tags chaos ./cmd/invcheck/...
ok  github.com/ParthMalhotra07/ReliefGrid/cmd/invcheck (cached)

bash scripts/verify-A1.1.sh
ok  github.com/ParthMalhotra07/ReliefGrid/cmd/api 1.080s
ok  github.com/ParthMalhotra07/ReliefGrid/internal/api 3.064s
ok    actual API event validates: request.created
ok    actual API event validates: request.state_changed
ok    actual API event validates: resource.registered
ok    actual API event validates: resource.state_changed
A1.1 VERIFY: ALL PASS

make gen-check
git diff --exit-code -- internal/api/gen internal/internalapi

```

`go mod tidy -diff` and the diff check over contracts, generated packages and
module files both exited 0 without output.

Raw output: make-check.log and verify.log. The Verify uses `-race -count=1 -v`;
there are 22 top-level tests across cmd/api and internal/api, plus their cases.
JSON responses are checked against the frozen OpenAPI schemas using the pinned
kin-openapi loader, and actual outbox payloads against their JSON Schemas.

## Failure cases exercised

The real memstore's fault hooks inject an intent-response loss, a failure before
domain creation and a committed creation with its response lost. A retry on a
fresh handler preserves the frozen metadata and creates one entity. Concurrent
handlers with different proposal timestamps converge on one original response;
concurrent different payloads yield one winner and a conflict. Replays after
request status or resource window edits preserve the original 201 and Location.

Other cases cover complete versus partial scans, durable NO_CANDIDATE/deferred
handoffs, all nine operations, all five entity types, required nullable response
fields, strict validation, public version conflict/operation reuse, unavailable
read classifications, capped remote deadlines, SSE access/cancellation and
process HTTP service backed by the real FSM.

Cleanup tests retain the live claim in CANCELLING/FULFILLING, check all resource
dispositions and repeat calls without another mutation. The test then drives
the real closure and terminal commands to verify that the public projection
does not present a historical assignment as current ownership. This is not an
implementation of the later A2.1 recovery worker.

## Corrections and review

Parth explicitly approved correcting the first fixtures' short keys to valid
UUIDs, enforcing the existing 36-character constraint and adding rejection
tests. The frozen contract was not relaxed. `validation-red.log` records the
new failures for invalid keys, explicit null defaults and nondecimal near
coordinates before their code fixes. `terminal-projection-red.log` records the
historical-assignment projection failure before its fix.

The closure fixture was corrected to the actual `CloseAttempt.Attempt` shape.
Lint required context-aware httptest constructors and a boolean-expression
style correction. No assertions were loosened, tests skipped, sleeps added or
timeouts raised. Test-first approval and initial missing-implementation failure
are recorded in test-first.md.

Pre-PR review: READY for review of A1.1. Preconditions remain in committed FSM
Apply; the API never mutates claims directly; metadata alone never produces a
201; unknown outcomes retain the durable command; partial scans fail visibly;
all store calls have deadlines. Scope is A-owned API code plus its verifier,
documentation and evidence. No local-only files are staged. Full GitHub CI and
S approval remain required; A1.1 is unticked until human merge and main Verify.

## Limits

Single trusted caller namespace; no multi-user auth or cloud release claim.
SSE is a heartbeat stub. A2.1 owns closure recovery; A1.3/A1.4 own messaging and
relay; A1.5 owns real cluster deployment. No real cluster public-API smoke run
is claimed here. Core lists have no public cursors and conservatively signal
more data when the merged result is truncated. See internal/api/README.md.

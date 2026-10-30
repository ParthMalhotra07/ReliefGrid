# A1.1 test-first checkpoint

Baseline: main a57054e, branch a/A1.1-public-api. Draft #59 starts with the
acceptance plan at 79afac8; its baseline make check and gen-check passed before
push. No public API implementation is present.

The local, uncommitted internal/api/api_contract_test.go proposes api.New(Config)
as an HTTP handler over store.Store. It tests the real three-shard SQLite
memstore, with its existing fault wrapper rather than a permissive fake.

The first cases cover all nine operation paths, registration geography and
reciprocal matching, replay after status/window changes and handler restart,
concurrent keyed proposals, caller namespace isolation, a lost committed create
response, validation/error cases, partial authoritative lists and SSE shutdown.
This is the first test batch; it is not yet a claim of exhaustive route/schema
coverage or a passing A1.1 implementation.

Command:

```sh
go test -race ./cmd/api/... ./internal/api/...
```

Actual output (exit 1):

```text
go: warning: "./cmd/api/..." matched no packages
github.com/ParthMalhotra07/ReliefGrid/internal/api: no non-test Go files in /mnt/c/Users/am/Desktop/CC/.reviews/A1.1/internal/api
FAIL github.com/ParthMalhotra07/ReliefGrid/internal/api [build failed]
?    github.com/ParthMalhotra07/ReliefGrid/internal/api/gen [no test files]
FAIL
```

This is the expected missing-implementation failure. The test assertions still
need Parth's review; implementation waits for `ok, continue`, as the playbook
requires. The tests and this checkpoint evidence remain local until pre-push
checks can pass. No failing test was skipped, hidden or pushed around make check.

Raw output: C:/Users/am/Desktop/CC/.tools/A1.1-test-first-2026-10-30.log.

## Review correction checkpoint

After Parth approved continuation, the first implementation passed these tests.
Further schema review found that the fixtures' short Idempotency-Key labels
conflict with public.yaml's frozen UUID / exactly-36-character constraint.
The first implementation also needs that constraint enforced. Stopped under
the playbook's test/plan rule and requested approval to replace fixture keys
with valid UUIDs and add invalid-key rejection tests. No assertion or contract
has been weakened or silently edited. The initial pass is not full A1.1 proof.

Parth subsequently approved: "Approve Correct the fixtures and continue".
Fixtures now use deterministic valid UUIDs; invalid UUID/length/duplicate header
cases explicitly expect 400. The implementation enforces the existing frozen
constraint. Further strict-input and terminal-projection tests failed before
their code fixes. The complete 22-test verification and all repository checks
now pass; see README.md, make-check.log and verify.log. Implementation/tests
can now be committed without bypassing the pre-push checks.

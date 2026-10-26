# A0.1 review corrections — 26 October 2026

## Behaviour

The public contract follows the stored domain model: request details, affected
people and urgency; nullable assignment and read-only recovery summary;
resource reusability, claim identity and location revision. Public versions are
opaque strings. Create idempotency is optional, and a keyed replay preserves
the first committed 201 body and Location. Public errors have an enumerated
mapping; resource listings disclose scan positions/failures; SSE documents
heartbeats and protected-route errors. Event schemaVersion is an integer and
resource claim events use requestId/attemptId.

The Makefile conflict was resolved by retaining gen-internal and adding the
public generator to gen-public. make gen runs both. This also addresses #37.
The generated runtime is now a direct module dependency after go mod tidy.

## Regression first

New tests were run against the previous schemas before correction:

```text
--- FAIL: TestPublicContractStoreBoundary
CreateRequestBody missing stored field details
assignedResourceId must be present and nullable when unassigned
ResourceResponse version must be an opaque string
--- FAIL: TestPublicCreateReplayAndErrorContract
Idempotency-Key must be optional; no-key retries can duplicate
POST /requests must replay the original 201
--- FAIL: TestPublicEventSchemaVersionsAndClaimIdentity
resource claim event must use requestId/attemptId, not an invented claimId
```

The same tests now pass. Examples are validated against the actual OpenAPI
schemas, and adversarial inputs cover geography, unknown urgency, zero affected
people, client assignment writes, unsupported request windows and long details.
A valid control create must also pass. The obsolete Disposition assertion was
replaced with boolean-reusability checks; no test was skipped or relaxed to
permit an invalid domain shape.

One correction run caught a stale nested MatchResponse example still using
requestorId/title/windows. The example was corrected and the same tests and
make check rerun successfully.

## Verification

Checked with Go 1.27.2 and the repository's pinned toolchain, after integrating
main e5abef0. Source correction commit: 94b4b52.

```text
go test ./contracts -run TestPublic -count=1
ok github.com/ParthMalhotra07/ReliefGrid/contracts
make check
go vet ./...
golangci-lint run ./...
0 issues.
go test -race ./...
```

make check exited 0; all packages passed, including the public contract tests
and the coordinator/storage tests. The full goal verifier was then run on the
committed contract tree:

```text
=== Results: 16 passed, 0 failed ===
A0.1 VERIFY: ALL PASS
```

It includes OpenAPI lint, event examples, generation of both contracts with no
contract/generated diff, generated-code compilation and the new regressions.
The raw verifier output is in verify.log; the full final check output is in
check-2026-10-26.log. The former empty go-vet.log was removed instead of
presenting an empty file as output.

## Limits

These are contracts and generated types; A1.1 supplies handlers and executable
runtime validation. UTC window ordering is a documented handler precondition,
since OpenAPI cannot express the comparison between the two fields. This PR
does not establish end-to-end matching, broker topology or disk admission.

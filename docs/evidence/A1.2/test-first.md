# A1.2 test-first checkpoint — 26 October 2026

Branch: `a/A1.2-matching-workflow`, based on `origin/main` at
`c61912ff883624cfec17195c09ba6be222d6fbcb`.

This checkpoint contains tests and a verifier only. There is no geography or
matcher implementation yet. The new assertions have **not executed**: compilation
fails because the APIs they describe do not exist. Nothing is committed or pushed
at this checkpoint.

## Prerequisites

The prerequisite A0.2 verifier was run on unchanged main:

```text
=== Results: 21 passed, 0 failed ===
A0.2 VERIFY: ALL PASS
```

`make check` was also run on unchanged main at c61912f. Exit code: 0.
Key output:

```text
go vet ./...
golangci-lint run ./...
0 issues.
go test -race ./...
ok  github.com/ParthMalhotra07/ReliefGrid/internal/fsm/memstore (cached)
ok  github.com/ParthMalhotra07/ReliefGrid/tests/workflow (cached)
```

The complete main test run passed, with cached package results. This does not
establish that A1.2 passes; A1.2 intentionally does not build yet.

## Tests prepared

- Known geohash cells, invalid coordinates, Haversine distances and a 50 km cover
  across cell boundaries, the dateline and the poles.
- Matching type, radius, availability and status filters; inclusive start and
  exclusive end; reciprocal claims and committed intent before TryClaim.
- Reproducible seeded selection among three candidates after resource-ID ties.
- Cursor continuation beyond an ineligible first page; authoritative fallback
  after an empty hint scan; partial scans cannot produce NO_CANDIDATE or a handoff.
- A complete negative result commits WAITING, a due time and retry outbox together.
- Unknown claims retain the intent and retry an identical command after matcher
  restart and clock advancement; a dropped intent-CAS response does not create a
  replacement intent.
- Retargeting requires the exact old closure; a held-resource observation with an
  unresolved claim cannot authorize a new claim.
- A delayed first claim after closure returns CLOSED, while a historical SUCCESS
  after cancellation cannot authorize confirmation.

The tests import A0.2's SCH02, SCH07, SCH09 and SCH17 state fixtures. Interleaving
hooks and call traces delegate to the real three-shard SQLite memstore; they do
not implement a replacement FSM. Executing all 17 schedules remains A2.3 work.

The local API proposed by the tests is `geo.Encode`, `geo.HaversineKM`, `geo.Cover`,
and `workflow.New(Config).Match(ctx, requestID)`. The matcher returns a result with
an Outcome; Config supplies the authoritative Store, optional CandidateScanner,
clock, ID source, selection seed and scan limit. This introduces no wire-contract
changes. Coordinator consistency options remain S-owned follow-up #41.

## Actual red run

Command, in the A1.2 worktree with the pinned WSL Go toolchain:

```sh
bash scripts/verify-A1.2.sh
```

Exit code: 1. Output:

```text
=== A1.2 Verify: geography and matching ===
# github.com/ParthMalhotra07/ReliefGrid/internal/geo [github.com/ParthMalhotra07/ReliefGrid/internal/geo.test]
internal/geo/geo_test.go:21:15: undefined: Encode
internal/geo/geo_test.go:30:16: undefined: Encode
internal/geo/geo_test.go:48:11: undefined: HaversineKM
internal/geo/geo_test.go:52:18: undefined: HaversineKM
internal/geo/geo_test.go:75:21: undefined: Cover
internal/geo/geo_test.go:80:20: undefined: HaversineKM
internal/geo/geo_test.go:83:18: undefined: Encode
FAIL github.com/ParthMalhotra07/ReliefGrid/internal/geo [build failed]
FAIL github.com/ParthMalhotra07/ReliefGrid/internal/workflow [build failed]
FAIL
# github.com/ParthMalhotra07/ReliefGrid/internal/workflow [github.com/ParthMalhotra07/ReliefGrid/internal/workflow.test]
internal/workflow/helpers_test.go:33:45: undefined: Config
internal/workflow/helpers_test.go:35:9: undefined: Config
internal/workflow/matcher_test.go:30:12: undefined: New
internal/workflow/matcher_test.go:63:14: undefined: New
internal/workflow/matcher_test.go:103:14: undefined: New
internal/workflow/matcher_test.go:126:15: undefined: New
internal/workflow/matcher_test.go:161:12: undefined: New
internal/workflow/matcher_test.go:201:12: undefined: New
internal/workflow/matcher_test.go:233:12: undefined: New
internal/workflow/matcher_test.go:250:12: undefined: New
internal/workflow/matcher_test.go:250:12: too many errors
```

This is the GOAL LOOP step 5 checkpoint. Implementation, `make check` on the
implemented branch, the passing A1.2 verifier and pre-PR review remain outstanding.

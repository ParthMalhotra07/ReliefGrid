# A1.2 — matching workflow verification

Date: 26 October 2026. Base: main c61912f (including scan cursor #38).
Branch: `a/A1.2-matching-workflow`.

The test-first checkpoint is in [test-first.md](test-first.md). Parth subsequently
approved implementation with "ok, continue". The approved tests were retained;
three additional failure/concurrency cases were added during review.

## Actual verification

| Command | Result | Evidence |
| --- | --- | --- |
| `bash scripts/verify-A0.2.sh` | 21 passed, 0 failed | Output excerpt below |
| `make check` | Exit 0; lint 0 issues; race tests pass | [make-check.log](make-check.log) |
| `bash scripts/verify-A1.2.sh` | Exit 0; 21 top-level tests pass | [verify.log](verify.log) |
| `make gen` | Exit 0; generated files unchanged | Output excerpt below |
| `git diff --exit-code -- contracts internal/internalapi go.mod go.sum` | Exit 0 | No output |

The checks ran with the repository's pinned Go toolchain in WSL Ubuntu. The
A1.2 verifier uses `go test -race -count=1 -v`; its output is uncached. The final
whole-repository check reused cached results for unchanged packages and ran the
changed geography/workflow packages. No passing check was inferred from CI.

```text
=== Results: 21 passed, 0 failed ===
A0.2 VERIFY: ALL PASS

go vet ./...
golangci-lint run ./...
0 issues.
go test -race ./...
ok  github.com/ParthMalhotra07/ReliefGrid/internal/geo      1.028s
ok  github.com/ParthMalhotra07/ReliefGrid/internal/workflow 5.277s

ok  github.com/ParthMalhotra07/ReliefGrid/internal/geo      1.023s
ok  github.com/ParthMalhotra07/ReliefGrid/internal/workflow 4.584s
A1.2 VERIFY: ALL PASS

gen-public: no contracts/public.yaml yet
go tool oapi-codegen -generate types -package internalapi \
    -o internal/internalapi/types.gen.go contracts/internal.yaml
```

The gen-public line is the existing main placeholder, owned by the still-open
A0.1 PR #5. A1.2 does not replace or claim verification of the public contract.

The original geography and 15 workflow tests passed on the first implementation
run. Review added tests for failure on a later cursor page, preserving a claim
command after RetryWorkflow changes UpdatedAt, and two workers reaching the same
intent CAS before either applies. All passed. Self-correction attempts used: 0.

The initial polar test took 1.71 s. Its all-longitude cover produced 256 precision-3
prefixes. Coarsening polar caps reduces that to 32 precision-2 prefixes while
retaining the exact <=50 km Haversine filter; the final polar test took 0.11 s.
No timeout or assertion was increased or weakened.

## Pre-PR review

Verdict: **READY for S review** within A1.2 scope.

- Safety: intent CAS precedes TryClaim. Retargeting requires the exact closure or
  a committed CLOSED/CONFLICT_CLOSED/REJECTED_CLOSED result. Unknown outcomes retain
  the intent. SUCCESS requires authoritative current ownership before confirmation.
- Idempotency: claim IDs and command timestamps survive restart and RetryWorkflow.
  Mutation retries capture an identical command. Transition/outbox IDs are stable.
- Concurrency: exact request version/status/attempt fences confirmations and
  retargets; the barrier test verifies the losing intent CAS issues no claim.
  Race checks pass. Every remote call and each invocation has a deadline.
- Scans: all cursor pages and expected shard positions are checked. Partial or
  interrupted walks cannot produce a negative result or retry handoff.
- Scope: geography, workflow, its verifier and evidence only. No S-owned source,
  contracts, module dependencies, cluster state or local-only instructions changed.
- Limitations: coordinator stale-read options remain #41; default scans are strong.
  API, RabbitMQ consumers/relay and real Raft end-to-end verification belong to
  A1.1/A1.3–A1.5. Cleanup/reconcilers and running all 17 schedules remain A2.x.
  The deterministic unsampled event trace context is not end-to-end tracing.

A1.2 remains unticked until its PR is human-merged and verified on main.

# A1.1 public API: acceptance plan

Goal: implement the nine operations in contracts/public.yaml under /api/v1,
using store.Store, generated public types and the existing matching workflow.
The deployed process uses coordclient; protocol tests use the real SQLite FSM
through memstore. No permissive store substitute or contract change is planned.

## Test-first review

The first test checkpoint covers:

- Every route and method; required public fields, null assignment fields and
  opaque versions. All five resource/request types, geography, bounded text,
  urgency, people counts, UTC resource windows, query bounds and forbidden
  client assignment/workflow fields.
- Both creation endpoints: optional idempotency key, canonical client payload,
  caller/operation namespaces, deterministic IDs, identical original 201 body
  and Location on replay after later entity changes and handler restart, and
  CONFLICT_IDEMPOTENCY_KEY/409 for a changed payload. Concurrent handlers with
  different proposal times must converge on the first committed creation.
- Lost creation response after commit: keep unknown outcome distinct from a
  rejection and recover the same keyed create without a duplicate entity.
- 400/404/409/503 public error envelope, Retry-After and correlation IDs;
  public CAS conflict is 409. A partial authoritative scan never becomes an
  empty success.
- Resource registration writes a geohash the existing matcher can find;
  explicit matching uses workflow.Matcher and preserves reciprocal ownership.
  Already matched requests return their existing match.
- Validated status changes cannot overwrite active claims. Cancellation and
  fulfillment can enter durable cleanup states; A2.1 owns the full recovery loop.
- SSE content type, an initial/periodic heartbeat and context cancellation;
  RabbitMQ-backed event projections are later integration work.

The idempotency design must preserve the complete original response durably.
The current Store result has no creation body, and FSM dedupe hashes the full
command including its timestamp. A process-local cache, current entity read,
or regenerated metadata alone is insufficient. Tests must demonstrate replay
after resource-window edits and restart before accepting an implementation.

## Checks

```sh
make check
go test -race ./cmd/api/... ./internal/api/...
make gen-check
bash scripts/verify-A1.1.sh
```

The draft started with this acceptance plan. The failing-test checkpoint and
the UUID-fixture correction were approved before the final implementation.
The complete source/tests now pass the repository and goal checks; see README.md
and the raw evidence logs. This plan is not a substitute for those results.

## Baseline

Based on main a57054e, after human merge of A0.3 #9. Main passed make check,
A0.1 Verify (16/16), A0.2 Verify (21/21) and A0.3 live broker restart Verify
on 30 October 2026. Contracts/public.yaml and both generated packages are clean.
Existing #42 is a tracked internal-contract example fix owned jointly; this
goal will not copy its relief.resources example. #41 is an upcoming coordinator
read-options handoff; strong default reads are safe for this API goal.

## Scope and dependencies

Application code: cmd/api/ and internal/api/ (generated files unchanged).
Verification: scripts/verify-A1.1.sh and docs/evidence/A1.1/.
Storage/FSM/coordinator implementation remains owned by S. Full application
deployment is A1.5; topology and relay remain A1.3 and A1.4. Local playbooks,
progress, prompts and architecture PDFs are excluded from commits.

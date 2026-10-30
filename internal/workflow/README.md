# Matching workflow [A1.2]

Construct a matcher with an authoritative `store.Store` and `ScanLimit` in
`1..1000`. `memstore.New` runs the real SQLite FSM for tests; a coordinator client
provides the same surface in the cluster. A separately configured `CandidateScanner`
may supply stale hints. Until coordinator read options land in #41, using the
default authoritative scanner is safe.

`Match(ctx, requestID)` reads the request, discovers candidates, commits an immutable
MATCHING intent, claims the resource, verifies current ownership and confirms the
exact request version/attempt. Confirmation writes `match.completed` atomically.
Intent creation writes `match.proposed`. A negative complete authoritative scan
writes WAITING, a five-second due time and `request.retry` to the outbox together.

Scans retain only the nearest three eligible resources, ordered by Haversine
distance and then ID. Selection hashes the configured seed, request key and
source version; it is reproducible and independent of scan insertion order.
Geohash precision-3 bounding-box prefixes (coarser at polar caps) reduce the candidate set; the final
predicate is distance <= 50 km, equal type, AVAILABLE without a claim and an
inclusive-start/exclusive-end availability window. Resource index hashes should
use at least three characters; the tests use seven, and API writers should do likewise.

Every cursor page must account for the expected shards. Partial pages, missing
positions, repeated cursors and the 1024-page limit return incomplete work. One
ring-version mismatch restarts the entire walk, discarding old candidates. An
empty/failed hint scan falls back to an authoritative walk before NO_CANDIDATE.
Candidates are re-read authoritatively before an intent is proposed.

## Durable command identity

An emitted fingerprint is `rg1:<proposal Unix ms>:<seed>:<SHA-256 digest>`. The
digest binds the request/version, type and location, resource eligibility snapshot,
timestamp and selection seed. This is a convention inside the existing opaque
fingerprint string, with no wire or FSM changes. It retains enough proposer input
to reconstruct TryClaim after restart, including after RetryWorkflow changes
UpdatedAt. Unversioned imported fixture intents use the immutable request creation
timestamp; new production intents always use rg1.

Claim operation IDs derive from request and attempt identity. Transition/event
IDs are deterministic UUIDs derived from their immutable command identity. Each
mutation may be retried once with the identical command. A returned error leaves
the stored intent intact. A resource held by another attempt does not authorize
retargeting; the exact attempt must have a committed rejection-and-closure or an
authoritative matching closure first.

SUCCESS, including a replay, requires a current request read and reciprocal
resource claim before confirmation. Concurrent cancellation/cleanup is fenced
by the exact request-state/version/attempt CAS. Eight claim-resolution steps and
overall/per-call deadlines bound each invocation; exhausting work preserves the
intent and returns an error.

## Integration and remaining goals

Callers must not acknowledge errors. MATCHED and IGNORED describe current committed
state; NO_CANDIDATE includes a committed retry handoff, and DEFERRED describes an
existing future WAITING schedule. RETRYING/CANCELLING/FULFILLING return
ErrNeedsRecovery without clearing the intent. Their cleanup, consumer backoff,
reconcilers and all 17 schedules remain A2.x work. RabbitMQ publication and
deployment remain A1.3–A1.5; this goal does not run the real cluster path.

Outbox envelopes carry a deterministic, unsampled root traceparent for retry
stability. End-to-end tracing and propagation belong to the later tracing work.
The three-shard scans are fenced pages, not a global snapshot: concurrent inserts
can be found on a later retry or resource-registration wakeup.

Run `bash scripts/verify-A1.2.sh` and `make check`. See
`docs/evidence/A1.2/` for the red checkpoint and actual passing output.

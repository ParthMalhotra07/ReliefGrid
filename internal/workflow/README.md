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
writes WAITING, a tiered due time and `request.retry` to the outbox together.

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
existing future schedule in any work state. RETRYING resumes exact-attempt
closure before WAITING; CANCELLING/FULFILLING resume the bounded A2.1 cleanup
below. Periodic reconciliation and the complete cluster adversarial suite remain
A2.3–A2.4; the focused goal verifier uses the real in-memory FSM and a live broker.

Outbox envelopes carry a deterministic, unsampled root traceparent for retry
stability. End-to-end tracing and propagation belong to the later tracing work.
The three-shard scans are fenced pages, not a global snapshot: concurrent inserts
can be found on a later retry or resource-registration wakeup.

Run `bash scripts/verify-A1.2.sh` and `make check`. See
`docs/evidence/A1.2/` for the red checkpoint and actual passing output.

## Cancellation and completion [A2.1]

The API commits CANCELLING/FULFILLING with the immutable attempt and the chosen
resource disposition. Cleanup(ctx, requestID), also reached by Match for those
states, reads the exact authoritative closure. If absent it submits CloseAttempt
for only that attempt. A returned historical OK is not enough: the helper must
read the exact bound permanent closure before proposing the terminal CAS.

The committed cleanup version supplies its proposal timestamp and disposition.
Operation IDs include that version and frozen payload identity, so retries and
restarts of the same version use identical commands despite clock changes.
RetryWorkflow may advance the request version; an old final CAS then loses
safely. A later helper inspects the durable closure first and never repeats an
old release or changes the stored disposition. Concurrent helpers use the same
deterministic final command, allowing one terminal mutation and one outbox row.

Cancellation and fulfillment notifications use the frozen request.state_changed
schema with newStatus=CANCELLED/FULFILLED and request.status.cancelled/fulfilled
routing, following the public API's existing convention. The event and terminal
transition commit together. Terminal records retain the FSM's historical audit
fields; the API hides historical assignment as a current ownership grant.

Unknown mutations and failed authoritative reads return errors with recovery
intent intact. A recorded closure cannot release a later different owner. The
helper does not start background scans or consume matching.wakeups; those remain
A2.4. Run make verify-A2.1 for the real-FSM fault/barrier cases, frozen schema
validation and API protections. make check runs it in CI via the shared Makefile.

## State-aware retries [A2.2]

Production consumers call Handle, which performs Match and, after an unresolved
error, attempts an exact-version RetryWorkflow. This commits nextAttemptAt and
request.retry together without changing status, intent or cleanup disposition.
The first business retry uses 5s, the second 30s, and subsequent retries 120s.
Future due times deduplicate old hints in all work states. Due WAITING with no
candidates uses RetryWorkflow because WAITING -> WAITING is not a domain transition.
A partial shard scan preserves its incomplete status; it never becomes NO_CANDIDATE.

Unknown claim outcomes retain the attempt and reconstruct the same TryClaim
operation ID and payload after retry metadata changes or process restart.
Abandon first CASes the exact MATCHING version/attempt to RETRYING, then closes
only that attempt, reads its permanent closure, and finally CASes to WAITING.
A confirmer that wins first is never cleaned up; when RETRYING wins the old
confirmation loses its exact CAS. The stored RETRYING version supplies stable
closure inputs; an expired availability window selects OFFLINE. A later helper
accepts the first exact closure and never changes its disposition.

If no workflow handoff can commit, Handle returns an error. Production then
uses the separately confirmed durable transport handoff described in mq/README.md;
it does not invent a successful domain result. Broker retry counts and persisted
business retryCount remain separate. Background due scans, resource wakeups,
and broad real-cluster crash schedules remain A2.3/A2.4.

Run make verify-A2.2. docs/evidence/A2.2 records fault/barrier tests, live recovery
after 25 transport failures, unroutable publishing, and a lost-confirm duplicate.

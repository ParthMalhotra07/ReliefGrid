# A1.3 review corrections — 31 October 2026

Addresses Hrishabh's requested changes on #61, review 5721503930.

## Permanent absence

The authoritative store contract distinguishes ErrNotFound and committed
tombstones from transient read failures. Missing/deleted requests now receive
one Reject(false) into matching.dead instead of restarting the consumer until
delivery-limit exhaustion. Wrapped not-found errors are recognized. Reject
failures propagate; other read/handler errors remain unacknowledged. Unknown
outcomes are never converted to absence.

Unit red output is in missing-red.log. New unit checks cover not-found,
wrapped-not-found, deleted records and reject transport failure. A live real-FSM
empty store produces one x-death rejection from matching.requests with count=1;
no consumer restart is requested. Existing failed-read/handoff, terminal stale
hint, malformed-message and interrupted-delivery assertions remain intact.

## Routed domain events

The new bounded durable single-member quorum queue matching.wakeups binds the
three planned relief.events patterns: resource.registered.*, match.* and
request.status.*. It uses the same safety arguments/policy, overflow limits and
DLX as matching.requests. Existing queue arguments are unchanged; only the
matching-safety policy pattern expands to include this new queue. Helm and
runtime definitions remain identical.

The live test first reproduced `mandatory return: NO_ROUTE` (events-red.log).
It now confirms and receives each of the three routes with unchanged body and
persistent mode. Unknown routing keys remain unroutable; the relay must keep
their outbox rows pending. No discard consumer or expiry was added. Processing
resource-registration wakeups remains A2.4; the queue retains that work until
then and applies visible backpressure if its bounds are reached. SSE subscribers
remain separate per-API temporary queues for A4.1.

## Verification and test observation

The first full run passed repository/chaos race tests, one-rejection handling
and all three mandatory routes. The new fast domain test sampled management
statistics at 0.83 seconds, before policy metadata appeared: Policy empty and
Effective map[]. The existing real 5-second TTL test already observed it later.
With Parth's authorization, the helper now awaits metadata presence inside the
same original 30-second context, then runs the unchanged exact safety/quorum/
TTL/routing assertions. No sleep, new budget, weakened assertion or skip.

Fresh command output in make-check.log:

```
GITHUB_ACTIONS=true make check
0 issues.
A1.3 VERIFY: ALL PASS
```

All repository and chaos-tag race packages passed. Integration-tag go vet and
golangci-lint passed (0 issues). make gen-check and go mod tidy -diff were clean.
No new frozen contract or S-owned source edits: the merge brings only the
already-merged #60 correction from main. #62 still owns its OutboxQuery rebase.

Follow-up #69 tracks transient reconnects consuming RabbitMQ's delivery limit
separately from business retryCount, for A2.2. These fixes do not claim that
broader retry recovery or A1.5 is complete. S review, CI, human merge and fresh
main Verify remain required.

# Durable workflow recovery [A2.4]

The matcher process runs a periodic worker and a separate manual-ack wakeup
consumer alongside its relay and request consumer. Both replicas use committed
CAS/claim/closure rules. The workers run in the existing matcher pods with
`reliefgrid.io/coordinator-client: "true"` for the coordinator NetworkPolicy.

Each periodic step scans at most 32 rows per participating shard and visits at
most 16 rows. Unvisited page tails are retained before advancing an opaque
cursor. Missing, duplicate, failed or partial shard results cannot complete a
walk. A ring-version mismatch restarts the walk. Work has a 15-second budget,
remote calls a 2-second bound, and passes run every 2 seconds with failure
backoff capped at 30 seconds. Actual convergence also depends on cluster health
and the number of pages; this is not an unbounded-load latency guarantee.

PENDING and due WAITING work retries matching. MATCHING work resumes the exact
intent, or fences an intent at least 30 seconds old into RETRYING using its read
snapshot and immutable fingerprint time. Future retry metadata cannot hide an
old intent. RETRYING closes the exact attempt before returning to WAITING.
CANCELLING/FULFILLING close the exact attempt before terminal commit. Historical
operation results are never used as evidence of current resource ownership.

Resource hints reread current availability, then filter nearby (50 km),
compatible WAITING requests and durably shorten at most 16 schedules. The bounded
walk rejects incomplete negative results; periodic due scans are the fallback.
Request and match hints reread current state and help cleanup. Duplicate events
are scheduling hints, and do not grant ownership or bypass committed checks.

`make verify-A2.4` runs the in-memory M8 and pinned-broker acceptance tests.
`make verify-A2.4-cluster` resets the local kind demo cluster, kills a test worker
at 13 committed boundaries, restores production matcher replicas and checks
public cancel/fulfil, a future waiter wakeup, exact closure, late CLOSED claims,
a complete 60-second workflow age audit and strict zero-backlog invcheck.
For a just-deployed current build, use `bash scripts/verify-A2.4-cluster.sh
--existing`. The test worker uses real coordinator mutations with pause hooks
in its own image; production images contain no fault flags.

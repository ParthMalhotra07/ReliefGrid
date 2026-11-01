# A2.4 — tested implementation checkpoint (not PR-ready)

Implemented periodic paged recovery, exact-snapshot old-attempt closure,
resource-triggered durable wakeups, schema-validated manual-ack wakeup consumption,
confirmed fixed-TTL wake transport retries, dead-queue alerts and controlled
single-event corrected replay. Recovery and wakeup workers run in existing
labelled matcher pods. No storage contract or S-owned implementation changes.

Actual checks: full make check (both lint runs 0 issues, ordinary/chaos race
suite), generation and the pinned live broker verifier pass. The live broker
kept delivery-limit20 and recovered after 25 failures / 25 confirmed deferrals,
with business retryCount1. It also proved wake TTL returns to the wakeup queue,
poison rejection and corrected routed replay before original acknowledgement.
Red-first and passing output are retained, including partial-scan protection,
37-record bounded paging, cleanup closure/late-claim fencing, immutable old-claim
age, concurrent new-intent protection, exact unknown wake retry and future-hint
clamping. Schemas are byte-identical copies of frozen contracts.

Outstanding before final PR: deploy the new matcher, real-cluster kill/restart
at workflow commit boundaries, public cancel/fulfil convergence and bounded-age
checks; final focused race count3, full verification and pre-PR review READY.
This checkpoint is for inspection and is not an assertion that A2.4 is complete.

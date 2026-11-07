# Application priority aging (A4.4)

Requires storage seam #150 on every replica before enabling a producer. The app
chart defaults `app.agingInterval` to `0s` (disabled). After the storage rollout,
set `app.agingInterval=1m` for a one-minute interval, or choose another positive
duration of at least1ms. The matcher receives AGING_INTERVAL; zero disables it.
This is explicit operator policy, not a hard-coded course-plan interval.

The reconciler uses its existing strong paged scans and bounded batches. Before
skipping a future NextAttemptAt, it asks the workflow helper to conditionally age
an unassigned PENDING/WAITING request. Original urgency stays unchanged. Intent,
claim, generation and retry-count decisions remain inside the storage command.

The proposer advances at most one due priority boundary per pass (a later
persisted UpdatedAt may span several). Timestamp is max(persisted UpdatedAt,
the next CreatedAt-based boundary), never the sampling clock. The read version,
interval and timestamp derive stable operation/event IDs. All worker replicas and
restart retries therefore reproduce the same unresolved command bytes, even after
the clock crosses another aging boundary. Already-capped requests emit no new
aging command. A later pass may raise HIGH to CRITICAL.

The seam atomically persists effective urgency and the immediate request.retry
event on relief.requests, shortening NextAttemptAt to the frozen proposer time.
The existing matching.requests binding receives it immediately. Current state is
reread after OK or a version conflict before ordinary recovery/matching proceeds.
Unknown outcomes remain errors; the same version reproduces the same proposal.

Subsequent workflow retry events use max(original,effective) urgency. Transport
redelivery preserves original event bytes rather than rewriting historical hints.
The existing publisher maps numeric urgency to AMQP priority. Metrics expose the
new command using the fixed bounded label `age`.

Verification uses the real M8 FSM, including exact threshold, future backoff,
unknown retries across clock boundaries, and priority preservation. A live pinned
RabbitMQ test relays both committed events with routed confirms, observes the
immediate priority2 wake before the5s retry tier, and marks only after confirms.
No kind acceptance is claimed until the new storage binary is rolled out.

Run `make verify-A4.4-aging` for the bounded real-FSM and disposable-broker checks. Set A44_AGING_EVIDENCE_DIR for a chosen output directory; CI invokes the same Verify from make check.

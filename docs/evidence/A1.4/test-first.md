# A1.4 acceptance plan

Relay pending rows from each owning shard using strong one-second long polls,
at most 100 rows per batch. Publish persistent messages with mandatory routing
and publisher confirms; only routed confirms authorize an idempotent mark batch.
An unknown mark response retains the exact operation ID, timestamp and event IDs.
Restart may republish; consumers reread durable state before acknowledging.

Tests use the real three-shard in-memory FSM, with transport fault wrappers.
Cover confirm-before-mark, failed reads, partial publish failure, an unroutable
row followed by routable work, 100-row boundaries, owning-shard grouping and
lost mark responses. Live acceptance stops the disposable pinned RabbitMQ
broker mid-batch, restarts it, and verifies recovery, mandatory returns and
duplicate-safe workflow handoff. No Raft-cluster or application e2e claim.

Dependencies: exact A1.3 head 16ed353 and S-owned read-options PR #62 at
307615f, integrated without manually editing S code. Parth explicitly authorized
starting A1.4 and continuing after the failing-test checkpoint on 30 October.
Human review/merge and post-merge verification remain required.

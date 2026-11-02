# C13: drain committed handoffs after pausing admission

The previous live audit failed because stopping matcher pods stops their embedded
relay too. One last committed event could remain pending. Do not equate a quiet
matching queue or one empty read before shutdown with a completed handoff.

The bounded demo helper reuses production outbox.Step and confirmed mandatory
publishing while all admission workers remain stopped. It does not consume or
acknowledge a request hint, alter workflow state, or bypass deterministic per-shard
batch marks. It must finish successfully before the unchanged zero-backlog audit.
Queued hints may remain durable after this drain; no queue purge is performed.
Broker credentials are child environment values, never command arguments/evidence.

Test first: the audit-order regression failed without the drain (red.log).
With the correction it passes (green.log), including restoration after audit failure.
Full make check, static A3.1 Verify and generation passed (check/static/gen.log).
The exact failed live output is retained in failed-live.log and attempt4 evidence.
Two complete clean live rounds remain required before the goal is READY.

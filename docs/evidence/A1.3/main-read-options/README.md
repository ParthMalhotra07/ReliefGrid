# Merged read-options integration — 31 October 2026

#62 merged as abfaede after A review, seven green CI jobs, current-main
integration checks and fresh post-merge verification. This branch incorporates
that merged main, including the owner's #68 storage correction.

The consumer test's old PendingOutbox(ctx, shard, 100) call failed to compile
against the merged interface (red.log). The mechanical adaptation uses
OutboxQuery{Shard: shard, Limit: 100}, preserving the same read and assertions.
No frozen contract or S-owned source was hand-edited.

Fresh GITHUB_ACTIONS=true make check passed ordinary/chaos race packages,
both lint runs (0 issues), and pinned RabbitMQ 4.3.7 live A1.3 Verify. Tagged
vet/lint, generation and module checks passed. See make-check.log.

The disposable-broker verifier also incorporates the already-tested A1.4
port-discovery correction: observe valid current loopback mappings inside its
existing readiness budget. The prior delayed-port control evidence is retained
in review-fixes/delayed-port-discovery.log. No assertion, timeout, event rule or
invariant was relaxed. Hrishabh re-review and main goal verification remain.

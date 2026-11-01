# A1.4 owner review fixes — 31 October 2026

Same-shard, same-event batches now sort their IDs and derive the mark UUID and
integer timestamp from immutable committed rows. Two relay instances with
reversed snapshots apply byte-identical commands to the real FSM, including a
deduplicated second application. Different batches retain different identities.

Publish nacks and invalid payload/urgency validation are definite per-row
failures: later valid rows may publish and only their routed confirms are marked.
Malformed rows stay pending for operator/source repair. A full first page of
permanent failures can still block later pages; no poison row is silently dropped
or marked without publication. Paging/quarantine is a separate storage handoff.

The new regressions failed before implementation (test-first.log), then passed
(regressions.log). GITHUB_ACTIONS=true make check passed, including both live
broker verifiers; gen-check left generated code unchanged; go mod tidy -diff was
empty. check.log and gen-check.log contain the actual output. No frozen contract,
FSM, Raft, broker policy or workflow rule changed.

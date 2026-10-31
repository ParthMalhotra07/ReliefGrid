# A1.3 messaging acceptance plan

Branch: a/A1.3-messaging-topology, from main 904aaee.

This is an early review draft. No topology or consumer implementation is
claimed by this commit. A0.3 was rerun on this branch: 100 confirmed messages
survived a broker pod restart on the same PVC and all priority probes passed.

## Acceptance checks

- Declare relief.requests (topic), relief.retry (direct), relief.events (topic)
  and relief.dlx (fanout). The durable matching, three retry and poison queues
  use quorum with an explicit initial group size of one.
- Bind matching.requests to request.created.* and request.retry. Bind the three
  fixed TTL queues to delay.5s, delay.30s and delay.120s. Their dead-letter
  destination is relief.requests with routing key request.retry.
- Commit and deploy policies for at-least-once dead-lettering and
  overflow=reject-publish. Matching poison messages route to matching.dead via
  relief.dlx. Keep broker delivery limits separate from business retries.
- Test the real 4.3.7 broker in isolated test vhosts: publish a persistent
  priority-3 message to relief.retry/delay.5s, receive it after expiry from
  matching.requests, and verify body, priority, routing key and x-death reason.
- Inspect live queue policies and arguments for every tier. Test malformed
  event rejection through the actual consumer into matching.dead.
- Use manual ack and per-consumer prefetch 8. Load current request state through
  store.Store before handling a valid event. Tests use the real SQLite memstore.
- Verify terminal/no-op events are acknowledged and valid work is acknowledged
  only after the handler's durable completion. Storage or handler errors leave
  the delivery unacknowledged and stop this consumption attempt; no nack loop.
- Verify bounded context cancellation and no leaked consumer goroutines. The
  workflow's business retry state machine remains A2.2, relay remains A1.4.

## Verification

`make check`, `bash scripts/verify-A1.3.sh`, `make gen-check` and
`go mod tidy -diff`. The goal Verify must fail if a broker is unavailable; it
does not silently skip the live acceptance tests. Normal unit tests remain
independent of a running broker; integration tests use a separate build tag.

No contracts or S-owned files are to be changed. Local playbooks, progress,
prompt files and the plan PDF are excluded from staging.

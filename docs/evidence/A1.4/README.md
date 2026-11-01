# A1.4 outbox relay — 30 October 2026

Acceptance uses three real in-memory FSM shards and the exact RabbitMQ image
pinned in the app Helm values:

```
rabbitmq:4.3.7-management@sha256:57bcedccc5c75e55ac33e5f3eaeb28529317b5a02fe5e2722a025f2674c8063e
```

## Verification

`GITHUB_ACTIONS=true make check` passed: whitespace/gofmt, vet, lint (0 issues),
repository race tests including chaos, and live A1.3 plus A1.4 verification.
`make-check.log` is the full final output. The additional integration-tag vet
and lint also passed (0 issues), and generation produced no generated-code diff.

Selected actual acceptance output:

```
mandatory return + broker ack leaves row pending; same session routes retry; only routed confirm marks
broker stopped after first routed confirm: marked=1 pending=2; after restart marked=2 received=3 persistent
confirm-before-mark process loss: 3 events replayed as 6 deliveries, 3 durable retry handoffs, retryCount=1 each
A1.4 VERIFY: ALL PASS
```

Nine relay tests cover exact mark retry after a committed response is lost,
confirm-before-mark, partial transport loss, mandatory return, strong bounded
long-poll options, failed reads, batch boundaries and owning-shard grouping,
independent shard workers and cancellation. Three live cases verify broker
restart/persistence, mandatory returns, and harmless duplicate workflow handoff.
Publisher urgency tests cover public strings, internal retry integers, explicit
NORMAL defaults and invalid values.

The real coordinator client also ran against an HTTP adapter backed by the
actual FSM. It sent `wait=1`, `consistency=strong`, `limit=100` and an owning-shard
batch mark. This is wire integration, not a claim of a real Raft-cluster run.

## Review and scope

Reviewed invariants, unknown outcomes, idempotency, context cancellation, mark
batch identity, goroutine cleanup, scope and hygiene. No claim/intent logic or
frozen contract changed. A1.3 is the temporary PR base; approved S-owned #62 at
307615f is a merge parent. All S-owned source remains byte-identical to that
reviewed head; the one new MQ test call mechanically adopts `OutboxQuery`.
There are no local-only files in the commit set. A1.4 remains unticked until
human review/merge and fresh main verification.

Limitations: one disposable single-member quorum broker is one failure domain.
Application images, Helm deployment and real-cluster/Traefik e2e remain A1.5.
`relief.events` has no subscriber binding yet and therefore remains pending;
100 permanently unroutable rows can fill a page and backpressure newer rows.
The relay does not discard these rows to bypass the backlog. Business cleanup,
state-aware recovery and reconcilers remain A2.2/A2.4.

## Correction history

`red.log` records the initial missing production package. `first-check-failed.log`
records the stale Docker host-port harness failure; `STOP.md` records the cause
and explicit approval. `port-correction-green.log` records all live cases passing
before the passive-inspection lint fix and the final entrypoint review fix.

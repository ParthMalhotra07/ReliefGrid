# ADR005 — Transactional outbox and durable retry handoff

Status: implemented core decision, included in the reviewed #137 report core;
compact submission text and final layout retain their own editorial review.

## Context and decision

Committing state and then independently publishing to RabbitMQ can lose work if
the process crashes between them. Commit the domain change and outbox event in
one FSM transaction. A bounded per-shard relay publishes persistent messages with
confirms and mandatory routing, then commits the published mark. Consumers use
manual acknowledgements and acknowledge only after their necessary durable state
or retry-handoff event is committed.

Batch mark operationId and timestamp are deterministic for the shard/event set,
so concurrent relay replicas do not reuse a random operationId with incompatible
content. Delayed retry queues keep transient recovery out of an immediate nack
loop. Reconciliation is a backstop, not a substitute for the outbox transaction.

## Alternatives and cost

Publish-before-commit can expose uncommitted work; commit-before-best-effort-publish
can lose it. Acknowledging a delivery before durable handoff loses recovery on a
consumer crash. The selected design tolerates duplicates after publish/mark races
and requires idempotent handlers, more persisted rows, scans and retry traffic.
It does not claim exactly-once transport delivery.

Assumptions: a functioning per-shard commit path, durable quorum queue configuration
and eventual recovery of broker/consumers. An unroutable publish is not a routed
confirm; an unavailable/partial outbox scan is not an empty successful result.

## Falsification and evidence

Stopping the broker mid-batch must leave unconfirmed events pending. Lost confirms,
mandatory unroutable messages and a consumer killed before durable handoff must
not disappear after restart. See [relay tests](../../internal/outbox),
[broker integration tests](../../internal/mq), [outbox evidence](../evidence/A1.4/README.md)
and [reconciler tests](../../internal/reconcile). A mark before routed confirmation
or an ack before durable handoff would falsify the intended safety boundary.

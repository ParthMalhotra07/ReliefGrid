# Transactional request aging

`Store.AgeRequest` and `POST /internal/requests/{id}/age` are the S-owned seam
for A4.4 (#135). A must add bounded reconciler scanning and proposer policy;
this PR does not enable that loop or complete A4.4.

The command carries operationId, positive Unix-millisecond at, expectedVersion,
positive agingIntervalMS and one proposed outbox event. Coordinator routing also
requires ringVersion. In committed apply, version must match and the request must
be PENDING or WAITING, without currentAttempt or assignedResourceId.

Original urgency is preserved. Absent effectiveUrgency means original urgency.
The target is original urgency plus whole intervals since CreatedAt, capped at 3.
Priority never decreases. UpdatedAt and retry count do not determine aging. An
already-achieved target returns committed OK with the existing version and no
new event; its operation result is still deduplicated.

A strict increase atomically writes effective urgency, the new entity version,
timestamps, one outbox event and the operation result. **Workflow-timing change:**
nextAttemptAt becomes at so the new event can wake waiting work. Status, intent,
assignment, workflow generation and retry count are preserved. Because aging
advances the entity version, an older public If-Match gets 412 under #136.

**Pending Parth's topology agreement:** the proposed request.retry event goes
directly to relief.requests with routing key request.retry, using the existing
matching.requests binding instead of a delay queue. The route is defined in one
constant declaration, store.AgingWakeExchange/AgingWakeRoutingKey. The proposer
must carry effective urgency in the retry payload; subsequent A-owned workflow
producers must preserve that priority. The existing publisher reads payload
urgency to set AMQP priority. Duplicate deliveries still reread current state.

Retain the exact operationId, at, interval and outbox payload on an unknown
outcome. A replay returns the historical result without changing current state,
even after cancellation. After a definitive VERSION_CONFLICT, reread and form a
new operation rather than changing the payload under an old operationId.

The interval is explicit policy supplied by the proposer. Changing policy cannot
lower already-persisted priority. There is no clock, random decision or local
configuration dependency inside apply. Positive timestamps permit safe age
subtraction; interval counts are capped before addition.

The internal contract is additive and requires both owners' approval. Older
binaries do not understand the new command: upgrade all storage replicas before
any producer starts issuing AgeRequest. This change adds no public input field.

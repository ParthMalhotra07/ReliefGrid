# Application messaging

`Dial` opens a context-owned connection with bounded TCP/AMQP setup. `Declare`
idempotently declares the embedded definitions on that connection. Use separate
connections for publishing and consumption; a stalled RPC may close its owning
connection to enforce the deadline. Automatic client recovery is disabled.

The app chart mounts the same definitions. Its startup hook waits on the
container-local RabbitMQ PID file, then imports the queues, bindings and policies.
This runs after ordinary broker startup so the configured user is initialized
on a new disk. The definitions contain no users or credentials. The checksum
annotation rolls the pod when definitions change.

Topology: relief.requests (topic), relief.retry (direct), relief.events (topic)
and relief.dlx (fanout). matching.requests accepts request.created.* and
request.retry. retry.5s/30s/120s have fixed queue TTLs and override the expired
message route to relief.requests/request.retry. matching.dead is the poison sink.
matching.wakeups durably accepts resource.registered.*, match.* and
request.status.* from relief.events, separately from request-ID work. All six
durable queues explicitly request quorum with one initial member.

Source queues have at-least-once dead-lettering, reject-publish overflow,
10,000-message/64-MiB bounds and a broker delivery limit of 20. Those bounds also
apply to matching.dead, which rejects overflow rather than dropping its head.
Safety arguments accompany the named policies so runtime declaration without
policy import cannot silently downgrade delivery. Changing immutable arguments
requires a queue migration. NORMAL/HIGH/CRITICAL publishers use explicit 1/2/3;
the pinned broker treats an omitted priority as 4.

`Consumer.Process` validates the supported event envelope and request identity,
then loads the current request through store.Store. It uses the frozen
request.created schema (a byte-identical embedded copy checked in tests) and
A1.2's existing internal request.retry shape. Duplicate/case-aliased properties,
unknown fields and trailing JSON are rejected. Payload data is never authority
for a claim. Malformed work is rejected without requeue; warnings and RabbitMQ's
x-death headers expose the reason and source queue.

Terminal/already-matched work is acknowledged after the authoritative reread.
Authoritative ErrNotFound (including wrapped errors) or a committed tombstone
is rejected once without requeue into matching.dead. Reject errors propagate;
transient read failures remain unacknowledged and are never treated as absence.
For other work, the handler must return nil only after its durable handoff
commits or an authoritative reread proves no action remains. An unknown outcome
must remain an error unless a durable state-preserving retry handoff commits.
Read/handler errors use the optional Retry callback below; without it they remain
unacknowledged.

`Consumer.Consume` uses one dedicated channel, manual acknowledgements and
prefetch 8. It owns channel cancellation and closes that channel on failure or
shutdown, returning unacknowledged work to the broker. It deliberately avoids
the library's concurrent Cancel RPC during shutdown. Callers must back off
before starting another consumption attempt; this package does not run a
reconnect loop. Call and work budgets default to 5 and 15 seconds; handlers must
honor their context.

Run `make verify-A1.3` or `bash scripts/verify-A1.3.sh`. By default the verifier
creates a disposable broker with the exact tag/digest in Helm values, exposes
loopback ports, tests isolated vhosts, then removes only its own container.
Alternatively provide MQ_AMQP_URL and MQ_MANAGEMENT_URL for a dedicated test
broker with administrator access (test vhosts are created/deleted). Deleting a
vhost can reset AMQP sockets and kill a shared kubectl port-forward, so direct
ports are used by default. Broker absence is a failure, never a skipped test.
CI's existing make check invokes this verifier via the shared Makefile when
GITHUB_ACTIONS=true. Unit tests alone need no broker.

Limits: one durable broker is one failure domain, not broker-node redundancy.
State-aware business retries are implemented in A2.2; the outbox relay is A1.4,
and matcher process/Helm application integration remains A1.5. matching.wakeups
retains domain events for the A2.4 resource-registration wakeup handler; that
handler is not implemented here. This queue has the same bounded backpressure
and dead-letter safety as matching.requests, with no expiry or discard consumer.
SSE still needs its own per-API temporary subscribers in A4.1. Unknown routing
keys remain unroutable and must stay in the outbox. Poison inspection/replay
tooling and periodic due-work repair remain later work.

## Confirmed delayed transport recovery [A2.2]

Production configures Consumer.Retry with TransportRetry. If an authoritative
read or domain handoff fails, it republishes the byte-identical original work
hint persistently and mandatorily to relief.retry/delay.5s, using a separate
bounded publisher connection. Only a routed positive confirm permits source
acknowledgement. A nack, return, cancellation, unknown confirm, or failed ack
returns an error; the consumption channel closes and the caller backs off before
reconnecting. With no Retry callback, the original unacknowledged behaviour stays.

The transport copy is a durable scheduling hint, not proof of current ownership
or a successful domain mutation. Its event ID, body and derived urgency remain
unchanged. It does not forward broker death/delivery headers; those describe a
delivery's transport history and do not increment persisted business retryCount.
Logs distinguish retryClass=transport, brokerDeliveryCount and brokerDeaths.
This allows more than 20 healthy transient failures without changing the broker's
poison delivery limit. Invalid and permanently absent work still dead-letters once.
A lost confirm may leave both copies: current-state/due-time rereads make them safe.

make verify-A2.2 runs unit fault cases plus live tests on the pinned broker.
The new integration,a22 tag isolates the 25-hop acceptance test (at least 125s
at the fixed 5s TTL) from A1.3's existing integration tests. The shared CI Makefile
runs both verifiers. Existing integration tests retain their 30s budget; the new
25-hop test has a measured-path 180s budget. This is transport recovery; periodic
durable workflow scans and wakeup consumption still belong to A2.4.

# Transactional outbox relay

`cmd/matcher` requires `COORDINATOR_URL` and `AMQP_URL`. It runs one publisher
worker per coordinator shard and a separate matching consumer connection.
Each worker requests a strong outbox page with `wait=1`, at most 100 rows.
Persistent mandatory publishes are serialized on that worker's confirm channel.
Only a routed positive confirm enters the owning shard's mark batch.

An uncertain mark retains the exact operation ID, timestamp and event list for
retry before another read or publish. These pending commands are process-local;
a crash can cause retransmission of still-pending rows. The event ID and body
stay unchanged. The consumer rereads current state and the workflow's committed
handoff/deferred schedule makes these duplicates harmless.

Mandatory returns/nacks leave the event pending and allow later rows in the
same page to proceed. Transport loss stops that page's publishes. Confirmed
earlier rows can still be marked. Failures reconnect with exponential backoff
from 100 ms to 5 s; even empty in-memory polls wait 100 ms to avoid spinning.
Uncertain operations never become rejection. A committed mark rejection stops
the process for diagnosis rather than silently dropping its batch.

`make verify-A1.4` creates a labelled disposable broker using the exact pinned
Helm image, stops/restarts only that container and removes its own data at exit.
Tests use the real three-shard in-memory FSM. This does not establish real
Raft-cluster or Traefik application e2e behavior; A1.5 owns that integration.

`relief.events` has no subscriber queues yet. Events on that exchange remain
pending on mandatory return until their consuming goal installs bindings.
At least 100 permanently unroutable rows can fill a page and backpressure newer
rows: operator/subscriber recovery is required; the relay never marks or silently
discards them to bypass the backlog. Reconciler/business retry policy is A2.2/A2.4.

Protocol reference: [RabbitMQ publisher confirms](https://www.rabbitmq.com/docs/confirms).
RabbitMQ sends a mandatory return before the associated ack/nack. The pinned
amqp091-go v1.16.0 `channel.go` dispatch enqueues returns synchronously before
resolving deferred confirms. The publisher has exactly one outstanding message
and checks the buffered return before treating a positive confirm as routed.

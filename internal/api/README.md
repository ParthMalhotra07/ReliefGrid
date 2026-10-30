# Public API (A1.1)

`cmd/api` serves the nine operations in `contracts/public.yaml` under `/api/v1`.
The handler uses `store.Store`; production connects through `coordclient`.
Tests use the real three-shard SQLite FSM via `memstore` and its fault wrapper.

Run locally against a coordinator:

```sh
COORDINATOR_URL=http://127.0.0.1:8081 CALLER_NAMESPACE=local-dispatcher go run ./cmd/api
```

`HTTP_ADDR` defaults to `:8080`. `BUDGET_MS=5000` caps ordinary requests;
`CALL_TIMEOUT_MS=4000` caps each store call and cannot exceed the request budget.
`/healthz` and `/readyz` describe process initialization, not availability of
every shard. Dependency failures are reported on the affected routes.

## Durable create replay

An optional `Idempotency-Key` must be a 36-character UUID. Its namespace includes
the trusted dispatcher and the creation operation. Normalized client data is
hashed without regenerated IDs, timestamps or trace metadata.

A create-only generic KV record (`api-create:<deterministic UUID>`) freezes the
payload hash, complete command, original response and Location. The existing
FSM CAS serializes racing proposals. The API then submits the frozen
CreateEntity command, with the same operation ID and payload on every retry.
Its domain record and outbox event commit atomically through the existing FSM.
Only a committed OK result permits a 201 response; the metadata record alone
never proves creation. Unknown outcomes return 503 and retain the intent.

A retry after entity edits or an API restart returns the original 201 body
and Location. A changed payload returns 409. The metadata and stored operation
results live for the project dataset lifetime; there is no process-local
idempotency cache or cleanup TTL. Without a key, retries may create duplicates.

The metadata key can own a different shard from the entity. This is an
intent-first protocol, not a cross-shard transaction. A crash between its two
commits is resumed by a keyed client retry; an uncompleted metadata record
does not start matching or acknowledge a created request. Both crash windows
are tested. Do not delete or mutate these metadata records independently.

`CALLER_NAMESPACE` is operator-controlled for a single trusted local dispatcher.
It is not accepted from a client header. A multi-user/public release needs
authenticated principal-to-namespace routing; this goal does not implement an
account system or make an unauthenticated caller header authoritative.

## Workflow and event boundaries

Registration computes the full geography index used by the shared matcher.
Explicit matching calls `workflow.Matcher`; a definitive NO_CANDIDATE requires
its complete authoritative scan and committed retry handoff. Future scheduled
work and durable cleanup return 202; a pending request without durable progress
does not. A terminal request cannot restart matching.

Status routes use current versions and the FSM's conditional commands.
Cancellation/completion with an attempt enters CANCELLING/FULFILLING and retains
the intent and chosen resource disposition. The A2.1 worker/reconciler performs
closure before finalization. This goal does not pretend that cleanup is already
complete. Terminal public responses show a null current assignment, while
storage can retain historical audit fields. Public resource updates cannot
replace an active claim.

Create and status events use the frozen envelopes. Resource registration is on
`relief.events`, not the outdated `relief.resources` internal example (#42).
No direct RabbitMQ publication or delivery acknowledgement is introduced here.

Authoritative lists reject failed/partial scans. They are bounded operational
views, not global snapshots or public cursor walks (A4.4). Near searches apply
the geohash cover and exact Haversine filter. `hasMore` is conservative if the
public limit truncates a merged page; it is not a promise of a globally complete
list. Work is bounded by a deadline and at most 32 internal pages.

## SSE and deployment

`/events` is a heartbeat-only projection stub. `STREAM_TOKEN`, if configured,
requires Bearer access and distinguishes 401 from 403. The stream checks request
cancellation and bounds each write; it does not publish event IDs or promise
event history. RabbitMQ-backed projections and the UI follow later.

Images, umbrella-chart app wiring and the real cluster create/register/match
smoke test remain A1.5. Read-option issue #41 is needed for the deployed A1.4
long-poll relay; this API uses the supported strong defaults.

## Verify

```sh
go test -race ./cmd/api/... ./internal/api/...
make check
bash scripts/verify-A1.1.sh
make gen-check
```

The Verify additionally needs Python 3 with `jsonschema` (also used by A0.1).
It validates four actual API outbox envelopes against the frozen JSON Schemas,
in addition to real-FSM fault/race tests and process HTTP startup/shutdown.

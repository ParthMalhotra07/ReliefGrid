# A0.3 — RabbitMQ / Helm spike

## Reproduce

Use the pinned tools from `tools.mk`, with Docker available and the five-node
development kind cluster reachable. Run from the repository root:

```sh
make doctor
bash spike/mq.sh
```

The defaults target **kind-reliefgrid**, namespace/release **reliefgrid**. This
is a development-only verifier: it installs/upgrades the umbrella chart, purges
only `reliefgrid.spike.durable`, publishes test messages, and deletes/restarts
the broker pod. It does not delete the cluster, namespace or PVC. Do not point
the overrides at a production cluster or a shared non-test queue. The release
and its PVC are deliberately left running for inspection.

RabbitMQ is `4.3.7-management`, pinned by tag and digest in the chart values:

```text
rabbitmq:4.3.7-management@sha256:57bcedccc5c75e55ac33e5f3eaeb28529317b5a02fe5e2722a025f2674c8063e
```

The client is `github.com/rabbitmq/amqp091-go v1.16.0`. The thin umbrella chart
locks Traefik chart `42.1.2`, with `versionOverride: v3.7.16` and image digest
`sha256:1336e85aaed293bd63d005086e0dd013b04078b26aaeb1c18b26a2f928af7e65`.
The version is the locked chart's appVersion; the digest was resolved from the
official multi-platform image index. The override is at `traefik.versionOverride`,
alongside `traefik.image.digest`, as this exact chart requires.
RabbitMQ runs on the infra worker, backed by a 1 GiB
PVC. The chart's username/password and Erlang cookie are public, fixed **local
test defaults**, not deployment credentials; do not reuse them outside this spike.

## Persistence experiment

The publisher declares a durable direct exchange, durable quorum queue and
binding, then publishes 100 persistent messages with mandatory routing and
publisher confirms. A return or negative/missing confirm fails verification.
The verifier waits for confirms before deleting only the StatefulSet pod. The
replacement reuses its existing PVC; the verifier compares the PVC UID before
and after restart. The consumer uses manual acknowledgements
and prefetch 8, checks persistence and rejects duplicate IDs.

Observed after the PVC-preserving restart:

```text
confirmed=100
received=100
```

## Priority experiment on this exact broker

All probes enqueue and confirm every message before starting a consumer with
prefetch 1. Each uses a separate quorum queue and deletes that test queue when
finished. The expected sequences are independent literals; the verifier fails
on any different sequence. Identity, body and persistence are checked too.

| Case | Published priority fields | Observed delivery fields |
| --- | --- | --- |
| Explicit nonzero | 1, 7, 31, 15, 2 | 31, 15, 7, 2, 1 |
| Application urgency | 1, 3, 2 | 3, 2, 1 |
| Omitted-property case | 1, 0, 7 | 7, 0, 1 |

Important distinction: in the pinned client's `write.go`, the priority-property
flag is set only when `Publishing.Priority > 0`. Its zero value therefore omits
the property; it does **not** encode explicit wire priority zero. The second
observation is consistent with the broker assigning absent priority an effective
value of 4. A delivered field of 0 still represents the absent property, not a
rewritten header. This spike does not claim to test explicit wire priority zero.
RabbitMQ documents the absent-priority default and strict quorum priorities
since 4.3 in its [priority guide](https://www.rabbitmq.com/docs/priority).

The application mapping is **NORMAL=1, HIGH=2, CRITICAL=3**. Every application
publisher must set the field explicitly. Zero is prohibited for application
urgency: amqp091-go omits it, and the broker's effective default of 4 would put
such messages ahead of CRITICAL=3. The persistence publisher now explicitly
sets NORMAL=1 and the consumer checks it after restart. Only the deliberate
omitted-property probe publishes zero. The independent urgency probe verifies
the exact delivery order `[3 2 1]`; the other strict expectations are retained.

The initial assertion incorrectly sorted Go zero as an explicitly transmitted
lowest priority and failed with `[31 15 7 0 1]`. After Parth's approval the probe
was split into the two strict cases above; it does not accept arbitrary ordering.

## Cancellation regression

The original custom DialContext bounded only TCP establishment. A silent peer
could then block the AMQP handshake after the context was cancelled. The new
test waits until a fake peer receives the AMQP protocol header, cancels, and
requires open to return an error. It failed before the fix. A context callback
now closes the underlying transport during handshake or channel RPCs; normal
connection close unregisters the callback. The same regression and race tests
pass without sleeps, skips or relaxed assertions.

## Limits

This is a single-broker, single-member quorum development spike. It demonstrates
pod restart and PVC recovery, not broker HA or protection from PVC/node loss.
Priority results apply to queued messages with the tested publication/consumer
order, not preemption or arbitrary concurrent traffic. The consumer has no
application workflow or durable downstream handoff; it is not the A1.3 consumer.
Production exchanges/retry/DLX policies belong to A1.3, and API/matcher images
and routes belong to A1.5. The chart is not a production release.

Historical evidence: `docs/evidence/A0.3/verification-2026-10-23.md`.
The separate cluster/RAM experiment subsequently completed and its evidence
merged in PR #10 under `docs/evidence/spike/ram-DESKTOP-EG9G7UI*`.
The current verifier preserves the cluster, namespace, and PVC.

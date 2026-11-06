# Local application autoscaling (A4.3)

The opt-in kind profile gives the API one CPU HPA (2–6 replicas, 60% of
requested CPU) and the matcher one KEDA-owned HPA (1–8 replicas, four ready
messages per replica on `matching.requests`). Helm omits `spec.replicas` for
these two Deployments while enabled. The UI, broker and storage have no new
autoscalers. With the profile disabled, fixed API/matcher counts remain two,
including the manual scaling demo 9.

From an existing `make demo-reset` + `make app-deploy` deployment:

```sh
make autoscaling-install
make autoscaling-up
make verify-A4.3-static
make verify-A4.3
# Restore fixed replica ownership before running the nine-demo gate:
make autoscaling-down
```

All mutating commands explicitly use `kind-${CLUSTER:-reliefgrid}` and namespace
`reliefgrid`; they never change the current kube context. The app release must
already exist. Its stored image settings and standalone storage ownership are
preserved. No reset, PVC deletion, cloud command or storage installation runs.

`autoscaling-install` verifies SHA-256 checksums for the official metrics-server
3.14.0 and KEDA 2.21.0 Helm archives. The chart's metrics-server default is
overridden with v0.9.1; all four controller images use immutable Linux/amd64
digests in `deploy/helm/autoscaling/` and `tools.mk`. Controllers run on the infra
worker. The local kubelet TLS exception is limited to this kind install helper.
These values are not a cloud deployment profile.

KEDA watches only `reliefgrid`. Its Secret read access is restricted to the
existing `reliefgrid-rabbitmq` Secret in that namespace. TriggerAuthentication
references `amqp-url`; credentials are neither duplicated into chart values nor
recorded in evidence. The scaler uses the private broker AMQP Service.

The RabbitMQ AMQP scaler measures **ready messages**, excluding unacknowledged
deliveries and delayed retry queues. Scale-down has a 30-second stabilization
window for this local demonstration; these thresholds are starting values, not
a throughput or production sizing claim. Short bursts may scale up and down
during an otherwise steady offered load. CPU requests are required for the API
utilization metric. Metrics/KEDA errors must be inspected through HPA and
ScaledObject conditions rather than treated as an empty queue.

`verify-A4.3` downloads a checksum-pinned k6 2.3.0 developer binary and exercises
the real matcher consumer with 20,000 persistent, schema-valid duplicate hints
for one already-cancelled synthetic request. k6 paces 32 workers with a 128 ms
offered-load interval (at most 250 hints/second, plus HTTP time), below the
broker's 10,000-message reject-publish bound as consumers
scale. Consumers must reread authoritative state before acking each hint.
This isolates scaling from persistent business backlog; it is not an end-user
request creation benchmark. No consumer is paused and no replica count is
manually changed to manufacture the result.

Acceptance requires a one-Ready-pod baseline, more than one **Ready** matcher
alongside observed queue load, then one Ready matcher after all 20,000 k6
iterations succeed and both ready/unacknowledged counts drain. Missing/idle
observations, rejected publishes, dropped iterations, replica bounds violations,
restarts, changed fixture state, a growing dead queue or unavailable API CPU
metrics fail the run. The script also checks exactly one HPA per Deployment and
the matcher HPA's controller owner reference. Timestamped observations, k6
output and controller snapshots are retained under `docs/evidence/A4.3/`.

The verifier does not uninstall controllers on exit; use `autoscaling-down` to
restore the course-demo app profile. Metrics-server and KEDA may remain idle.
Concurrent queue producers can invalidate the baseline/drain check; run this
acceptance on the local development cluster without other demo traffic.

Official references: [KEDA RabbitMQ scaler](https://keda.sh/docs/2.21/scalers/rabbitmq-queue/),
[KEDA chart](https://github.com/kedacore/charts/tree/main/keda),
[metrics-server](https://github.com/kubernetes-sigs/metrics-server/releases/tag/v0.9.1),
[k6 2.3.0](https://github.com/grafana/k6/releases/tag/v2.3.0).

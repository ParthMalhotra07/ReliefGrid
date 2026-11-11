# UI broker monitoring account — A6.1 / #142

The chart gives the UI a separate random 40-character password and a monitoring
identity. The application account and canonical queue topology are unchanged.
The monitoring tag permits queue statistics; configure/write/read permissions are
all `^$`, denying resource operations. The UI receives only `ui-management-url`,
never the application AMQP or administrative management credential.

RabbitMQ native boot import installs both identities. Upgrading a pre-change
chart performs one broker rollout to import the account into the existing PVC.
The fixed migration annotation does not restart it on subsequent no-op upgrades.
Helm lookup reuses the existing monitor password; it is not regenerated on a
normal Helm upgrade or pod restart. Render-only Helm template cannot perform
lookup; use Helm upgrade against the cluster for this credential lifecycle.

## Evidence

- [Red regression](ui-monitor-red.log): independent credential was missing.
- [Full check](monitor-check.log), [generation](monitor-gen.log),
  [tagged vet](monitor-tag-vet.log), [tagged lint](monitor-tag-lint.log), and
  [chart checks](monitor-chart.log).
- [Live result](result.json): fresh PVC, existing PVC upgrade, same PVC restart,
  no-op upgrade. Application persistent messages survived. Monitoring HTTP
  statistics returned real numeric counters; HTTP queue/user mutation, AMQP
  consume/configure/default-exchange publish were denied.
- [Upgrade probe](upgrade-probe.txt), [restart probe](restart-probe.txt),
  [fresh-install probe](fresh-probe.txt).
- [Prior observation failure](prior-observation-failure.txt) is retained: broker
  readiness preceded publication of management counters. The verifier now waits
  at most 15 seconds (three default collection intervals) for actual non-null
  counters; missing counters never count as zero. No permission assertion changed.

Reproduce on local kind only:

```sh
REQUIRE_HELM=1 go test -race -count=3 -tags integration -run 'TestAppUIBrokerMonitorIdentity|TestAppBrokerNativeBootImport' ./scripts
python3 scripts/verify-ui-monitor.py --out /tmp/reliefgrid-monitor-unique-run
```

The live verifier uses a new disposable namespace, removes that namespace and
only its PVCs, and never resets the existing ReliefGrid installation. The old
baseline is pinned to main `0fa3a63cbddb283a8ddb5f73b100a3afa967202f`.
Logs contain no generated credentials or Secret manifests.

References: [RabbitMQ access control](https://www.rabbitmq.com/docs/access-control)
and [management roles](https://www.rabbitmq.com/docs/management).
Internal service authentication (#29) remains a separate S-owned handoff.

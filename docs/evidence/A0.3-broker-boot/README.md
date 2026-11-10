# A0.3 follow-up: RabbitMQ native boot import

Base: main b454dc04af617b42e7520df0bb8c44437b6ecb73. Verified 7 November 2026 on the existing kind cluster, in a disposable broker-only namespace with a new PVC.

The chart no longer executes `rabbitmqctl wait` / `import_definitions` concurrently with the image entrypoint through a postStart hook. RabbitMQ imports the existing topology in its own boot sequence. Boot definitions include the configured user and permissions in a projected Secret, because importing definitions on a blank node suppresses default user creation. The public ConfigMap retains only configuration and canonical non-secret topology. The image's existing conf.d files remain mounted.

## Commands and actual output

- Red-first: `go test -count=1 -tags integration ./scripts -run TestAppBrokerNativeBootImport` — failed on both default/custom credential cases: `boot import must not run in a concurrent postStart CLI`. See red.txt.
- `make check` — PASS, vet/lint and all race-enabled Go tests including the chaos build. See check.txt. A fresh uncached run also passed before final review; final rerun uses Go's valid cache.
- `make gen-check` — PASS, generated types unchanged. See gen.txt.
- `go test -count=1 -tags integration ./scripts -run 'TestApp(BrokerNativeBootImport|BrokerOnlyProfileRemainsIndependent|HelmFullDeployment)'` — PASS (4.521s). See chart.txt.
- `go test -race -count=3 -tags integration ./scripts -run TestAppBrokerNativeBootImport` — PASS (5.577s). See chart-race.txt.
- `timeout 600 python3 scripts/verify-broker-boot.py` — PASS. See live.txt. Fresh install: restartCount=0, custom credentials work, canonical queues/exchanges/policies present. Same-PVC replacement: new pod UID, restartCount=0, same PVC UID, topology and a persistent quorum-queue message retained. Only the generated verification namespace was removed.

## Pre-PR review

Final verdict: READY.

1. Invariants / idempotency: no FSM, claim, operation ID, outbox handoff or ack-path changes. Canonical topology is compared in full by the regression. Same-PVC replay preserves an additional durable queue/message.
2. Concurrency: import belongs to the broker boot sequence; no concurrent Erlang CLI in postStart. Verification remote calls have explicit timeouts; port-forward startup is bounded and processes are terminated.
3. Errors: import failures remain broker startup failures; they are not swallowed. Live checks assert exact zero restarts and fail on missing credentials/topology/message.
4. Tests: retained initial red output; no skipped test, relaxed invariant, added sleep or raised production timeout. Replaced an erroneous substring credential check with full public JSON equality, since the default password also occurs in non-secret path names.
5. Scope: app chart plus its regression/verification and compact evidence only. No S-owned code, contract, image or probe policy changes. #129's independent probe changes remain in its owner's PR.
6. Hygiene: final review removed an unintended duplicate config key under metadata. Final checks and live verification ran again afterward. No credentials, local paths, binaries, kubeconfigs or local-only instruction/progress files are included.

## Limits

The reported old-hook failure was not deterministically reproduced here; the red test pins removal of that concurrent hook, and the replacement was verified on fresh and retained PVCs. This is a broker-boot follow-up, not a repeat of the full nine-demo gate or A4.2/A4.3 acceptance. RabbitMQ probe improvements are reviewed separately in #129.

References: [RabbitMQ definition import](https://www.rabbitmq.com/docs/definitions), [pinned 4.3.7 password import implementation](https://github.com/rabbitmq/rabbitmq-server/blob/v4.3.7/deps/rabbit/src/rabbit_auth_backend_internal.erl).

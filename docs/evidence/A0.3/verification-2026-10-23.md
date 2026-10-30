# A0.3 verification — 23 October 2026

Base: `2b8f9f7c6c818bfee3ab27a5b3f4fc0f2f831108` (current main).
Host: Parth's Windows laptop, Ubuntu under WSL, existing `kind-reliefgrid` cluster.
Commands below were run locally; selected real output is reproduced.

## Pinned tools and full checks

```text
make doctor
ok        go             1.27.2
ok        golangci-lint  2.15.0
ok        kind           0.33.0
ok        kubectl        1.37.1
ok        helm           3.22.1
ok        oapi-codegen   2.8.0
ok        docker         daemon 29.7.2
ok        RAM            11.6 GiB available, need 8 GiB (PROFILE=full)
doctor: all tools match tools.mk; Docker, RAM and ports ok

go test ./spike/mq -run TestOpenCancellation -count=1
ok  github.com/ParthMalhotra07/ReliefGrid/spike/mq  0.012s

make check
go vet ./...
golangci-lint run ./...
0 issues.
go test -race ./...
ok  github.com/ParthMalhotra07/ReliefGrid/spike/mq  1.016s

make gen
gen: no contracts yet, nothing to generate

go mod verify
all modules verified
```

All other packages passed (cached) or reported no test files. Generation is a
no-op on this base because the public contract PR remains unmerged. The initial
full check on the outdated branch failed formatting on old storage files; the
branch was fast-forwarded to current main and its checkout normalized to LF.
No S-owned source edits were made. Only the pinned AMQP dependency and its
checksums were added; existing module pins were retained.

## Broker restart and priority

```text
bash spike/mq.sh
STATUS: deployed
REVISION: 5
partitioned roll out complete: 1 new pods have been updated...
confirmed=100
pod "reliefgrid-rabbitmq-0" deleted from reliefgrid namespace
pod/reliefgrid-rabbitmq-0 condition met
received=100
priority=strict:[31 15 7 2 1]
priority=omitted-default-4:[7 0 1]
PASS  A0.3 RabbitMQ restart verification
```

Live StatefulSet image:

```text
rabbitmq:4.3.7-management@sha256:57bcedccc5c75e55ac33e5f3eaeb28529317b5a02fe5e2722a025f2674c8063e
```

PVC `data-reliefgrid-rabbitmq-0` remained Bound, 1 GiB, `standard` storage class,
volume `pvc-7357a892-b5ff-4260-97bd-659ec909d965` (age 14h at inspection).
The verifier also checks infra-node placement and Traefik readiness.

## Chart validation

Using a temporary copy of the chart to avoid checking in vendored dependencies:

```text
helm dependency build <temporary-chart>
helm lint <temporary-chart> --strict
1 chart(s) linted, 0 chart(s) failed

helm template reliefgrid <temporary-chart> --namespace reliefgrid > <temporary-render>
kubectl --context kind-reliefgrid apply --dry-run=server -f <temporary-render>
statefulset.apps/reliefgrid-rabbitmq created (server dry run)
deployment.apps/reliefgrid-traefik configured (server dry run)
```

All 10 rendered resources passed server dry-run validation. Kubectl warned that
Helm-managed resources lack its last-applied annotation; this was dry-run only
and did not change their management or install real resources.

## Regression before correction

```text
go test ./spike/mq -run TestOpenCancellation -count=1
--- FAIL: TestOpenCancellationInterruptsStalledHandshake (5.01s)
    main_test.go:52: AMQP handshake ignored context cancellation after TCP connected
```

The failing priority baseline and the approved correction are explained in
`spike/mq/FINDINGS.md`. No assertions were weakened or skipped.

Pre-PR review: READY for human PR creation and S review. The experiment is
development-only, not HA or the production messaging topology. No cluster/PVC
was deleted. Second-laptop cluster-recreation/RAM evidence remains separate and
unverified. A0.3 is not complete until merge and post-merge verification on main.

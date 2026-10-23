# S0.2 — second-laptop kind RAM verification

Date: 23 October 2026. Owner: Parth (A).
Source base: main `2b8f9f7c6c818bfee3ab27a5b3f4fc0f2f831108`.
Environment: Windows laptop, Ubuntu under WSL2, Docker Desktop integration.

## Result

The unchanged `bash scripts/verify-kind.sh` completed with:

```text
PASS  nodes: 5 in total
PASS  nodes: 1 control plane
PASS  nodes: 3 workers labelled role=data
PASS  nodes: 1 worker labelled role=infra
PASS  nodes: every kubelet runs v1.37.0 (the pinned node image)
PASS  ports: 6443 published on 127.0.0.1 only
PASS  ports: 8080 published on 127.0.0.1 only
PASS  ports: 8443 published on 127.0.0.1 only
PASS  taint: a pod without the toleration is not scheduled on infra
PASS  taint: a pod with the toleration is scheduled on infra (reliefgrid-worker4)
PASS  make doctor: passes on this machine with the cluster up (its own ports are ok)
PASS  evidence: docker stats lists 5 cluster containers
PASS  evidence: no trailing whitespace
PASS  S0.1: scripts/verify-skeleton.sh still passes
verify-kind: all checks passed
```

The raw generated file is [ram-DESKTOP-EG9G7UI.txt](ram-DESKTOP-EG9G7UI.txt).
It was not edited. Its timestamp is 2026-10-23T04:52:44Z, after the default
90-second settling period on a fresh cluster with no application workloads.
The five reported container values sum to 1208.2 MiB, approximately 1.18 GiB.
`free -h` reports 15 GiB total and 12 GiB available inside WSL.

These are Docker's container memory readings, not a measurement of the complete
Windows/WSL VM footprint. Other unrelated Docker applications were left running;
their memory is excluded from the five-container sum but can affect WSL totals.
This is idle infrastructure evidence, not a loaded application benchmark.

## Data-preservation decision

Parth delegated the preserve/recreate decision after explaining that he had not
started testing. A read-only inventory found only the RabbitMQ/Traefik spike:
one broker PVC and one durable quorum test queue with 0 ready messages,
0 unacknowledged messages and 0 consumers. No ReliefGrid application/storage
workloads or other user namespaces were found.

Before deletion, the configuration, broker definitions and an offline copy of
the PVC were saved outside the repository. The archive was readable and its
SHA256 checks passed. The old broker was restarted successfully after backup.
No backups, credentials, manifests or kubeconfigs are included in this change.
The unrelated MongoDB and Vault containers were not targeted.

The verifier removed/recreated only the reliefgrid dev cluster and its old PVC.
The original PVC content remains in the private local backup; this is not a
claim that a full backup restore has been tested.

## Repository validation

On this main-based branch, before and after the measurement:

```text
make check
go vet ./...
golangci-lint run ./...
0 issues.
go test -race ./...
```

All packages passed or reported no test files. `make gen` reported
`gen: no contracts yet, nothing to generate`, with no tracked changes.
This follow-up changes evidence only: no verifier, kind configuration, tool pins,
contracts or S-owned implementations were edited.

## Development environment redeployment

After the idle measurement, the same spike chart/code from A0.3 commit
`a22dd9c35cfc043c8d82aa0f7c6eb2b04eb2b9c6` was redeployed on the fresh cluster.
This intentionally uses a new PVC, not an untested raw-data restore. The only
previous queue was empty, and its test topology is recreated by the spike.
The unchanged `bash spike/mq.sh` completed:

```text
STATUS: deployed
REVISION: 1
confirmed=100
pod "reliefgrid-rabbitmq-0" deleted from reliefgrid namespace
pod/reliefgrid-rabbitmq-0 condition met
received=100
priority=strict:[31 15 7 2 1]
priority=omitted-default-4:[7 0 1]
PASS  A0.3 RabbitMQ restart verification
```

The idle RAM file was captured before this redeployment; it does not include
RabbitMQ/Traefik memory. The cluster and spike are left running for development.

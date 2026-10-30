# A0.3 review corrections — 26 October 2026

## Behaviour and failure cases

Traefik chart 42.1.2 now pins its official image index by digest. The chart's
appVersion is v3.7.16; its actual values place versionOverride at the subchart
root, alongside image.digest. The official image index was inspected before
writing the pin:

```text
Name: docker.io/library/traefik:v3.7.16
Digest: sha256:1336e85aaed293bd63d005086e0dd013b04078b26aaeb1c18b26a2f928af7e65
```

Application urgency is NORMAL=1, HIGH=2, CRITICAL=3. The persistence publisher
explicitly sets 1, the consumer checks it after restart, and a new independent
broker probe requires delivery order [3 2 1]. Both earlier strict probes remain.
Only the intentional omitted-property probe publishes zero; leaving an
application priority unset would give it effective priority 4 and put it ahead
of CRITICAL=3. FINDINGS documents that every application publisher must set a
nonzero priority.

The verifier additionally requires a digest-pinned deployed Traefik image and
compares the broker PVC UID before and after restart. It does not replace or
delete the PVC.

## Environment and scope

Integrated main e5abef0 in an isolated checkout. Initial doctor failed because
Docker Desktop was stopped; starting the installed Desktop restored WSL Docker
integration. Doctor then passed with all six tool pins, Docker 29.7.2, RAM and
ports. The existing five-node kind-reliefgrid cluster was used. Inventory found
only the RabbitMQ/Traefik release in the project namespace, with zero ready or
unacknowledged test messages and no consumers before this experiment.

## Verification output

make check exited 0: go vet passed, golangci-lint reported `0 issues.`, and all
race-test packages passed. make gen ran the internal generator without source
drift; public.yaml is still in the separate A0.1 PR. go mod verify reported
`all modules verified`.

The scoped spike verifier upgraded the dev release and restarted only the
broker pod:

```text
traefik_image=docker.io/traefik@sha256:1336e85aaed293bd63d005086e0dd013b04078b26aaeb1c18b26a2f928af7e65
confirmed=100
pvc_same_uid=3d982505-ac00-4eb2-b6ec-8b8b0cafb9b2
received=100
priority=strict:[31 15 7 2 1]
priority=urgency:[3 2 1]
priority=omitted-default-4:[7 0 1]
PASS  A0.3 RabbitMQ restart verification
```

Strict Helm lint: `1 chart(s) linted, 0 chart(s) failed`. All ten rendered
resources passed Kubernetes server-side dry-run validation. The chart index
refresh printed a connection-reset warning during the final lint setup, but
the locked dependency downloaded successfully and subsequent checks exited 0.
Dry-run warnings concern Helm-managed resources lacking kubectl's last-applied
annotation; no real kubectl apply was performed.

Raw doctor, check, spike, lint and dry-run outputs accompany this report.

## Limits

Single-member quorum queue and same-PVC pod restart only; no broker HA,
PVC-loss or worker-loss claim. The two-owner review and merge are still
required, followed by post-merge checks before A0.3 is complete. Retry/DLX and
application workflows remain A1.3/A1.5. The separate RAM evidence has already
merged as #10; the earlier text saying it was still pending is superseded.

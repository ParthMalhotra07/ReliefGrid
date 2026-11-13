# Operational app-sender wiring [A4.4]

This support remains opt-in. It does not enable either chart or certify a live
rollout. Receiver/client support is #163, API/matcher startup is #171. Both must
be accepted before an authenticated deployment. Keep internalAuth.enabled=false
until both owners have completed the invocation inventory and kind checks.

## Host calls

Set INTERNAL_AUTH_TOKEN_FILE to a protected file containing exactly the separately
staged **app sender-token**, without a newline. Do not set it to a node credential,
receiver current/previous token, operator Bearer or broker credential. Provision
the matching Kubernetes app Secret through the storage owner's rotation runbook;
this helper never creates Secrets or copies token bytes into manifests/evidence.
Do not pass credential bytes in argv, save them in source/evidence, or enable
shell tracing around provisioning. The value is a path, not the credential.

Python startup rejects missing, empty, whitespace, comma, non-ASCII and oversized
configured files. An unset/empty path preserves the disabled profile; an invalid
configured path does not fall back to anonymous. The shared sender binds calls
to an explicit coordinator origin and /internal/ paths, disables proxies and
refuses all redirects. It replaces internal header values and, when enabled,
does not combine the sender with operator/proxy authorization. Public API and
telemetry calls use their existing transports and receive no internal token.
Use only trusted coordinator endpoints; the header does not add TLS itself.

Covered A callers:

- metrics_fault scratch mutation, same-input recovery and paged cleanup scans;
  the A4.2 verifier's internal coordinator queries. Public API/Prometheus/Grafana
  requests retain their separate identities.
- verify-reconcile coordinator reads and inherited Go host-probe environment.
- app demo coordinator reads/outbox drain; its host drain-outbox process already
  inherits the sender-file environment. Retry bytes/operation IDs, scan authority
  checks and strict audit ordering are unchanged.

## Audit/recovery Pods and Jobs

When the host file is configured and valid, app demo invcheck Jobs (also reused
by A4.3's post-round audit), verify-reconcile crash workers and its final audit
Job mount only reliefgrid-internal-auth/sender-token read-only at
/var/run/reliefgrid/internal-auth/sender-token. File mode is 0440 and Pod fsGroup
65532. The environment contains that in-Pod path, never a token value. Current,
previous and node keys are not mounted. Missing Kubernetes Secrets prevent Pod
startup. With the path unset, generated Pod specs are unchanged. Use an existing
tools/recovery image containing the accepted #163 Go client support, not a stale
pre-auth image with the same mutable tag.

## Remaining joint prerequisites

S owns the separate node sender for direct storage calls and wiring of storage/
chaos/load callers. Shared shell-demo and schedules entry points must still be
checked against their final accepted owner changes; this patch is not a blanket
claim that every command is auth-ready. Operator endpoints/probes/metrics retain
their separate exemptions and access policy. The complete enablement run must
exercise valid/invalid/missing credentials, unknown/retry outcomes, no-secret
diagnostics, receiver-first rotation/restarts and all final operational Jobs.
No live kind, broker, enablement or rotation execution is claimed here.

# Application internal-call sender wiring [A4.4]

This follow-up depends on #163. Authentication remains disabled by default;
merging caller support does not authorize or perform enablement.

API startup reads the optional INTERNAL_AUTH_TOKEN_FILE path into its process
configuration and passes it to coordclient.WithInternalAuthFile before the
initial ring request. Matcher startup uses the same option. If configured, a
missing/unreadable/empty/malformed file stops startup before coordinator I/O or
listeners; bytes are not trimmed. An unset path preserves the disabled profile.
The authenticated Store is shared with app metrics, relay, consumer and
reconciler, so those workers need no separate credentials or client wrapper.

The #163 app chart mounts only reliefgrid-internal-auth/sender-token for API and
matcher when internalAuth.enabled=true. UI receives no internal/node/operator
credential. Storage owns the Secret; app Helm does not create another copy.
Files load once: restart senders after staged rotation. Never pass a token in
argv or log its contents. See [the owner runbook](internal-auth.md).

## Operational inventory before enablement

The Go invcheck, drain-outbox and reconcile-crash binaries have optional sender
file support in #163. Their actual host/Pod/Job invocation must also supply the
file. A binary accepting an option is not evidence that every invocation is
configured. A operational support is implemented separately in #172 and must be merged
and checked with these binaries. Final deployment still requires:

- #172 Python verifier coordinator calls (metrics_fault, verify-reconcile), app
  demo clients and generated recovery/audit Pods reused by A4.2/A4.3. Its local
  regression and mutation checks pass; final images/mounts still need live kind
  acceptance. Public API/telemetry calls stay separate; retry identity, uncertainty
  handling and strict audit ordering remain unchanged.
- Shared demos and test-schedules/chaos/load/audit tooling: configure the owning
  script's sender-file/mount before use. S owns chaos/load/storage tooling.
- Direct storage probes use the separately staged node sender credential,
  never the app sender. Health/metrics/status probes stay exempt per #163.

Do not enable protected receivers until both owners have checked that complete
invocation inventory and performed the planned kind authentication/rotation run.
This document and fixture tests establish no live rollout or rotation success.

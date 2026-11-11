# Internal HTTP authentication (#29 / S-29)

Dependency: #156 was OPEN at 42ca268 when this branch was started. The wiring
branch is based on a/A4.4-auth-helpers with current main merged in. No deployment
or credential rotation has been performed.

The **storage Helm release owns both** `reliefgrid-internal-auth` (application)
and `reliefgrid-node-auth` (node) Secrets. Do not create copies in the application
release. Current tokens are independent random keys, preserved by Helm lookup
on upgrades. Offline/GitOps rendering must supply explicit stable values through
secure values delivery: lookup cannot retain them there. Never commit credentials.

Coordinator accepts the app token on `/internal/` data, ring, scan and outbox
routes; `/internal/nodes/` registration/heartbeat accepts only the node token.
Storage data routes accept only the node token. All are guarded before handlers.
Storage refusals retain server/shard identity for coordinator verification.
Health/readiness and the separate metrics listener remain unauthenticated.
Operator `/admin` and `/mgmt` routes keep their separate bearer-token policy.
Raft TCP is not protected by this header; existing network policy remains the
boundary. This change does not add Raft mTLS.

Startup reads mounted files once, validates bytes without trimming, and fails
closed for missing, empty or malformed current/sender credentials. Previous keys
may be absent. Coordinator uses `INTERNAL_AUTH_TOKEN_FILE` and
`INTERNAL_AUTH_PREVIOUS_TOKEN_FILE` for app reception. Both binaries use
`NODE_AUTH_TOKEN_FILE`, `NODE_AUTH_PREVIOUS_TOKEN_FILE` and
`NODE_AUTH_SENDER_TOKEN_FILE`. Defaults match chart mounts under
`/var/run/reliefgrid/{internal,node}-auth`. There is no file watch reload.

API/matcher mounts expose **only** the app sender key, using
`INTERNAL_AUTH_TOKEN_FILE`. Their A-owned startup integration must load/validate
this file and pass `coordclient.WithInternalAuth(auth.SenderConfig{Token: token})`.
This branch wires coordinator/storage binaries and provides that client option;
it does not change A-owned application binaries. Complete that integration and
update operational clients (invcheck, load/demo tooling) before deploying the
protected receivers. Health-only callers need no token.

Construct app and node HTTP clients separately from a raw unauthenticated client:
`auth.NewClient(raw, appConfig)` and `auth.NewClient(raw, nodeConfig)`. Never wrap
one authenticated client in another, even through a tracing transport. coordclient
applies WithInternalAuth after all options and before the initial ring request,
so invalid credentials cannot result in an anonymous request. Every call must
have a context deadline or client timeout; redirects are refused.

## Rotation (restart required at every phase)

For each Secret independently, values are `internalAuth.*` or `nodeAuth.*` in the
storage chart. Use fresh random tokens, not the examples in tests.

1. Set `token` to new, `previousToken` to old, **senderToken to old**. Upgrade the
   owning storage release. Restart all receivers and wait for rollout completion
   (coordinator for app auth; coordinator and storage nodes for node auth).
   Dual-role processes continue sending old via the separate sender key.
2. Keep both receiver tokens. Set `senderToken` to new, upgrade, and restart all
   senders. For app auth this includes API/matcher and other operational clients;
   for node auth, coordinator and all storage nodes. Verify rollouts complete.
3. Clear `previousToken`, upgrade, and restart all receivers. Keep senderToken=new
   or clear its override only when it equals token. No automatic checksum rollout
   is used: it would conflate receiver and sender phases.

A 401 UNAUTHORIZED describes **this attempt**, which never reached a handler.
The coordinator never replaces an earlier unknown outcome with that refusal.
coordclient also retains unresolved operation identities within its lifetime and
keeps later failures unknown until a committed result resolves them. Unresolved
entries are not evicted; durable workflow intent remains the cross-process source
of truth. Client replacement/restart cannot prove rejection. Fix credentials and
retry the **same operationId and payload**; never clear an intent merely on 401.

## Validation pending

User requested no go test, make check, builds or Docker during the measuring load
run. Added tests are unrun. After authorization run focused tests for internalauth,
auth, coordclient, coordinator, storagenode, cmd/coordinator, cmd/storagenode and
scripts chart tests; then make check and make gen-check before any push.

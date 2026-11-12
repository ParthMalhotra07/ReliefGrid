# Internal HTTP authentication (#29 / S-29)

This branch includes the shared authentication helpers from #156. No deployment
or credential rotation has been performed.

Both charts default `internalAuth.enabled: false`. With it off, no auth Secrets,
mounts or auth environment variables are rendered; coordinator/storage skip all
credential loading and use their original unauthenticated internal clients and
handlers. `INTERNAL_AUTH_ENABLED` defaults false in both binaries; invalid boolean
values prevent startup. The flag controls **both** application and node auth.
Set it consistently in storage and application chart values only after API,
matcher and operational callers are wired and have their sender mounts. Empty
credentials are never an enabled compatibility mode.

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

`cmd/invcheck`, `scripts/demos/drain-outbox` and `scripts/reconcile-crash` use
`coordclient.WithInternalAuthFile(os.Getenv("INTERNAL_AUTH_TOKEN_FILE"))`.
When set, that path is loaded once and validated before the initial ring request;
missing/unreadable/empty/malformed files fail before network I/O. When unset,
these tools keep their original anonymous behavior for disabled clusters. Mount
only `reliefgrid-internal-auth`'s `sender-token` and set the env path to
`/var/run/reliefgrid/internal-auth/sender-token` in each operational pod/job.
For invcheck's direct storage `--probe` reads, separately mount the node Secret's
`sender-token` and set `NODE_AUTH_SENDER_TOKEN_FILE`; the app token must never
be reused for node probes. Never pass credentials as command-line values.

Construct app and node HTTP clients separately from a raw unauthenticated client:
`auth.NewClient(raw, appConfig)` and `auth.NewClient(raw, nodeConfig)`. Never wrap
one authenticated client in another, even through a tracing transport. coordclient
applies WithInternalAuth after all options and before the initial ring request,
so invalid credentials cannot result in an anonymous request. Every call must
have a context deadline or client timeout; redirects are refused.

## Rotation (restart required at every phase)

For each Secret independently, values are `internalAuth.*` or `nodeAuth.*` in the
storage chart. Use fresh random tokens, not the examples in tests. Rendering
fails if either
`previousToken` is nonempty while its `senderToken` is empty. Explicitly stage
senderToken in **both** overlap phases; the fallback to token is only safe when
previousToken is empty.

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

Before advancing **each** phase, verify the mounted bytes in **every** affected
pod against the intended Secret keys, not just the Secret object. Kubernetes
volume updates do not reload process memory; wait for the required restarts too.
The production images are distroless: use an approved, digest-pinned ephemeral
debug image with a shell, sha256sum and access to the target process namespace.
For example (set namespace, pod, container and Secret for the credential class):

```sh
# PINNED_DEBUG_IMAGE must be the approved debug image@sha256:... reference.
expected=$(kubectl -n "$NS" get secret "$SECRET" -o jsonpath='{.data.sender-token}' | base64 -d | sha256sum | cut -d' ' -f1)
kubectl debug -n "$NS" "pod/$POD" --target="$CONTAINER" --image="$PINNED_DEBUG_IMAGE" -- \
  sh -eu -c 'sha256sum /proc/1/root/var/run/reliefgrid/'"$CLASS"'-auth/sender-token'
# Compare the returned digest to "$expected"; never print raw token contents.
```

`CLASS` is `internal` or `node`; verify that `/proc/1/root` belongs to the target
container before using this path. Repeat for receiver `token` and, during overlap,
`previous-token`, using the corresponding Secret keys and mounted paths. App
senders expose only sender-token. In phase 1, receiver token=new, previous=old,
sender=old; in phase 2 sender=new with both receivers retained; in phase 3 token
and sender=new and previous absent. Do not proceed if any pod differs. This check
verifies mounted contents; completed restarts establish which bytes were loaded.

A 401 UNAUTHORIZED describes **this attempt**, which never reached a handler.
The coordinator never replaces an earlier unknown outcome with that refusal.
coordclient also retains unresolved operation identities within its lifetime and
keeps later failures unknown until a committed result resolves them. Unresolved
entries are not evicted; durable workflow intent remains the cross-process source
of truth. Client replacement/restart cannot prove rejection. Fix credentials and
retry the **same operationId and payload**; never clear an intent merely on 401.

## Validation

Run `go test ./... -count=1`, `make check`, `make gen-check`, the integration-tagged
auth chart tests and the 16 review mutation checks before accepting this wiring.
Each mutation must fail its targeted test, with a passing baseline. These checks
are local acceptance, not authorization to enable auth before all callers are ready.

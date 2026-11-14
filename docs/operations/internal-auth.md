# Internal HTTP authentication (#29)

The coordinator's and storage nodes' `/internal/` routes can require a token in the
`X-ReliefGrid-Internal-Token` header. There are two credential classes:

- **app**: the coordinator's data, ring, scan and outbox routes. Senders are the API,
  the matcher and the operational tools (invcheck, drain-outbox, reconcile-crash, the
  demo, chaos, load and verify scripts).
- **node**: `/internal/nodes/` registration and heartbeat on the coordinator, and every
  storage data route. Senders are the coordinator and the storage nodes.

Health/readiness and the separate metrics listener stay unauthenticated. The operator
`/admin` and `/mgmt` routes keep their own bearer token. Raft TCP is not covered by this
header: the existing network policy is its boundary, and there is no Raft mTLS.

Both charts default to `internalAuth.enabled: false`. When it is off, nothing auth-related
is rendered (no Secrets, mounts or variables), and every binary and script behaves as it
did before #29.

## Turning it on

Enable it in **both** releases with the same setting. The **storage release owns both
Secrets**, `reliefgrid-internal-auth` (app) and `reliefgrid-node-auth` (node). Its chart
generates random tokens and keeps them across upgrades through Helm `lookup`. The
application release only mounts the app `sender-token`.

**Fresh deployment (demo/dev):**

```sh
make demo-reset INTERNAL_AUTH=true      # or: INTERNAL_AUTH=true make demo-reset
```

- The storage install gets `-f deploy/helm/storage/values-internal-auth.yaml`.
- The app install (`scripts/app-deploy.sh`) gets
  `-f deploy/helm/reliefgrid/values-internal-auth.yaml`.
- Those files only set `internalAuth.enabled: true`. No token is ever on a command line
  or in a committed file.
- With `INTERNAL_AUTH` unset, both scripts run exactly the commands they ran before.
  `INTERNAL_AUTH` must be `true` or `false`.

**App release only:**

```sh
INTERNAL_AUTH=true make app-deploy
```

- `app-deploy` refuses to run unless the storage release has the same setting.
- It also refuses `INTERNAL_AUTH=true` with `APP_WITH_STORAGE=true`: the app chart does not
  pass the setting to its storage subchart.

**Existing deployment:** turn it on with the app quiesced.
1. Scale `reliefgrid-api` and `reliefgrid-matcher` to 0.
2. Upgrade the storage release:
   ```sh
   helm --kube-context "$CTX" -n "$NS" upgrade storage deploy/helm/storage --reuse-values \
     -f deploy/helm/storage/values-internal-auth.yaml --wait --wait-for-jobs
   ```
   This creates the Secrets and changes the pod templates, so the coordinator and storage
   pods roll.
3. Wait for nine nodes UP and one leader per shard (see "Restarting receivers and senders").
4. Run `INTERNAL_AUTH=true make app-deploy`.
5. Scale the app back.

Senders that start before the receivers enforce are harmless: a receiver that is off
ignores the header. Receivers that enforce before the app sends refuse it, which is why the
app is quiesced first.

**After enabling,** check:
- both releases' `helm get values <release> -a` show `internalAuth.enabled: true`;
- both Secrets exist with `token` and `sender-token`;
- a request without a token gets 401, and the same request with the right class's
  `sender-token` gets 200.

`scripts/verify-storage-k8s.sh` with `STORAGE_INTERNAL_AUTH=true` checks this.

## How tokens are loaded and sent

- **Startup:** receivers and senders read their mounted files once at startup and validate
  the bytes without trimming: 1–4096 printable ASCII bytes, no comma, no trailing newline.
  A missing, empty or malformed current or sender credential stops startup. A previous key
  may be absent. There is no file-watch reload.
- **Mounts and variables:**
  - The coordinator uses `INTERNAL_AUTH_TOKEN_FILE` and `INTERNAL_AUTH_PREVIOUS_TOKEN_FILE`
    to receive app tokens.
  - The coordinator and storage nodes use `NODE_AUTH_TOKEN_FILE`,
    `NODE_AUTH_PREVIOUS_TOKEN_FILE` and `NODE_AUTH_SENDER_TOKEN_FILE`.
  - The mounts are under `/var/run/reliefgrid/{internal,node}-auth`, mode 0440,
    fsGroup 65532.
- **App senders:** the API and matcher mount only the app `sender-token` and read it through
  `INTERNAL_AUTH_TOKEN_FILE`.
- **Operational Go tools** (`cmd/invcheck`, `scripts/demos/drain-outbox`,
  `scripts/reconcile-crash`):
  - They use `coordclient.WithInternalAuthFile(os.Getenv("INTERNAL_AUTH_TOKEN_FILE"))`.
    When the variable is set, the file is validated before any network I/O. When it is
    unset, they send no token.
  - invcheck's direct storage `--probe` reads use the node `sender-token` through
    `NODE_AUTH_SENDER_TOKEN_FILE`. Never reuse the app token for node calls.
- **Shell callers** use `scripts/internal-auth-env.sh`:
  - It reads whether the storage release has auth on. Only a JSON boolean counts; anything
    unreadable or malformed stops the caller.
  - When on, it copies each Secret's `sender-token` into a 0600 file, removed on exit, and
    exports the two variables above.
  - It sends curl headers from header files (`-H @file`).
  - It adds the read-only Secret mounts to Jobs and `kubectl run` pods.
- **Building clients in Go:** construct app and node HTTP clients separately from a raw
  client, with `auth.NewClient(raw, appConfig)` and `auth.NewClient(raw, nodeConfig)`. Never
  wrap one authenticated client in another. Every call needs a deadline. Redirects are
  refused.

## Refusals

A refused request never reaches a handler. It gets `401` with code `UNAUTHORIZED`, and the
receiver logs one line at WARN:

```
msg="internal auth refused" credential=app|node method=PUT route=/internal/data/<key> peer=<ip:port> reason=<reason>
```

| `reason` | Meaning |
|---|---|
| `missing` | No header. |
| `repeated header` | More than one header value. |
| `malformed` | Not 1–4096 printable bytes, or it contains a comma. |
| `unknown credential` | Well-formed, but neither the current nor the previous token. |
| `receiver misconfigured` | The receiver's own configuration is invalid. |

The presented value and the configured tokens are never logged; the reason comes only from
the header's presence, count and syntax. During a rotation, these lines are how a sender
still using a credential that is no longer accepted shows up:

```sh
kubectl --context "$CTX" -n "$NS" logs deployment/storage-coordinator --since=10m | grep 'internal auth refused'
kubectl --context "$CTX" -n "$NS" logs storage-shard-0-0 -c storagenode --since=10m | grep 'internal auth refused'
```

A 401 describes **this attempt**. The coordinator never replaces an earlier unknown outcome
with that refusal. coordclient keeps unresolved operation identities for its lifetime, and
later failures stay unknown until a committed result resolves them. Fix the credentials and
retry the **same operationId and payload**; never clear an intent merely because of a 401.

## Rotation (a restart is required at every phase)

Values are `internalAuth.{token,previousToken,senderToken}` (app) and
`nodeAuth.{token,previousToken,senderToken}` (node), in the **storage** chart.

### Setup, before any other command

Set the cluster and namespace explicitly and check them first. Every command below goes
through `k` and `h`, so none can reach another cluster or namespace by default:

```sh
CTX=kind-reliefgrid NS=reliefgrid            # the cluster and namespace being rotated
k() { kubectl --context "$CTX" -n "$NS" "$@"; }
h() { helm --kube-context "$CTX" -n "$NS" "$@"; }
kubectl config get-contexts -o name | grep -qx "$CTX" || { echo "no kube context $CTX"; exit 1; }
kubectl --context "$CTX" get namespace "$NS" >/dev/null || { echo "no namespace $NS in $CTX"; exit 1; }
h status storage >/dev/null || { echo "no storage release in $NS"; exit 1; }
h get values storage -a -o json | jq -e '.internalAuth.enabled == true' >/dev/null || { echo "internal auth is not on in $NS"; exit 1; }
k get secret reliefgrid-internal-auth reliefgrid-node-auth >/dev/null || { echo "the auth Secrets are missing in $NS"; exit 1; }
```

**Token files.** Every token file is created private, in a private directory. Write each
token straight into its file; never echo it and never pass it as an argument:

```sh
umask 077                                    # every file and directory below is private
TMP="$(mktemp -d)"                           # 0700
for f in new-app-token new-node-token; do
  python3 -c 'import secrets, sys; sys.stdout.write(secrets.token_hex(24))' > "$TMP/$f"
done
# The current tokens (the next previousToken), from the Secrets into files:
k get secret reliefgrid-internal-auth -o jsonpath='{.data.token}' | base64 -d > "$TMP/old-app-token"
k get secret reliefgrid-node-auth -o jsonpath='{.data.token}' | base64 -d > "$TMP/old-node-token"
stat -c '%a %n' "$TMP" "$TMP"/*              # expect 700 for the directory, 600 for each file
```

The values file for each phase is written from those files by a program, not by shell
interpolation, so no token passes through a shell variable or a command line. The
arguments are the token file names for the app token, previous and sender, then the node
token, previous and sender; `-` means empty:

```sh
values() {
  python3 - "$TMP" "$@" > "$TMP/rotation.yaml" <<'EOF'
import sys
d, ta, pa, sa, tn, pn, sn = sys.argv[1:]
r = lambda n: '' if n == '-' else open(f'{d}/{n}').read()
for key, (t, p, s) in {'internalAuth': (ta, pa, sa), 'nodeAuth': (tn, pn, sn)}.items():
    print(f'{key}:\n  token: "{r(t)}"\n  previousToken: "{r(p)}"\n  senderToken: "{r(s)}"')
EOF
}
# phase 1: values new-app-token old-app-token old-app-token new-node-token old-node-token old-node-token
# phase 2: values new-app-token old-app-token new-app-token new-node-token old-node-token new-node-token
# phase 3: values new-app-token - new-app-token new-node-token - new-node-token
```

Remove `$TMP` (`rm -rf "$TMP"`) once phase 3 is verified.

- **Rotating both classes:** both may be rotated together. Each phase then applies to both,
  and the restarts are the union of what each class needs.
- **Fresh tokens:** generate fresh random ones, as files in the private directory from
  "Setup" above.
- **Rendering rule:** rendering fails if a `previousToken` is set while its `senderToken`
  is empty. Set `senderToken` explicitly in both overlap phases.
- **Passing tokens to Helm:**
  - Never use `--set`: the value would be on the command line and in shell history.
  - Write the values file with `values` (above) and pass it with `-f`, keeping
    `--reuse-values` so `internalAuth.enabled` stays on:
    ```sh
    h upgrade storage deploy/helm/storage --reuse-values -f "$TMP/rotation.yaml" --wait --wait-for-jobs
    ```
  - Values supplied this way are kept in the Helm release history. `helm get values storage
    --revision N` shows them for as long as that revision exists. After phase 3 the old
    token is no longer accepted anywhere, but the current one is in the latest revisions as
    well as in the Secret. Restrict who can read the release Secrets in the namespace, or
    limit retained revisions with `--history-max`.

**Phases:**
1. **New token, old token still accepted.** Set `token` to new, `previousToken` to old and
   **`senderToken` to old**. Upgrade, then restart the receivers: the coordinator for app
   auth; the coordinator and all storage nodes for node auth. Processes that are both
   sender and receiver keep sending old through the separate sender key.
2. **Senders switch to new.** Keep both receiver tokens and set `senderToken` to new.
   Upgrade, then restart the senders: the API and matcher for app auth (operational tools
   read the Secret when they start); the coordinator and all storage nodes for node auth.
3. **Old token retired.** Clear `previousToken`. Keep `senderToken` set to new, or clear
   the override only when it equals `token`. Upgrade, then restart the receivers.

No checksum rollout is automatic; it would mix the receiver and sender phases.

### Restarting receivers and senders

- **Coordinator:**
  `k rollout restart deployment/storage-coordinator && k rollout status deployment/storage-coordinator`.
- **Storage:** one StatefulSet at a time, so no shard loses quorum:
  ```sh
  for s in 0 1 2; do
    k rollout restart statefulset/storage-shard-$s
    k rollout status statefulset/storage-shard-$s --timeout=600s
  done
  ```
  Then wait until `/admin/nodes` (operator token) reports nine nodes UP, and every shard
  has exactly one leader (`storagectl status` in each pod).
- **API and matcher:** `k rollout restart deployment/reliefgrid-api deployment/reliefgrid-matcher`,
  then `k rollout status` for each.

### Before advancing each phase

Do all of the following before moving on:
- Verify the mounted bytes in **every** affected pod against the intended values (below),
  not just the Secret object.
- Check the refusal log lines above.
- Run one write and read-back through the coordinator with the current `sender-token`.

Pods that were not restarted in a phase (the API and matcher in phases 1 and 3) see Secret
changes only after the kubelet sync, usually within one to two minutes, so retry the mount
check for a few minutes before treating a difference as real. Kubernetes volume updates
never reload process memory. Completed restarts establish which bytes a process loaded.

**Checking the mounts.** The images are distroless, so use an ephemeral debug container
from a digest-pinned image with a shell and `sha256sum`. The one used for this
procedure is `gcr.io/distroless/static-debian12@sha256:d5563cc7f2f44313f332e91138cc8c6a158899afeeeab2fce3b0f9ccdb3cf9ee`
(the `debug-nonroot` variant). It runs as uid 65532, the same as the targets, so it can
read their files through `/proc/1/root`.

On kind, load it first with `kind load docker-image gcr.io/distroless/static-debian12:debug-nonroot`.
A digest reference may still pull from gcr.io if the local import did not record the
digest.

```sh
DEBUG_IMAGE=gcr.io/distroless/static-debian12@sha256:d5563cc7f2f44313f332e91138cc8c6a158899afeeeab2fce3b0f9ccdb3cf9ee
expected=$(k get secret "$SECRET" -o jsonpath='{.data.token}' | base64 -d | sha256sum | cut -d' ' -f1)   # a digest only
k debug "pod/$POD" -c authcheck-$RANDOM --target="$CONTAINER" --image="$DEBUG_IMAGE" --profile=general -- \
  sh -c 'for f in /proc/1/root/var/run/reliefgrid/*-auth/*; do [ -f "$f" ] && sha256sum "$f"; done'
k logs "$POD" -c authcheck-<n>   # compare each digest; never print raw token contents
```

**Where the files are:**
- The coordinator (container `coordinator`) mounts both Secrets in full: `token`,
  `sender-token`, and `previous-token` while one is set.
- Storage pods (`storagenode`) mount the node Secret.
- The API and matcher mount only the app `sender-token`.

**Intended values per phase:**

| | `token` | `previous-token` | `sender-token` |
|---|---|---|---|
| Before phase 1 | old | absent | old |
| After phase 1 | new | old | old |
| After phase 2 | new | old | new |
| After phase 3 | new | absent | new |

**Acceptance per phase:**
- Before phase 1, only the old token is accepted.
- After phases 1 and 2, both are accepted.
- After phase 3, only the new token is accepted, and the old one gets 401 with
  `reason=unknown credential`.

Do not proceed if any pod differs.

## Validation

Run before accepting changes to this wiring:
- `go test ./... -count=1`;
- `make check` and `make gen-check`;
- the integration-tagged auth chart tests;
- the review mutation checks (`scripts/internal-auth-mutations.py`). Each mutation must
  fail its targeted test, against a passing baseline.

These are local acceptance checks. The deployed enable and rotation procedure above is
accepted only by a kind run.

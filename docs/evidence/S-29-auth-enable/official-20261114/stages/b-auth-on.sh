# Auth is really on: both releases, both Secrets, the pods' environment, and one refused request.
. "$R/stages/lib.sh"
for rel in storage reliefgrid; do
	helm --kube-context kind-reliefgrid -n reliefgrid get values "$rel" -a -o json | jq -e '.internalAuth.enabled == true' >/dev/null &&
		ok "release $rel: internalAuth.enabled=true (computed values)" || die "release $rel: internalAuth.enabled is not true"
done
for s in reliefgrid-internal-auth reliefgrid-node-auth; do
	$K get secret "$s" -o json | jq -e '.data | has("token") and has("sender-token") and (has("previous-token") | not)' >/dev/null &&
		ok "Secret $s: token and sender-token, no previous-token" || die "Secret $s missing or unexpected keys"
done
envof() { $K get "$1" -o json | jq -r --arg c "$2" '.spec.template.spec.containers[] | select(.name == $c) | [.env[]? | "\(.name)=\(.value // "")"] | .[]'; }
envof deployment/storage-coordinator coordinator | quiet_grep -x 'INTERNAL_AUTH_ENABLED=true' && ok "coordinator: INTERNAL_AUTH_ENABLED=true" || die "coordinator: INTERNAL_AUTH_ENABLED not true"
for s in 0 1 2; do
	envof statefulset/storage-shard-$s storagenode | quiet_grep -x 'INTERNAL_AUTH_ENABLED=true' && ok "storage-shard-$s: INTERNAL_AUTH_ENABLED=true" || die "storage-shard-$s: INTERNAL_AUTH_ENABLED not true"
done
for d in api matcher; do
	envof deployment/reliefgrid-$d $d | quiet_grep -x 'INTERNAL_AUTH_TOKEN_FILE=/var/run/reliefgrid/internal-auth/sender-token' && ok "$d: INTERNAL_AUTH_TOKEN_FILE set" || die "$d: no INTERNAL_AUTH_TOKEN_FILE"
done
since="$(date -u +%FT%TZ)"
pf svc/storage-coordinator 8080 || die "no coordinator forward"
code="$(curl -s -m 6 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PF_PORT/internal/ring")"
[ "$code" = 401 ] && ok "coordinator GET /internal/ring without a token: 401" || die "coordinator GET /internal/ring without a token: $code"
sleep 2
refusals "$since" | quiet_grep 'route=/internal/ring.*reason=missing\|"route":"/internal/ring".*"reason":"missing"' &&
	ok "the refusal is logged: route /internal/ring, reason missing" || die "no refusal log line for the request without a token: $(refusals "$since" | head -3)"
reset_start="$(awk -F'\t' '$1 == "a-reset" { print $2 }' "$R/timeline.tsv")"
[ -n "$reset_start" ] || die "no a-reset start in the timeline"
[ -z "$(in_cluster_refusals "$reset_start")" ] && ok "no in-cluster sender refused since the reset began ($reset_start)" || die "in-cluster refusals: $(in_cluster_refusals "$reset_start" | head -5)"
# The pinned debug image for the rotation's mount checks, loaded so no pull is needed (by tag;
# the digest reference falls back to a pull if containerd does not record it).
kind load docker-image --name reliefgrid gcr.io/distroless/static-debian12:debug-nonroot >/dev/null 2>&1 && ok "debug image loaded into kind" || log "debug image not preloaded; the mount checks will pull it by digest"

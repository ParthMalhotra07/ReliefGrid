#!/usr/bin/env bash
# Verify check for S4.6b (test seams for SCH01/12/13, #88) on the local "reliefgrid" kind
# cluster, which must already run the storage chart (make demo-reset). It never creates or
# deletes the cluster:
#   scripts/verify-S4.6b.sh                 # SCH01, SCH12, SCH13a, SCH13b on shard 1
#   SEAMS_SHARD=2 scripts/verify-S4.6b.sh
#   SEAMS_TESTS='SCH13a' scripts/verify-S4.6b.sh
# It builds reliefgrid/storagenode:seams (make storage-image-seams), upgrades the storage
# release onto it, runs tests/seams (go test -tags kind) through a coordinator port-forward,
# and on exit, pass or fail, upgrades the release back to the default image and waits for it.
# Kills are SIGKILLs of the storagenode container through crictl on the kind node (the PVC
# and emptyDir survive); kubectl delete --force is the reported fallback. Follower lag uses
# the S2.3 isolation label (reliefgrid.io/chaos-isolated) with the conntrack fallback.
# Evidence: docs/evidence/S4.6b/kind-<host>.txt.
set -uo pipefail

CTX="${SEAMS_CONTEXT:-kind-reliefgrid}"
NS="${SEAMS_NAMESPACE:-reliefgrid}"
RELEASE="${STORAGE_RELEASE:-storage}"
CHART=deploy/helm/storage
DEFAULT_TAG="${DEFAULT_TAG:-dev}"
OUT="docs/evidence/S4.6b/kind-$(hostname -s).txt"
TESTS="${SEAMS_TESTS:-SCH01|SCH12|SCH13a|SCH13b}"

kn() { kubectl --context "$CTX" -n "$NS" "$@"; }
log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

kind get clusters 2>/dev/null | grep -qx "${CTX#kind-}" || { log "no kind cluster ${CTX#kind-}; this script never creates one"; exit 2; }
kn get statefulset "storage-shard-0" >/dev/null 2>&1 || { log "no storage release in $NS (make demo-reset first)"; exit 2; }

rollout() { # rollout <tag>
	helm --kube-context "$CTX" upgrade "$RELEASE" "$CHART" -n "$NS" --reset-then-reuse-values --set "image.tag=$1" --wait --timeout 15m >/dev/null &&
		for s in 0 1 2; do kn rollout status "statefulset/storage-shard-$s" --timeout=600s >/dev/null || return 1; done
}
pf_pid=""
restore() {
	status=$?
	cleanup_status=0
	trap - EXIT
	[ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null
	for s in 0 1 2; do for i in 0 1 2; do kn label pod "storage-shard-$s-$i" reliefgrid.io/chaos-isolated- >/dev/null 2>&1; done; done
	log "restoring the storage release to image tag $DEFAULT_TAG"
	rollout "$DEFAULT_TAG" || { log "RESTORE FAILED: run helm upgrade $RELEASE --reset-then-reuse-values --set image.tag=$DEFAULT_TAG"; cleanup_status=1; }
	if [ "$status" = 0 ]; then status=$cleanup_status; fi
	exit "$status"
}
trap restore EXIT

log "building and loading reliefgrid/storagenode:seams"
make -s storage-image-seams >/dev/null || { log "storage-image-seams failed"; exit 2; }
log "upgrading $RELEASE to image tag seams"
rollout seams || { log "rollout onto the seams image failed"; exit 2; }

port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')"
kn port-forward svc/storage-coordinator "$port:8080" >/dev/null 2>&1 &
pf_pid=$!
for _ in $(seq 1 50); do curl -fsS "http://127.0.0.1:$port/internal/ring" >/dev/null 2>&1 && break; sleep 0.2; done

mkdir -p "$(dirname "$OUT")"
log "running tests/seams ($TESTS); evidence $OUT"
COORDINATOR_URL="http://127.0.0.1:$port" SEAMS_CONTEXT="$CTX" SEAMS_NAMESPACE="$NS" \
	go test -tags kind -count=1 -v -timeout 30m -run "TestKind($TESTS)" ./tests/seams/ 2>&1 | tee "$OUT"
status=${PIPESTATUS[0]}
[ "$status" = 0 ] && log "PASS" || log "FAIL (exit $status)"
exit "$status"

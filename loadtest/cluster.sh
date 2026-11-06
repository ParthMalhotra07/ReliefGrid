#!/usr/bin/env bash
# The S5.2 load deployment on a fresh kind cluster:
#   make cluster-up; storage, coordinator and tools images;
#   deploy/helm/storage with values-load.yaml (load Raft profile, Guaranteed pods);
#   make app-deploy, then the app release upgraded with values-load.yaml
#   (Guaranteed pods; image tags and everything else reused);
#   then checks every pod in the namespace is Guaranteed and Ready.
# It creates the cluster if needed and refuses one that already runs storage:
# start from `make cluster-down` (a load result needs a known starting state).
# app-deploy writes docs/evidence/A1.5; this script restores it on exit.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
CLUSTER="${CLUSTER:-reliefgrid}"
CTX="kind-$CLUSTER"
NS="${NAMESPACE:-reliefgrid}"
RELEASE="${APP_RELEASE:-reliefgrid}"
log() { printf '%s cluster: %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
restore_a15() {
	git checkout -q -- docs/evidence/A1.5 2>/dev/null || true
	git clean -fdq -- docs/evidence/A1.5 2>/dev/null || true
}
trap restore_a15 EXIT

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER" &&
	kubectl --context "$CTX" -n "$NS" get sts -l app.kubernetes.io/name=storage -o name 2>/dev/null | grep -q .; then
	echo "cluster: $CLUSTER already runs storage; make cluster-down first" >&2
	exit 1
fi
log "cluster and images"
make -s cluster-up
make -s storage-image coordinator-image tools-image
log "storage (values-load.yaml)"
helm --kube-context "$CTX" install storage deploy/helm/storage -n "$NS" --create-namespace \
	-f deploy/helm/storage/values-load.yaml --wait --wait-for-jobs --timeout 8m
log "app"
make -s app-deploy
helm --kube-context "$CTX" upgrade "$RELEASE" deploy/helm/reliefgrid -n "$NS" --reuse-values \
	-f deploy/helm/reliefgrid/values-load.yaml --wait --timeout 10m
kubectl --context "$CTX" -n "$NS" wait --for=condition=Ready pod -l 'job-name notin (storage-bootstrap-0,storage-bootstrap-1,storage-bootstrap-2)' \
	--field-selector=status.phase=Running --timeout=5m >/dev/null
bad="$(kubectl --context "$CTX" -n "$NS" get pods --field-selector=status.phase=Running -o json |
	jq -r '.items[] | select(.status.qosClass != "Guaranteed") | "\(.metadata.name) \(.status.qosClass)"')"
[ -z "$bad" ] || { echo "cluster: pods not Guaranteed:"$'\n'"$bad" >&2; exit 1; }
log "ready: every running pod is Guaranteed"

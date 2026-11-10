#!/usr/bin/env bash
# The S5.2 load deployment on a fresh kind cluster:
#   make cluster-up; storage, coordinator and tools images;
#   deploy/helm/storage with values-load.yaml (load Raft profile, Guaranteed pods);
#   make app-deploy, then the app release upgraded with values-load.yaml
#   (Guaranteed pods; image tags and everything else reused);
#   then checks every pod in the namespace is Guaranteed and Ready.
# It creates the cluster if needed and refuses one that already runs storage:
# start from `make cluster-down` (a load result needs a known starting state).
# app-deploy writes to A15_EVIDENCE_DIR; existing A1.5 evidence is untouched.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
CLUSTER="${CLUSTER:-reliefgrid}"
[ "$CLUSTER" = reliefgrid ] || {
 echo "cluster: this branch's cluster-up only supports CLUSTER=reliefgrid" >&2
 exit 2
}
CTX="kind-$CLUSTER"
NS="${NAMESPACE:-reliefgrid}"
RELEASE="${APP_RELEASE:-reliefgrid}"
log() { printf '%s cluster: %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
# Preserve existing A1.5 evidence, including untracked and ignored files.
# Keep deployment diagnostics in a separate directory even on failure.
export A15_EVIDENCE_DIR="${A15_EVIDENCE_DIR:-$(mktemp -d -t reliefgrid-load-a15.XXXXXXXX)}"
log "A1.5 deployment diagnostics: $A15_EVIDENCE_DIR"

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

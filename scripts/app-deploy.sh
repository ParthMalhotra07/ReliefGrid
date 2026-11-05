#!/usr/bin/env bash
# Development deployment only: no reset, uninstall, PVC deletion or Raft bypass.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
cluster="${CLUSTER:-reliefgrid}"
namespace="${NAMESPACE:-reliefgrid}"
release="${APP_RELEASE:-reliefgrid}"
context="kind-$cluster"
# Kind-only (kind load, docker image inspect): as scripts/kind-guard.sh, inline so the script
# stands alone.
case "${KUBE_CONTEXT:-$context}" in
kind-*) ;;
*) echo "scripts/app-deploy.sh is kind-only (kind, docker or conntrack on kind nodes, or a kind cluster rebuild); refusing context $KUBE_CONTEXT" >&2; exit 2 ;;
esac
# Demo/dev storage belongs to its standalone release. CI may explicitly use
# the full subchart in its isolated namespace.
with_storage="${APP_WITH_STORAGE:-false}"
case "$with_storage" in true|false) ;; *) echo 'APP_WITH_STORAGE must be true or false' >&2; exit 1 ;; esac
export APP_IMAGE_TAG="${APP_IMAGE_TAG:-a15-$(date -u +%Y%m%d%H%M%S)}"
export A15_EVIDENCE_DIR="${A15_EVIDENCE_DIR:-docs/evidence/A1.5}"
mkdir -p "$A15_EVIDENCE_DIR"
kn() { kubectl --request-timeout=10s --context "$context" -n "$namespace" "$@"; }
kubectl --request-timeout=10s --context "$context" get nodes -o json > "$A15_EVIDENCE_DIR/nodes.json"
python3 - "$A15_EVIDENCE_DIR/nodes.json" <<'PY'
import json, sys
nodes = json.load(open(sys.argv[1]))['items']
for role, count in [('data', 3), ('infra', 1)]:
    eligible = [n for n in nodes if n['metadata'].get('labels', {}).get('role') == role]
    assert len(eligible) == count, (role, len(eligible), count)
    assert all(any(c['type'] == 'Ready' and c['status'] == 'True' for c in n['status']['conditions']) for n in eligible)
print('kind placement inputs: three Ready data workers and one Ready infra worker')
PY
if [ "$with_storage" = false ]; then
  kn get service storage-coordinator >/dev/null
  # Do not remove storage previously installed by this app release. Migrate
  # explicitly or keep the full profile in that isolated namespace.
  prior_values=""
  releases="$(helm --kube-context "$context" list -n "$namespace" -o json)"
  if printf '%s' "$releases" | jq -e --arg name "$release" 'any(.[]; .name == $name)' >/dev/null; then
    prior_values="$(helm --kube-context "$context" get values "$release" -n "$namespace" -o json)"
  fi
  if [ -n "$prior_values" ] && printf '%s' "$prior_values" | jq -e '.storage.enabled == true' >/dev/null; then
    echo 'app release currently owns storage; refusing to remove it; use a standalone-storage namespace or APP_WITH_STORAGE=true' >&2
    exit 1
  fi
fi
pvc="data-$release-rabbitmq-0"
prior_pvc="$(kn get pvc "$pvc" --ignore-not-found -o jsonpath='{.metadata.uid}')"
make app-images
components=(api matcher ui)
if [ "$with_storage" = true ]; then
  components+=(storagenode coordinator)
  for component in storagenode coordinator; do
    image="reliefgrid/$component:$APP_IMAGE_TAG"
    docker build --progress=plain --provenance=false -f "cmd/$component/Dockerfile" -t "$image" .
  done
fi
: > "$A15_EVIDENCE_DIR/deployed-image-ids.txt"
for component in "${components[@]}"; do
  image="reliefgrid/$component:$APP_IMAGE_TAG"
  kind load docker-image --name "$cluster" "$image"
  docker image inspect "$image" --format '{{.Id}}' | \
    awk -v image="$image" '{print image " " $0}' >> "$A15_EVIDENCE_DIR/deployed-image-ids.txt"
done
bash scripts/app-chart-deps.sh
helm --kube-context "$context" upgrade --install "$release" deploy/helm/reliefgrid \
  -n "$namespace" --create-namespace -f deploy/helm/reliefgrid/values-app-kind.yaml \
  --set storage.enabled="$with_storage" \
  --set-string app.api.image.tag="$APP_IMAGE_TAG" \
  --set-string app.matcher.image.tag="$APP_IMAGE_TAG" \
  --set-string app.ui.image.tag="$APP_IMAGE_TAG" \
  --set-string storage.image.tag="$APP_IMAGE_TAG" \
  --set-string storage.coordinator.image.tag="$APP_IMAGE_TAG" \
  --wait --wait-for-jobs --timeout 10m
current_pvc="$(kn get pvc "$pvc" -o jsonpath='{.metadata.uid}')"
if [ -n "$prior_pvc" ] && [ "$prior_pvc" != "$current_pvc" ]; then echo 'broker PVC identity changed' >&2; exit 1; fi
printf 'broker_pvc_uid=%s preserved=%s\n' "$current_pvc" "$([ -n "$prior_pvc" ] && echo true || echo new)" | tee "$A15_EVIDENCE_DIR/broker-pvc.txt"
KUBE_CONTEXT="$context" NAMESPACE="$namespace" bash scripts/placement-check.sh | tee "$A15_EVIDENCE_DIR/placement.log"
for shard in 0 1 2; do
  states="$(mktemp)"
  for replica in 0 1 2; do
    timeout 10 kubectl --context "$context" -n "$namespace" exec "storage-shard-$shard-$replica" \
      -c storagenode -- /storagectl status >> "$states"
  done
  jq -s -e '([.[] | select(.state == "Leader")] | length) == 1 and ([.[].leaderId] | unique | length) == 1 and all(.[]; (.leaderId | length) > 0)' "$states" >/dev/null
  jq -s --argjson shard "$shard" '{shard:$shard, replicas:map({serverId,state,leaderId,appliedIndex})}' "$states" > "$A15_EVIDENCE_DIR/shard-$shard-status.json"
  rm -f "$states"
  printf 'shard %s: three replicas, one agreed leader\n' "$shard"
done
kn get pods -o wide > "$A15_EVIDENCE_DIR/pods.txt"
kn get deployments -o json > "$A15_EVIDENCE_DIR/deployments.json"
kn get service,ingress -o wide > "$A15_EVIDENCE_DIR/routes.txt"
printf 'A1.5 APP DEPLOY: PASS\n'

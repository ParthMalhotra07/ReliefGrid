#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
cluster="${CLUSTER:-reliefgrid}"
namespace="${NAMESPACE:-reliefgrid}"
release="${METRICS_RELEASE:-observability}"
context="kind-$cluster"
kubectl --context "$context" -n "$namespace" get deployment reliefgrid-api reliefgrid-matcher -o json | python3 -c 'import json,sys;d=json.load(sys.stdin);assert all(any(e.get("name")=="METRICS_ADDR" for e in x["spec"]["template"]["spec"]["containers"][0].get("env",[])) for x in d["items"]),"Deploy the A4.2 app images/chart before metrics-up"'
infra="$(kubectl --context "$context" get nodes -l role=infra -o json | python3 -c 'import json,sys;n=json.load(sys.stdin)["items"];assert len(n)==1;print(n[0]["metadata"]["name"])')"
architecture="$(kubectl --context "$context" get node "$infra" -o jsonpath='{.status.nodeInfo.architecture}')"
[ "$architecture" = amd64 ] || { echo 'A4.2 monitoring images are pinned for the local Linux/amd64 kind profile' >&2;exit 1; }
for definition in "prom/prometheus:PROMETHEUS" "grafana/grafana:GRAFANA";do
 repository="${definition%:*}"; pin="${definition##*:}"
 version="$(make -s print-"${pin}_VERSION")"
 digest="$(make -s print-"${pin}_IMAGE_DIGEST")"
 [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]]
 image="$repository:$version@$digest"
 docker pull "$image"
 # Keep a named tag in the Docker archive: digest-only archives create
 # temporary import names that containerd may normalize inconsistently.
 tag="${image%@*}"
 docker tag "$image" "$tag"
 kind load docker-image --name "$cluster" --nodes "$infra" "$tag"
done
helm --kube-context "$context" upgrade --install "$release" deploy/helm/observability -n "$namespace" --wait --timeout 5m
echo 'A4.2 private observability installed (ephemeral six-hour/512MB Prometheus retention).'

#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
cluster="${CLUSTER:-reliefgrid}"
context="kind-$cluster"
# This profile has kind-only kubelet TLS settings. Never apply it to cloud.
kubectl --context "$context" get nodes >/dev/null
cache="${XDG_CACHE_HOME:-$HOME/.cache}/reliefgrid-autoscaling"
mkdir -p "$cache"
fetch(){
 local url="$1" file="$2" expected="$3"
 if [ ! -f "$file" ];then curl --fail --location --silent --show-error --connect-timeout 10 --max-time 60 "$url" -o "$file";fi
 printf '%s  %s\n' "$expected" "$file" | sha256sum -c -
}
ms_version="$(make -s print-METRICS_SERVER_CHART_VERSION)"
keda_version="$(make -s print-KEDA_CHART_VERSION)"
ms="$cache/metrics-$ms_version.tgz"
keda="$cache/keda-$keda_version.tgz"
fetch "https://github.com/kubernetes-sigs/metrics-server/releases/download/metrics-server-helm-chart-$ms_version/metrics-server-$ms_version.tgz" "$ms" "$(make -s print-METRICS_SERVER_CHART_SHA256)"
fetch "https://kedacore.github.io/charts/keda-$keda_version.tgz" "$keda" "$(make -s print-KEDA_CHART_SHA256)"
helm --kube-context "$context" upgrade --install metrics-server "$ms" -n kube-system -f deploy/helm/autoscaling/metrics-server-values.yaml --wait --timeout 3m
# Install alongside the app so secret access can be restricted to this namespace
# and to the existing broker Secret; no dispatcher/operator credential is copied.
helm --kube-context "$context" upgrade --install keda "$keda" -n reliefgrid -f deploy/helm/autoscaling/keda-values.yaml --wait --timeout 5m
kubectl --context "$context" wait --for=condition=Available apiservice/v1beta1.metrics.k8s.io --timeout=60s
kubectl --context "$context" wait --for=condition=Available apiservice/v1beta1.external.metrics.k8s.io --timeout=60s
echo 'A4.3 pinned local autoscaling controllers ready'

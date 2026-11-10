#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
context="kind-${CLUSTER:-reliefgrid}"
enabled="${1:-true}"
case "$enabled" in true|false) ;; *) echo 'expected true or false' >&2; exit 1;; esac
# Refuse to reapply an app-owned storage subchart from this application goal.
values="$(helm --kube-context "$context" get values reliefgrid -n reliefgrid -o json)"
if printf '%s' "$values" | jq -e '.storage.enabled == true' >/dev/null; then
  echo 'autoscaling requires the standalone storage release (app storage.enabled=false)' >&2
  exit 1
fi
if [ "$enabled" = true ]; then
  kubectl --context "$context" wait --for=condition=Available apiservice/v1beta1.metrics.k8s.io --timeout=30s
  kubectl --context "$context" wait --for=condition=Available apiservice/v1beta1.external.metrics.k8s.io --timeout=30s
  kubectl --context "$context" get crd scaledobjects.keda.sh >/dev/null
fi
bash scripts/app-chart-deps.sh
# Reuse the existing release's images and storage.enabled=false ownership. This
# profile changes controllers, never installs storage or resets application data.
helm --kube-context "$context" upgrade reliefgrid deploy/helm/reliefgrid -n reliefgrid \
  --reset-then-reuse-values --set app.autoscaling.enabled="$enabled" --wait --timeout 5m
if [ "$enabled" = true ]; then
  kubectl --context "$context" -n reliefgrid wait --for=condition=Ready scaledobject/reliefgrid-matcher --timeout=90s
fi

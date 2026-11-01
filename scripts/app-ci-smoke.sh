#!/usr/bin/env bash
# Additional app verification after CI has checked the prebuilt storage images.
set -euo pipefail
cd "$(dirname "$0")/.."
: "${EVIDENCE_DIR:?EVIDENCE_DIR: CI evidence root}"
: "${SMOKE_RUN_ID:?SMOKE_RUN_ID: CI run identifier}"
[[ "$SMOKE_RUN_ID" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,100}$ ]]
export APP_WITH_STORAGE=true
export NAMESPACE="${APP_CI_NAMESPACE:-reliefgrid-app-smoke}"
export A15_EVIDENCE_DIR="$EVIDENCE_DIR/app-$SMOKE_RUN_ID"
mkdir -p "$A15_EVIDENCE_DIR/diagnostics"
diagnostics() {
  local result=$?
  kubectl --request-timeout=10s --context "kind-${CLUSTER:-reliefgrid}" -n "$NAMESPACE" \
    get pods -o wide > "$A15_EVIDENCE_DIR/diagnostics/pods.txt" 2>&1 || true
  kubectl --request-timeout=10s --context "kind-${CLUSTER:-reliefgrid}" -n "$NAMESPACE" \
    get events --sort-by=.lastTimestamp > "$A15_EVIDENCE_DIR/diagnostics/events.txt" 2>&1 || true
  return "$result"
}
trap diagnostics EXIT
make app-deploy 2>&1 | tee "$A15_EVIDENCE_DIR/app-deploy.log"
make e2e-smoke 2>&1 | tee "$A15_EVIDENCE_DIR/e2e-smoke.log"
printf 'A1.5 CI APP SMOKE: PASS\n'

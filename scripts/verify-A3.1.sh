#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
case "${1:---static}" in
 --static)
  go test -race -count=1 -run '^(TestAppDemoClient|TestAppDemosRequireDeployment|TestRunServesRealFSMAndStops)' ./scripts/... ./cmd/api/...
  go test -race -count=1 -tags integration -run '^TestAppDemoProfileHasThirtySecondEvictionTolerations$' ./scripts/...
  ;;
 --live)
  cluster="$(make -s print-CLUSTER)"
  if [ "${KUBE_CONTEXT:-kind-$cluster}" != "kind-$cluster" ]; then echo "Verify refuses a context other than kind-$cluster" >&2; exit 1; fi
  run="${A31_RUN_ID:-A3.1-$(date -u +%Y%m%dT%H%M%SZ)}"
  for round in 1 2; do
   echo "A3.1 round $round: clean reset of kind-$cluster"
   RESET_CONFIRM="$cluster" make demo-reset
   for demo in 2 3 5 8 9; do
    DEMO_RUN_ID="$run/round-$round" make "demo-$demo"
   done
  done
  ;;
 *) echo 'usage: verify-A3.1.sh --static|--live' >&2; exit 1 ;;
esac
echo 'A3.1 VERIFY: ALL PASS'

#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
go test -race -tags integration -count=1 -run 'TestAutoscaling|TestAppHelmFullDeployment|TestAppBrokerOnlyProfileRemainsIndependent' ./scripts/...
PYTHONDONTWRITEBYTECODE=1 python3 scripts/verify-A4.3-test.py
node --test scripts/autoscaling-client-test.mjs
for script in scripts/autoscaling-{install,profile,load-tool}.sh; do bash -n "$script"; done
echo 'A4.3 STATIC VERIFY: PASS'

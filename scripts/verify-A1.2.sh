#!/usr/bin/env bash
# A1.2: geography and intent-first workflow against the real SQLite memstore.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"
echo '=== A1.2 Verify: geography and matching ==='
go test -race -count=1 -v ./internal/geo/... ./internal/workflow/...
echo 'A1.2 VERIFY: ALL PASS'

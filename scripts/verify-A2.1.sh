#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
echo '=== A2.1 Verify: cancellation/completion ==='
go test -race -count=1 -run '^TestCleanup' -v ./internal/workflow/...
go test -race -count=1 ./cmd/api/... ./internal/api/... ./internal/mq/... ./cmd/matcher/...
echo 'A2.1 VERIFY: ALL PASS'

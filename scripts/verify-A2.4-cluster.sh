#!/usr/bin/env bash
# Real cluster A2.4 fault/closure verification. Only kind-reliefgrid is allowed.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
run="${A24_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
out="$ROOT/docs/evidence/A2.4/cluster/$run"
mkdir -p "$out"
case "${1:---deploy}" in
 --deploy) A15_EVIDENCE_DIR="$out/deployment" RESET_CONFIRM=reliefgrid make demo-reset ;;
 --existing) echo 'Using the just-deployed current A2.4 application on kind-reliefgrid' ;;
 *) echo 'usage: verify-A2.4-cluster.sh [--deploy|--existing]' >&2;exit 1 ;;
esac
kubectl --context kind-reliefgrid -n reliefgrid rollout status deployment/reliefgrid-matcher --timeout=90s
# Test worker copies pinned app bases; it is not shipped in the application image.
go build -o "$out/recovery-probe.tmp" ./scripts/reconcile-crash
# Binary is temporary, never part of evidence staged for the PR.
tmp="$(mktemp -d)"
mv "$out/recovery-probe.tmp" "$tmp/recovery-probe"
pid=""
cleanup(){ if [ -n "$pid" ];then kill "$pid" 2>/dev/null || true;wait "$pid" 2>/dev/null || true;fi;rm -rf "$tmp"; }
trap cleanup EXIT
docker build -f scripts/reconcile-crash/Dockerfile -t reliefgrid/reconcile-crash:a24 . > "$out/image-build.log" 2>&1
kind load docker-image reliefgrid/reconcile-crash:a24 --name reliefgrid > "$out/image-load.log" 2>&1
port="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
kubectl --context kind-reliefgrid -n reliefgrid port-forward svc/storage-coordinator "$port:8080" > "$tmp/forward.log" 2>&1 &
pid=$!
export DEMO_COORD_URL="http://127.0.0.1:$port"
ready=false
for _ in $(seq 50);do if curl --fail --silent --max-time 1 "$DEMO_COORD_URL/internal/ring" -o "$tmp/ring.json";then ready=true;break;fi;sleep .1;done
[ "$ready" = true ] || { cat "$tmp/forward.log" >&2;exit 1; }
PYTHONDONTWRITEBYTECODE=1 python3 scripts/verify-reconcile.py --out "$out" --probe "$tmp/recovery-probe"
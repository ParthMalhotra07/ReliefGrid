#!/usr/bin/env bash
# Keep one loopback operator endpoint while the coordinator pod is replaced.
# Port-forward uses the pod loopback permitted by the existing NetworkPolicy.
set -euo pipefail
port="${1:?loopback port required}"
[[ "$port" =~ ^[0-9]+$ ]] || exit 1
child=""
cleanup(){
 if [ -n "$child" ];then kill "$child" 2>/dev/null || true;wait "$child" 2>/dev/null || true;fi
}
trap cleanup EXIT
trap 'exit 143' TERM INT
while :;do
 kubectl --context kind-reliefgrid -n reliefgrid port-forward --address=127.0.0.1 svc/storage-coordinator "$port:8080" &
 child=$!
 wait "$child" || true
 child=""
 sleep .1
done

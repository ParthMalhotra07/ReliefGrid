#!/usr/bin/env bash
# demo-harness.sh <repo root>: drives the demo helpers' internal calls (scripts/demos/kube.sh
# cput, cget, stale_get and demo 1's replica reads) against a fake kubectl and a fake curl
# that logs each call ($CURL_LOG). Run on main's files it gives the off-mode expectation
# (demo-calls.off.txt); internal_auth_env tests run it on this tree.
set -uo pipefail
ROOT="$1"
REPO_ROOT="$ROOT" CLUSTER=reliefgrid
DEMO_TMP="$(mktemp -d)"
export DEMO_TMP
DEMO_CLEANUPS=()
demo_on_exit() { DEMO_CLEANUPS+=("$(printf '%q ' "$@")"); }
retry_on() { shift 2; "$@"; }
# shellcheck source=scripts/demos/kube.sh
. "$ROOT/scripts/demos/kube.sh"
next_port=7000
forward() { next_port=$((next_port + 1)); FWD_PORT="$next_port"; }
if coord_forward; then echo "coord_forward ok"; else echo "coord_forward failed: ${AUTH_WHY:-}"; fi
if [ -n "$COORD_PORT" ]; then
	cput "cfg:demo" "op-demo-1" '{"a":1}' >/dev/null
	cget "cfg:demo" >/dev/null
	cget "cfg:demo" strong >/dev/null
	stale_get 7100 "cfg:demo" "$DEMO_TMP/stale.json" >/dev/null
	eval "$(sed -n '/^read_replicas() {/,/^}/p' "$ROOT/scripts/demos/demo-1.sh")"
	declare -A port=([storage-shard-1-0]=7201 [storage-shard-1-1]=7202 [storage-shard-1-2]=7203)
	s=1 id=demo-id version=3
	replica_reads_agree() { cat >/dev/null; }
	read_replicas
fi
for c in "${DEMO_CLEANUPS[@]}"; do eval "$c"; done
echo "after cleanup: INTERNAL_AUTH_TOKEN_FILE=${INTERNAL_AUTH_TOKEN_FILE-unset}"
rm -rf "$DEMO_TMP"

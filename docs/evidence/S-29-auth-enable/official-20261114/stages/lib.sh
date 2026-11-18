# Shared helpers for the official auth-run stages.
set -euo pipefail
. "$R/env.sh"
cd "$REPO"
. scripts/internal-auth-env.sh
# quiet_grep: grep with its output discarded. Unlike grep -q it reads all its input, so under
# pipefail the writer is never killed by SIGPIPE (exit 141) when the match comes early.
quiet_grep() { grep "$@" >/dev/null; }
log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }
ok() { printf 'PASS  %s\n' "$*"; }
die() { printf 'FAIL  %s\n' "$*"; exit 1; }
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
PFS=()
pf() { # pf <target> <remote port>: sets PF_PORT
	PF_PORT="$(free_port)"
	$K port-forward "$1" "$PF_PORT:$2" >/dev/null 2>&1 &
	PFS+=($!)
	for _ in $(seq 100); do (exec 3<>"/dev/tcp/127.0.0.1/$PF_PORT") 2>/dev/null && return 0; sleep 0.1; done
	return 1
}
stop_pfs() { [ "${#PFS[@]}" -gt 0 ] && kill "${PFS[@]}" 2>/dev/null; PFS=(); return 0; }
trap 'stop_pfs; internal_auth_cleanup' EXIT
# refusals <since RFC3339>: every "internal auth refused" line logged since then by the
# coordinator and the nine storage pods, as JSON lines {pod, line}.
refusals() {
	local p
	for p in $($K get pods -l 'app.kubernetes.io/component in (coordinator,storagenode)' -o jsonpath='{.items[*].metadata.name}'); do
		# A pod with no refusal is not an error: grep's exit 1 must not fail the caller's pipeline.
		$K logs "$p" --all-containers --since-time="$1" 2>/dev/null | grep 'internal auth refused' | sed "s|^|$p |" || true
	done
	return 0
}
# in_cluster_refusals <since>: the refusals whose peer is not this host's port-forward.
in_cluster_refusals() { refusals "$1" | grep -v -E 'peer=("?)(127\.0\.0\.1|\[::1\]):|"peer":"(127\.0\.0\.1|\[::1\]):' || true; } # none is not an error
# no_token_in_logs <token file>...: no pod log of the release contains any of the tokens.
no_token_in_logs() {
	local p f
	for p in $($K get pods -o jsonpath='{.items[*].metadata.name}'); do
		for f in "$@"; do
			$K logs "$p" --all-containers 2>/dev/null | quiet_grep -F -f "$f" && { echo "a token appears in the logs of $p"; return 1; }
		done
	done
	return 0
}
optok() { $K get secret storage-operator-token -o jsonpath='{.data.token}' | base64 -d; }
# leader_of <shard>: the pod that is Leader of that shard
leader_of() {
	local p
	for p in storage-shard-$1-0 storage-shard-$1-1 storage-shard-$1-2; do
		[ "$(timeout 10 $K exec "$p" -c storagenode -- /storagectl status 2>/dev/null | jq -r .state)" = Leader ] && { echo "$p"; return 0; }
	done
	return 1
}

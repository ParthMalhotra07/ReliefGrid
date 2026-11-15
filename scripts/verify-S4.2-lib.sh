#!/usr/bin/env bash
# verify-S4.2-lib.sh: shard-state helpers of scripts/verify-S4.2.sh, sourced after
# chaos/scripts/lib.sh (whose st and field these replace). Kept separate so that
# scripts/verify_s42_lib_test.go runs them against a fake kubectl.
# Needs CTX, NS, SHARD and kn (chaos/scripts/lib.sh).

pods() { printf 'storage-shard-%s-0 storage-shard-%s-1 storage-shard-%s-2' "$SHARD" "$SHARD" "$SHARD"; }
st() { timeout 5 kubectl --context "$CTX" -n "$NS" exec "$1" -c storagenode -- /storagectl status 2>/dev/null; }
field() { jq -r "$2 // empty" <<<"$1" 2>/dev/null; }
bfield() { jq -r "$2" <<<"$1" 2>/dev/null; } # a boolean: jq's // would turn false into empty
leader_pod() { local p; for p in $(pods); do [ "$(field "$(st "$p")" .state)" = Leader ] && echo "$p" && return; done; }
voter_count() { jq -r '[.configuration.servers[]? | select(.suffrage == "Voter")] | length' <<<"$(st "$(leader_pod)")" 2>/dev/null; }
# wait_for <seconds> <command...>: until it succeeds
wait_for() {
	local i n=$(($1 * 2))
	shift
	for ((i = 0; i < n; i++)); do "$@" >/dev/null 2>&1 && return 0; sleep 0.5; done
	return 1
}
group_ok() { # three committed voters, all caught up, every member naming the same leader
	local p s l lead=""
	for p in $(pods); do
		s="$(st "$p")"
		[ "$(field "$s" .caughtUp)" = true ] || return 1
		l="$(field "$s" .leaderId)"
		[ -n "$l" ] || return 1 # a member that knows no leader: not settled (an empty first sample once passed)
		[ -z "$lead" ] && lead="$l"
		[ "$l" = "$lead" ] || return 1
	done
	[ "$(voter_count)" = 3 ]
}
# await_leader <seconds>: print the member that reports Leader, polling every 0.5 s; exit 1,
# printing nothing, if none does in time. Deleting the leader leaves the shard leaderless for
# a few seconds (heartbeat and election timeouts): a single leader_pod read can be empty.
await_leader() {
	local i n=$(($1 * 2)) l
	for ((i = 0; i < n; i++)); do
		l="$(leader_pod)"
		[ -n "$l" ] && { echo "$l"; return 0; }
		sleep 0.5
	done
	return 1
}
# dial_peer <target> <seconds>: a member to dial <target> from: the leader, or another member
# when the target leads. Exit 1, printing nothing, if no leader within <seconds>: a dial from an
# empty pod name fails, which a negative check would read as "unreachable".
dial_peer() {
	local l p
	l="$(await_leader "$2")" || return 1
	[ "$l" != "$1" ] && { echo "$l"; return 0; }
	for p in $(pods); do [ "$p" != "$1" ] && { echo "$p"; return 0; }; done
	return 1
}
pod_uid() { kn get pod "$1" -o json 2>/dev/null | jq -r '.metadata.uid // empty' 2>/dev/null; }
# pod_replaced <pod> <old uid>: the pod is a new object (another UID), not terminating, and
# Ready. After `delete pod --wait=false` the old pod still answers for a moment, with its
# old serverId; it must not count as the restart.
pod_replaced() {
	local j
	[ -n "$2" ] || return 1
	j="$(kn get pod "$1" -o json 2>/dev/null)" || return 1
	jq -e --arg old "$2" '(.metadata.uid // "") as $u | $u != "" and $u != $old and .metadata.deletionTimestamp == null and
		any(.status.conditions[]?; .type == "Ready" and .status == "True")' <<<"$j" >/dev/null 2>&1
}

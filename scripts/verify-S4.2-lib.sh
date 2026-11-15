#!/usr/bin/env bash
# verify-S4.2-lib.sh: shard-state helpers of scripts/verify-S4.2.sh, sourced after
# chaos/scripts/lib.sh (whose st and field these replace). Kept separate so that
# scripts/verify_s42_lib_test.go runs them against a fake kubectl.
# Needs CTX, NS and SHARD.

# Deadlines. A wait's bound is elapsed time from the start of the wait, including the time
# spent inside each kubectl call, not a count of polls. A wait sets _DL (microseconds since
# the epoch; empty: none) with `local _DL="${_DL:-}"; deadline <seconds>`, which only ever
# tightens an outer deadline, so nested waits share one. Every call is given at most the time
# left (budget), and is checked against the deadline before and after: nothing that ends after
# the deadline counts.
_now() { _NOW=${EPOCHREALTIME//[!0-9]/}; }
deadline() {
	local d
	_now
	d=$((_NOW + $1 * 1000000))
	if [ -z "${_DL:-}" ] || [ "$d" -lt "$_DL" ]; then _DL=$d; fi
}
due() { [ -n "${_DL:-}" ] && _now && [ "$_NOW" -ge "$_DL" ]; }
# budget <cap ms>: the seconds a call may take: the cap, or the time left if less. Exit 1 once
# less than a millisecond is left (timeout 0 would mean no limit).
budget() {
	local us=$(($1 * 1000)) left
	if [ -n "${_DL:-}" ]; then
		_now
		left=$((_DL - _NOW))
		[ "$left" -ge 1000 ] || return 1
		[ "$left" -lt "$us" ] && us=$left
	fi
	printf '%d.%06d' $((us / 1000000)) $((us % 1000000))
}
nap() { local t; t="$(budget "$1")" || return 1; sleep "$t"; } # nap <ms>, within the deadline
kc() { local t; t="$(budget 10000)" || return 1; timeout "$t" kubectl --context "$CTX" -n "$NS" "$@"; }

pods() { printf 'storage-shard-%s-0 storage-shard-%s-1 storage-shard-%s-2' "$SHARD" "$SHARD" "$SHARD"; }
st() {
	local t
	t="$(budget 5000)" || return 1
	timeout "$t" kubectl --context "$CTX" -n "$NS" exec "$1" -c storagenode -- /storagectl status 2>/dev/null
}
field() { jq -r "$2 // empty" <<<"$1" 2>/dev/null; }
bfield() { jq -r "$2" <<<"$1" 2>/dev/null; } # a boolean: jq's // would turn false into empty
leader_pod() {
	local p
	for p in $(pods); do
		due && return 1
		[ "$(field "$(st "$p")" .state)" = Leader ] && ! due && echo "$p" && return 0
	done
	return 1
}
voter_count() { jq -r '[.configuration.servers[]? | select(.suffrage == "Voter")] | length' <<<"$(st "$(leader_pod)")" 2>/dev/null; }
# wait_for <seconds> <command...>: until it succeeds, within <seconds> of elapsed time. A
# command that is not a shell function runs under timeout with the time left; a function is
# bounded through the helpers it calls.
wait_for() {
	local _DL="${_DL:-}" t
	deadline "$1"
	shift
	while ! due; do
		if [ "$(type -t "$1")" = function ]; then
			"$@" >/dev/null 2>&1 && ! due && return 0
		else
			t="$(budget 600000)" || return 1
			timeout "$t" "$@" >/dev/null 2>&1 && ! due && return 0
		fi
		nap 500 || return 1
	done
	return 1
}
group_ok() { # three committed voters, all caught up, every member naming the same leader
	local p s l lead=""
	for p in $(pods); do
		due && return 1
		s="$(st "$p")"
		[ "$(field "$s" .caughtUp)" = true ] || return 1
		l="$(field "$s" .leaderId)"
		[ -n "$l" ] || return 1 # a member that knows no leader: not settled (an empty first sample once passed)
		[ -z "$lead" ] && lead="$l"
		[ "$l" = "$lead" ] || return 1
	done
	[ "$(voter_count)" = 3 ] && ! due
}
# group_sample: one sample of a settled group: print "<leader pod> <term>", or exit 1. Every
# member admitted, caught up (its FSM has applied every command up to the leader's commit
# index) and with Raft applied up to its commit index; every member naming the same leader in
# the same term; exactly one Leader, whose serverId is that leader, with three committed
# voters. (raftAppliedIndex, not appliedIndex, is compared with commitIndex: the new leader's
# no-op and configuration entries never reach the FSM, so on an idle shard the FSM's index
# stays below the commit index after an election; caughtUp already allows for that.)
group_sample() {
	local p r state id lid term voters committed lead="" lterm="" leader="" leaders=0
	for p in $(pods); do
		due && return 1
		r="$(jq -r 'select(.admitted == true and .caughtUp == true and (.leaderId // "") != ""
			and .commitIndex != null and .raftAppliedIndex == .commitIndex) |
			[.state, (.serverId // ""), .leaderId, .term,
			 ([.configuration.servers[]? | select(.suffrage == "Voter")] | length),
			 (.configuration.committed == true)] | map(tostring) | join("|")' <<<"$(st "$p")" 2>/dev/null)"
		[ -n "$r" ] || return 1
		IFS='|' read -r state id lid term voters committed <<<"$r" # not a tab: empty fields would merge
		[ -z "$lead" ] && lead="$lid" lterm="$term"
		[ "$lid" = "$lead" ] && [ "$term" = "$lterm" ] || return 1
		if [ "$state" = Leader ]; then
			[ "$id" = "$lid" ] && [ "$voters" = 3 ] && [ "$committed" = true ] || return 1
			leader="$p" leaders=$((leaders + 1))
		fi
	done
	[ "$leaders" = 1 ] && ! due && echo "$leader $lterm"
}
# group_settled: two consecutive settled samples, 0.5 s apart, with the same leader and term.
group_settled() {
	local a b
	a="$(group_sample)" || return 1
	nap 500 || return 1
	b="$(group_sample)" || return 1
	[ "$a" = "$b" ] && echo "$a"
}
# await_leader <seconds>: print the member that reports Leader, polling every 0.5 s; exit 1,
# printing nothing, if none does in time. Deleting the leader leaves the shard leaderless for
# a few seconds (heartbeat and election timeouts): a single leader_pod read can be empty.
await_leader() {
	local _DL="${_DL:-}" l
	deadline "$1"
	while ! due; do
		l="$(leader_pod)" && ! due && { echo "$l"; return 0; }
		nap 500 || return 1
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
pod_uid() { kc get pod "$1" -o json 2>/dev/null | jq -r '.metadata.uid // empty' 2>/dev/null; }
# pod_replaced <pod> <old uid>: the pod is a new object (another UID), not terminating, and
# Ready. After `delete pod --wait=false` the old pod still answers for a moment, with its
# old serverId; it must not count as the restart.
pod_replaced() {
	local j
	[ -n "$2" ] || return 1
	j="$(kc get pod "$1" -o json 2>/dev/null)" || return 1
	jq -e --arg old "$2" '(.metadata.uid // "") as $u | $u != "" and $u != $old and .metadata.deletionTimestamp == null and
		any(.status.conditions[]?; .type == "Ready" and .status == "True")' <<<"$j" >/dev/null 2>&1
}
# step_off <pod> <seconds>: make sure <pod> is not the leader of a settled group before the
# wipe. If it leads, it hands leadership to another voter (storagectl transfer-leader, S3.1:
# answers once it no longer leads). Either way, wait until another member leads a settled group
# (group_settled). Prints "<leader> transferred|unchanged"; exit 1, printing nothing, if there
# is no leader, the transfer fails, or no settled group led by another member appears, all
# within one deadline of <seconds>. Deleting a leader forces an election on its own, which a
# check for "no election caused by the unadmitted pod" must not count; and a group that is
# not settled may hold an election for reasons of its own.
step_off() {
	local _DL="${_DL:-}" l r how=unchanged
	deadline "$2"
	l="$(await_leader "$2")" || return 1
	if [ "$l" = "$1" ]; then
		kc exec "$1" -c storagenode -- /storagectl transfer-leader >/dev/null 2>&1 || return 1
		due && return 1
		how=transferred
	fi
	while ! due; do
		r="$(group_settled)" && [ "${r%% *}" != "$1" ] && ! due && { echo "${r%% *} $how"; return 0; }
		nap 500 || return 1
	done
	return 1
}

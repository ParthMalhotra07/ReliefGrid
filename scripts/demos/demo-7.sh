#!/usr/bin/env bash
# demo-7 (owner S, goal S3.2): v3.1 §28 row 7.
#
# What to show:   Restart the failed node and show replay/catch-up
# Acceptance:     Recovered node becomes data-ready; equal committed state after settling
#
# Seed/reset: continues from demo 6 when <evidence dir>/demo-6.state names a pod that is still
# stopped (the gate runs 6 then 7 in one run directory). Otherwise it stops shard DEMO7_SHARD's
# (default 1) leader itself, as demo 6 does (cordon its worker, delete the pod). Either way it
# then writes DEMO7_WRITES (100) keys demo7-<seed>-<run>-<i> owned by that shard while the pod
# is down, so the restarted replica has entries to replay. DEMO7_SEED (7) names the keys.
# The worker is uncordoned on every exit path.
#
# Assertions:
#   1. The node is stopped at the start and its shard has a leader among the survivors.
#   2. All DEMO7_WRITES writes commit while it is down (the majority serves them).
#   3. Restarting it (uncordon) brings the pod Running on its own worker again (its PV is
#      there) within DEMO7_REJOIN_SECONDS (120).
#   4. Replay/catch-up: polled from its first answer, its appliedIndex rises to at least the
#      shard's commit index taken after the writes, and it reports caughtUp.
#   5. Data-ready: the pod's Ready condition (readiness is gated on catch-up, S3.1) is True.
#   6. Same identity: the same serverId (podName/incarnation) as before the stop, and one
#      leader that all three replicas name.
#   7. Equal committed state after settling: the three replicas reach one appliedIndex with
#      identical dumps (storagectl dump sha256).
#   8. The writes made while it was down are on the recovered replica: stale reads served by
#      that pod's own FSM return every one at version 1.
#   9. The coordinator reports all nine nodes UP again.
. "$(dirname "$0")/lib.sh"
demo_begin 7 "Restart the stopped node; replay, catch-up and equal committed state"
# shellcheck source=scripts/demos/kube.sh
. "$(dirname "$0")/kube.sh"

SEED="${DEMO7_SEED:-7}"
WRITES="${DEMO7_WRITES:-100}"
REJOIN_SECONDS="${DEMO7_REJOIN_SECONDS:-120}"
PREFIX="demo7-$SEED-$(date +%s)"
STATE="$(dirname "$DEMO_EVIDENCE")/demo-6.state"

SHARD="" POD="" NODE="" SERVER_ID="" PENDING_UID="" from=""
if [ -f "$STATE" ]; then
	# shellcheck disable=SC1090
	. "$STATE"
	from="demo 6 ($STATE)"
fi
SHARD="${SHARD:-${DEMO7_SHARD:-1}}"
demo_reset "${from:+continues from $from: }shard $SHARD's ${POD:-leader} stopped; $WRITES writes under $PREFIX while it is down (seed $SEED); restart by uncordoning its worker"

# Preflight differs from the other demos: one storage pod is expected to be down.
coord_forward || { fail "preflight: cannot reach the coordinator"; demo_end; }
pass "preflight: coordinator reachable"
# NODE is read at exit (the stop below may set it): the worker is uncordoned on every exit path.
restore() { [ -n "$NODE" ] && demo_uncordon "$NODE"; }
demo_on_exit restore

# --- the stop, when demo 6 did not leave one ---------------------------------------------------
if [ -z "$POD" ] || ! still_stopped "$POD" "$PENDING_UID"; then
	[ -n "$NODE" ] && demo_uncordon "$NODE" # a stale hand-off: undo its cordon first
	POD="" NODE="" from=""
	if stop_leader_pod "$SHARD"; then
		POD="$STOP_POD" NODE="$STOP_NODE" SERVER_ID="$STOP_ID" PENDING_UID="$STOP_UID"
		pass "stop: $POD ($SERVER_ID) deleted with $NODE cordoned; its replacement is Pending"
	else
		NODE="$STOP_NODE"
		fail "stop: shard $SHARD's leader is stopped and stays Pending" "$STOP_WHY"
		demo_end
	fi
else
	pass "stop: continuing from demo 6: $POD is still Pending with $NODE cordoned"
fi

# --- 1: stopped, with a surviving leader -----------------------------------------------------------
ready="$(kn get pod "${POD:-none}" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)"
g="$(wait_leader "$SHARD" 15 0 "${POD:-none}")"
[ -n "$POD" ] && [ "$ready" != True ] && [ -n "$g" ] && pass "stopped: $POD is down; ${g% *} leads shard $SHARD at term ${g#* }" ||
	fail "stopped: a shard-$SHARD pod is down and the survivors have a leader" "pod ${POD:-none} Ready=$ready, group: ${g:-none}"

# --- 2: writes while it is down --------------------------------------------------------------------
keys=() bad=""
for ((i = 0; ${#keys[@]} < WRITES && i < 100000; i++)); do
	kk="$PREFIX-$i"
	[ "$(cadmin "/admin/locate/$kk" | jq -r .shard)" = "$SHARD" ] || continue
	keys+=("$kk")
	read -r code body <<<"$(cput "$kk" "op-$kk" "{\"i\":$i}")"
	[ "$code" = 200 ] || bad+="$kk: $code $body"$'\n'
done
[ "${#keys[@]}" = "$WRITES" ] && [ -z "$bad" ] && pass "writes: $WRITES writes to shard $SHARD committed while ${POD:-the pod} was down" ||
	fail "writes: $WRITES writes committed while the pod was down" "$(head -5 <<<"$bad")"
target="$(field "$(st "${g%% *}")" .commitIndex)"
demo_note "shard $SHARD commit index after the writes: ${target:-unknown} (leader ${g%% *})"

# --- 3: restart -----------------------------------------------------------------------------------
t0="$SECONDS"
demo_uncordon "$NODE"
running=""
for _ in $(seq "$REJOIN_SECONDS"); do
	j="$(kn get pod "$POD" -o json 2>/dev/null)"
	[ "$(jq -r .status.phase <<<"$j")" = Running ] && [ "$(jq -r .spec.nodeName <<<"$j")" = "$NODE" ] && { running=1; break; }
	sleep 1
done
[ -n "$running" ] && pass "restart: $NODE uncordoned; $POD is Running on it again ($((SECONDS - t0)) s)" ||
	fail "restart: the pod is Running on its worker again within ${REJOIN_SECONDS}s" "$(kn get pod "$POD" -o wide 2>&1)"

# --- 4, 5: replay, catch-up, data-ready ------------------------------------------------------------
: >"$DEMO_TMP/polls"
caught="" t0="$SECONDS"
while [ $((SECONDS - t0)) -lt "$REJOIN_SECONDS" ]; do
	s="$(st "${POD:-none}")"
	[ -n "$s" ] && echo "$((SECONDS - t0))s $(jq -c '{state, appliedIndex, commitIndex, leaderCommitIndex, restoredIndex, caughtUp}' <<<"$s")" >>"$DEMO_TMP/polls"
	[ "$(field "$s" .caughtUp)" = true ] && [ -n "$target" ] && [ "$(field "$s" .appliedIndex)" -ge "$target" ] && { caught=1; break; }
	sleep 0.5
done
demo_show "replay/catch-up of $POD (polls of /mgmt/status)" "$(cat "$DEMO_TMP/polls")"
[ -n "$caught" ] && pass "catch-up: ${POD:-the pod} replayed to appliedIndex $(field "$s" .appliedIndex) (>= commit index $target) and is caughtUp" ||
	fail "catch-up: the restarted replica reaches the shard's commit index and reports caughtUp" "target ${target:-unknown}; last poll: $(tail -1 "$DEMO_TMP/polls")"
check "data-ready: ${POD:-the pod} is Ready (readiness gated on catch-up)" kn wait --for=condition=Ready "pod/${POD:-none}" --timeout="${REJOIN_SECONDS}s"

# --- 6: same identity, one group --------------------------------------------------------------------
now_id="$(field "$(st "${POD:-none}")" .serverId)"
g="$(group_state "$SHARD")"
[ -n "$SERVER_ID" ] && [ "$now_id" = "$SERVER_ID" ] && [ -n "$g" ] && pass "identity: ${POD:-the pod} rejoined as $now_id (same disk); ${g% *} leads all three" ||
	fail "identity: same serverId as before the stop, one agreed leader" "before ${SERVER_ID:-unknown}, now ${now_id:-unknown}, group ${g:-none}"

# --- 7: equal committed state ------------------------------------------------------------------------
all_caught_up "$SHARD" 60 || fail "settle: the three replicas caught up at one appliedIndex"
d="$(dumps "$SHARD")"
demo_show "dumps of shard $SHARD after settling" "$d"
same_dumps "$SHARD" && pass "equal state: the three replicas' dumps are identical at appliedIndex $(awk 'NR == 1 { print $2 }' <<<"$d")" ||
	fail "equal state: identical dumps on all three replicas after settling" "$d"

# --- 8: the missed writes are on the recovered replica --------------------------------------------------
bad=""
if forward "pod/${POD:-none}" 8080; then
	for kk in "${keys[@]}"; do
		code="$(curl -s -m 5 -o "$DEMO_TMP/stale.json" -w '%{http_code}' "http://127.0.0.1:$FWD_PORT/internal/data/$kk?consistency=stale")"
		[ "$code" = 200 ] && [ "$(jq -r .record.version "$DEMO_TMP/stale.json")" = 1 ] && [ "$(jq -r .read.servedBy "$DEMO_TMP/stale.json")" = "$POD" ] ||
			bad+="$kk: $code $(cat "$DEMO_TMP/stale.json")"$'\n'
	done
else
	bad="no port-forward to ${POD:-the pod}"
fi
[ -z "$bad" ] && [ "${#keys[@]}" -gt 0 ] && pass "replayed data: all ${#keys[@]} writes made while it was down are served by ${POD:-the pod}'s own FSM" ||
	fail "replayed data: the recovered replica serves every write it missed" "$(head -5 <<<"$bad")"

# --- 9: coordinator ----------------------------------------------------------------------------------------
up=""
for _ in $(seq 30); do
	[ "$(cadmin /admin/nodes | jq '[.nodes[] | select(.state == "UP")] | length')" = 9 ] && { up=1; break; }
	sleep 1
done
[ -n "$up" ] && pass "coordinator: all nine nodes UP" || fail "coordinator: all nine nodes UP" "$(cadmin /admin/nodes | jq -c '[.nodes[] | {nodeId, state}]')"
rm -f "$STATE"
demo_end

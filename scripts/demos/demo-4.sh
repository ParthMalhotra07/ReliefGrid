#!/usr/bin/env bash
# demo-4 (owner S, goal S3.2): v3.1 §28 row 4.
#
# What to show:   Seed a sufficiently large deterministic dataset; show logical key counts and
#                 locate samples
# Acceptance:     All three shards hold data; replica counts shown separately
#
# Seed/reset: from the gate's clean deployment. DEMO4_SEED (default 4) and DEMO4_KEYS (300)
# fix the dataset: keys demo4-<seed>-<i>, value {"seed":<seed>,"i":<i>}, operation IDs
# demo4-<seed>-<i>. The same seed gives the same keys and values; running again replays the
# same operations (idempotent), so no other reset is needed. Other demos' data may share the
# shards, so counts are checked as "at least the seeded keys".
#
# Assertions:
#   1. Every seeded key has a placement (/admin/locate): a shard 0..2, a 16-hex-digit hash,
#      three replicas that are that shard's pods, and a leader among them.
#   2. The dataset is large enough: every shard owns some of the seeded keys.
#   3. Every seeded key reads back (strong, through the coordinator) at version 1 with its
#      value, from the shard /admin/locate names.
#   4. /admin/distribution: every shard's counts are known, it holds at least the seeded keys
#      it owns (logical records, from the leader), and it has three replicas.
#   5. Replica copies, shown separately: each shard's three replicas settle at one appliedIndex
#      with identical dumps, and each holds at least the shard's seeded keys.
. "$(dirname "$0")/lib.sh"
demo_begin 4 "Seed a deterministic dataset; logical key counts, replica copies and locate samples"
# shellcheck source=scripts/demos/kube.sh
. "$(dirname "$0")/kube.sh"

SEED="${DEMO4_SEED:-4}"
KEYS="${DEMO4_KEYS:-300}"
demo_reset "clean deployment; dataset seed $SEED: $KEYS keys demo4-$SEED-0..$((KEYS - 1)), value {seed, i}, operation IDs demo4-$SEED-<i> (re-runs replay)"
require_preflight

key() { echo "demo4-$SEED-$1"; }
value() { echo "{\"seed\":$SEED,\"i\":$1}"; }

# --- seed ---------------------------------------------------------------------------------
# Each key is created once with its fixed operation ID. A 503 (no leader yet, or an unknown
# outcome) is retried with the same operation ID and value (AGENTS.md invariant 5), so a
# re-run, or a retry after an unknown outcome, replays the committed result.
seed_dataset() {
	local i code body tries bad="" replayed=0 t0="$SECONDS"
	for ((i = 0; i < KEYS; i++)); do
		for ((tries = 0; tries < 20; tries++)); do
			read -r code body <<<"$(cput "$(key "$i")" "$(key "$i")" "$(value "$i")")"
			[ "$code" = 503 ] || break
			sleep 0.5
		done
		if [ "$code" = 200 ] && [ "$(field "$body" .result.outcome)" = OK ]; then
			[ "$(field "$body" .result.replayed)" = true ] && replayed=$((replayed + 1))
		else
			bad+="$(key "$i"): $code $body"$'\n'
		fi
	done
	[ -z "$bad" ] && pass "seed: $KEYS keys committed through the coordinator ($replayed replayed from an earlier run, $((SECONDS - t0)) s)" ||
		fail "seed: every key commits" "$(grep -c . <<<"$bad") of $KEYS failed, e.g.:"$'\n'"$(head -3 <<<"$bad")"
}
seed_dataset

# --- 1, 2: placement of every seeded key ---------------------------------------------------
declare -A owner want
for s in "${SHARDS[@]}"; do want[$s]=0; done
bad=""
for ((i = 0; i < KEYS; i++)); do
	p="$(cadmin "/admin/locate/$(key "$i")")" || { bad+="$(key "$i"): locate failed"$'\n'; continue; }
	s="$(field "$p" .shard)"
	owner[$i]="$s"
	want[$s]=$((${want[$s]:-0} + 1))
	reps="$(jq -r '.replicas | sort | join(" ")' <<<"$p")"
	leader="$(field "$p" .leaderId)"
	[[ "$s" =~ ^[0-2]$ ]] && [[ "$(field "$p" .hash)" =~ ^[0-9a-f]{16}$ ]] && [ "$reps" = "$(pod_names "$s")" ] &&
		[[ " $reps " == *" ${leader%%/*} "* ]] || bad+="$(key "$i"): $p"$'\n'
done
[ -z "$bad" ] && pass "locate: all $KEYS keys have a shard, a 16-hex hash, the shard's three replicas and a leader among them" ||
	fail "locate: every key has a valid placement" "$(head -5 <<<"$bad")"
split="shard 0: ${want[0]}, shard 1: ${want[1]}, shard 2: ${want[2]}"
[ "${want[0]}" -gt 0 ] && [ "${want[1]}" -gt 0 ] && [ "${want[2]}" -gt 0 ] &&
	pass "dataset: every shard owns some of the $KEYS seeded keys ($split)" || fail "dataset: every shard owns some seeded keys" "$split"

# --- 3: read back -------------------------------------------------------------------------
bad=""
for ((i = 0; i < KEYS; i++)); do
	read -r code body <<<"$(cget "$(key "$i")")"
	[ "$code" = 200 ] && [ "$(jq -c .record.value <<<"$body")" = "$(jq -c . <<<"$(value "$i")")" ] &&
		[ "$(field "$body" .record.version)" = 1 ] || bad+="$(key "$i") (shard ${owner[$i]:-?}): $code $body"$'\n'
done
[ -z "$bad" ] && pass "read back: all $KEYS keys at version 1 with their values (strong reads)" ||
	fail "read back: every seeded key at version 1 with its value" "$(grep -c . <<<"$bad") of $KEYS wrong, e.g.:"$'\n'"$(head -3 <<<"$bad")"

# --- 4: logical counts ---------------------------------------------------------------------
# Counts come with leader heartbeats (1 s): wait until they cover the seed.
counts_cover() {
	local d s
	d="$(cadmin /admin/distribution)" || return 1
	for s in "${SHARDS[@]}"; do
		jq -e --argjson s "$s" --argjson w "${want[$s]}" '.shards[] | select(.shard == $s) | .countsKnown and .counts.kv >= $w and .records > 0 and .replicas == 3' <<<"$d" >/dev/null || return 1
	done
}
for _ in $(seq 30); do counts_cover && break; sleep 0.5; done
dist="$(cadmin /admin/distribution)"
run "logical record counts per shard (/admin/distribution, the leader's counts)" jq -r \
	'.shards[] | "shard \(.shard): records \(.records) (kv \(.counts.kv // "?"), request \(.counts.request // "?"), resource \(.counts.resource // "?")) replicas \(.replicas) countsKnown \(.countsKnown) heartbeat age \(.heartbeatAgeMs // "?") ms"' <<<"$dist"
counts_cover && pass "distribution: every shard holds at least its seeded keys ($split), counts known, three replicas" ||
	fail "distribution: every shard holds at least its seeded keys, counts known, three replicas" "$dist"

# --- 5: replica copies ---------------------------------------------------------------------
for s in "${SHARDS[@]}"; do
	logical="$(jq --argjson s "$s" '.shards[] | select(.shard == $s) | .records' <<<"$dist")"
	all_caught_up "$s" 30 || fail "replicas shard-$s: all three caught up at one appliedIndex"
	d="$(dumps "$s")"
	demo_show "replica copies of shard $s (logical $logical records x 3 replicas = $((logical * 3)) copies)" "$d"
	short="$(awk -v w="${want[$s]}" '{ split($4, a, "="); if ($2 == "unreachable" || a[2] < w) print }' <<<"$d")"
	if same_dumps "$s" && [ -z "$short" ]; then
		pass "replicas shard-$s: three identical copies at appliedIndex $(awk 'NR == 1 { print $2 }' <<<"$d"), each with at least ${want[$s]} seeded keys"
	else
		fail "replicas shard-$s: three identical copies, each with its seeded keys" "$d"
	fi
done

# --- locate samples, for the evidence -------------------------------------------------------
samples=""
for s in "${SHARDS[@]}"; do
	n=0
	for ((i = 0; i < KEYS && n < 2; i++)); do
		[ "${owner[$i]:-}" = "$s" ] && { samples+="$(cadmin "/admin/locate/$(key "$i")" | jq -c '{key, shard, hash, leaderId, replicas}')"$'\n'; n=$((n + 1)); }
	done
done
demo_show "locate samples (two keys per shard)" "${samples%$'\n'}"
demo_end

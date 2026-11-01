# shellcheck shell=bash
# S4.3 faults for the harness (sourced by lib.sh): Chaos Mesh NetworkChaos (isolate leader,
# lose quorum, delay a follower, delay then isolate), a stopped data worker (docker stop of its
# kind node: Chaos Mesh can't stop a node) and a coordinator restart. Each sets
# fault_started_at / fault_healed_at and a facts JSON for chaos/judge (harness_facts), and
# leaves nothing behind: every chaos object carries reliefgrid.io/chaos-run=<runId>, and the
# EXIT trap (cleanup_faults) deletes them and starts a node it stopped.
#
# fault_healed_at is stamped when removal *begins*: removing one partition can restore a
# majority while the next is still being deleted, so a stamp taken after the deletes return
# would count post-heal commits as committed during the fault.
#
# Environment: DELAY_MS (300), DELAY_JITTER_MS (50), DELAY_SECONDS (25), DELAY_LEAD_SECONDS (5),
# QUORUM_SECONDS (20), WORKER_SECONDS (30), WORKER_BACK_SECONDS (240).

DELAY_MS="${DELAY_MS:-300}"
DELAY_JITTER_MS="${DELAY_JITTER_MS:-50}"
DELAY_SECONDS="${DELAY_SECONDS:-25}"
DELAY_LEAD_SECONDS="${DELAY_LEAD_SECONDS:-5}"
QUORUM_SECONDS="${QUORUM_SECONDS:-20}"
WORKER_SECONDS="${WORKER_SECONDS:-30}"
WORKER_BACK_SECONDS="${WORKER_BACK_SECONDS:-240}"
stopped_node="" # a kind node this run stopped and has not started again

# mesh_apply <name> <spec JSON>: create NetworkChaos <name> for this run and wait until it is
# injected into every selected pod
mesh_apply() {
	jq -n --arg n "$1" --arg ns "$NS" --arg run "$run_id" --argjson spec "$2" '{apiVersion: "chaos-mesh.org/v1alpha1",
		kind: "NetworkChaos", metadata: {name: $n, namespace: $ns,
		labels: {"reliefgrid.io/chaos": "true", "reliefgrid.io/chaos-run": $run}}, spec: $spec}' | kn apply -f - >/dev/null || return 1
	kn wait "networkchaos/$1" --for=condition=AllInjected --timeout=30s >/dev/null 2>&1
}
# mesh_delete <name>: remove it and wait until Chaos Mesh has taken its rules out
mesh_delete() { kn delete networkchaos "$1" --ignore-not-found --wait=true --timeout=90s >/dev/null 2>&1; }
# pods_json <pods...>: a Chaos Mesh pod selector for those pods
pods_json() { printf '%s\n' "$@" | jq -Rsc --arg ns "$NS" 'split("\n") | map(select(. != "")) | {pods: {($ns): .}}'; }
partition_spec() { # partition_spec <selector JSON> <target JSON>
	jq -nc --argjson s "$1" --argjson t "$2" '{action: "partition", mode: "all", selector: $s, direction: "both",
		target: {mode: "all", selector: $t}}'
}
delay_spec() { # delay_spec <selector JSON> <target JSON>
	jq -nc --argjson s "$1" --argjson t "$2" --arg l "${DELAY_MS}ms" --arg j "${DELAY_JITTER_MS}ms" \
		'{action: "delay", mode: "all", selector: $s, direction: "both", delay: {latency: $l, jitter: $j, correlation: "0"},
		  target: {mode: "all", selector: $t}}'
}

cleanup_faults() {
	[ -n "${run_id:-}" ] && kn delete networkchaos -l "reliefgrid.io/chaos-run=$run_id" --wait=true --timeout=90s >/dev/null 2>&1
	if [ -n "$stopped_node" ]; then
		docker start "$stopped_node" >/dev/null 2>&1
		stopped_node=""
	fi
}

# lag_sample <follower>: leader commit index minus the follower's applied index, or "" if either
# can't be read
lag_sample() {
	local lead s f
	lead="$(group_state "${peers[@]}")"
	lead="${lead%% *}"
	[ -n "$lead" ] || return 0
	s="$(st "$lead")" f="$(st "$1")"
	[ -n "$s" ] && [ -n "$f" ] || return 0
	echo $(($(field "$s" .commitIndex) - $(field "$f" .appliedIndex)))
}

# isolation_facts <isolated pod> <t_c1> <t_c2> <m_c1> <m_c2>: the judge's isolation object
isolation_facts() {
	local led=false
	[ "${recovery_leader:-}" = "$1" ] && led=true
	jq -cn --argjson a "$([ "${3:-0}" -gt "${2:-0}" ] && echo true || echo false)" \
		--argjson b "$([ "${5:-0}" -gt "${4:-0}" ] && echo true || echo false)" --argjson c "$led" \
		--argjson t1 "${2:-0}" --argjson t2 "${3:-0}" --argjson m1 "${4:-0}" --argjson m2 "${5:-0}" \
		'{targetCommitAdvanced: $a, majorityCommitAdvanced: $b, isolatedStillLed: $c, method: "chaos-mesh partition",
		  targetCommit: [$t1, $t2], majorityCommit: [$m1, $m2]}'
}

# fault_mesh_isolate [delay-first]: partition the leader from both peers. With delay-first, one
# peer (the seeded follower) is delayed DELAY_LEAD_SECONDS before the cut and until the heal, so
# acks from before the cut are still in flight when the old leader is read (the S4.1 stale-ack
# case, at network level: probabilistic, unlike its unit test).
fault_mesh_isolate() {
	local delayed="" t_c1 t_c2 m_c1 m_c2 m_leader
	if [ "${1:-}" = delay-first ]; then
		delayed="${peers[$pick]}"
		mesh_apply "delay-$run_id" "$(delay_spec "$(pods_json "$delayed")" "$(pods_json "$target_pod" "${peers[$((1 - pick))]}")")" ||
			no_verdict "the delay on $delayed was not injected"
		note "delayed $delayed by ${DELAY_MS}ms (+-${DELAY_JITTER_MS}ms) both ways; isolating in ${DELAY_LEAD_SECONDS}s"
		sleep "$DELAY_LEAD_SECONDS"
	fi
	fault_started_at="$(now_ms)"
	mesh_apply "partition-$run_id" "$(partition_spec "$(pods_json "$target_pod")" "$(pods_json "${peers[@]}")")" ||
		no_verdict "the partition of $target_pod was not injected"
	wait_recovery "$target_term" 1 "${peers[@]}" || failed "recovery: the majority elected no leader within ${RECOVERY_MAX_SECONDS}s"
	m_leader="${recovery_leader:-${peers[0]}}"
	t_c1="$(field "$(st "$target_pod")" .commitIndex)" m_c1="$(field "$(st "$m_leader")" .commitIndex)"
	while [ "$(now_ms)" -lt $((fault_started_at + ISOLATE_SECONDS * 1000)) ]; do sleep 0.5; done
	t_c2="$(field "$(st "$target_pod")" .commitIndex)" m_c2="$(field "$(st "$m_leader")" .commitIndex)"
	fault_healed_at="$(now_ms)"
	mesh_delete "partition-$run_id" || note "could not delete partition-$run_id"
	[ -n "$delayed" ] && { mesh_delete "delay-$run_id" || note "could not delete delay-$run_id"; }
	isolation="$(isolation_facts "$target_pod" "$t_c1" "$t_c2" "$m_c1" "$m_c2")"
	harness_isolation="$isolation"
}

# fault_lose_quorum: cut every replica of the shard off from the other two, so no majority exists
fault_lose_quorum() {
	local p=($(shard_pods "$shard"))
	fault_started_at="$(now_ms)"
	mesh_apply "quorum-a-$run_id" "$(partition_spec "$(pods_json "${p[0]}")" "$(pods_json "${p[1]}" "${p[2]}")")" &&
		mesh_apply "quorum-b-$run_id" "$(partition_spec "$(pods_json "${p[1]}")" "$(pods_json "${p[2]}")")" ||
		no_verdict "the quorum partitions of shard $shard were not injected"
	while [ "$(now_ms)" -lt $((fault_started_at + QUORUM_SECONDS * 1000)) ]; do sleep 0.5; done
	fault_healed_at="$(now_ms)"
	mesh_delete "quorum-a-$run_id" || note "could not delete quorum-a-$run_id"
	mesh_delete "quorum-b-$run_id" || note "could not delete quorum-b-$run_id"
	wait_recovery 0 0 "${p[@]}" || failed "recovery: shard $shard agreed on no leader within ${RECOVERY_MAX_SECONDS}s of the heal"
}

# fault_delay_follower: delay the follower's traffic with both peers; sample its lag
fault_delay_follower() {
	local lag max=0 n=0
	fault_started_at="$(now_ms)"
	mesh_apply "delay-$run_id" "$(delay_spec "$(pods_json "$target_pod")" "$(pods_json "${peers[@]}")")" ||
		no_verdict "the delay on $target_pod was not injected"
	while [ "$(now_ms)" -lt $((fault_started_at + DELAY_SECONDS * 1000)) ]; do
		lag="$(lag_sample "$target_pod")"
		if [ -n "$lag" ]; then
			n=$((n + 1))
			[ "$lag" -gt "$max" ] && max="$lag"
		fi
		sleep 0.5
	done
	fault_healed_at="$(now_ms)"
	mesh_delete "delay-$run_id" || note "could not delete delay-$run_id"
	delay_max_lag="$max" delay_samples="$n"
}
# delay_final_lag: after the settle (one_group means equal applied indexes, so normally 0)
delay_final_lag() {
	local lag
	lag="$(lag_sample "$target_pod")"
	harness_delay="$(jq -cn --argjson m "${delay_max_lag:-0}" --argjson f "${lag:-1}" --argjson n "${delay_samples:-0}" \
		--arg d "${DELAY_MS}ms+-${DELAY_JITTER_MS}ms" '{maxLag: $m, finalLag: $f, samples: $n, delay: $d}')"
}

# fault_stop_worker: stop the kind node that runs the target replica (one replica of every
# shard lives on each data worker), then start it and wait for its pods
fault_stop_worker() {
	local node back=false deadline
	node="$(kn get pod "$target_pod" -o jsonpath='{.spec.nodeName}')"
	[ -n "$node" ] && [ "$(k get node "$node" -o jsonpath='{.metadata.labels.role}')" = data ] ||
		no_verdict "$target_pod is not on a data worker ($node)"
	fault_started_at="$(now_ms)"
	docker stop "$node" >/dev/null || no_verdict "docker stop $node"
	stopped_node="$node"
	note "stopped data worker $node (its storage replicas went with it)"
	while [ "$(now_ms)" -lt $((fault_started_at + WORKER_SECONDS * 1000)) ]; do sleep 0.5; done
	docker start "$node" >/dev/null || failed "worker: docker start $node failed"
	stopped_node=""
	deadline=$(($(now_ms) + WORKER_BACK_SECONDS * 1000))
	while [ "$(now_ms)" -lt "$deadline" ]; do
		if [ "$(k get node "$node" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" = True ] &&
			[ "$(kn get pods -l app.kubernetes.io/component=storagenode --field-selector "spec.nodeName=$node" -o json 2>/dev/null |
				jq '[.items[] | select(any(.status.conditions[]?; .type == "Ready" and .status == "True"))] | length')" = 3 ]; then
			back=true
			break
		fi
		sleep 1
	done
	fault_healed_at="$(now_ms)"
	$back || failed "worker: $node or its 3 storage pods not Ready within ${WORKER_BACK_SECONDS}s of docker start"
	harness_worker="$(jq -cn --arg n "$node" --argjson b "$back" '{node: $n, back: $b}')"
}

# shard_terms: {"0": term, ...} from each shard's agreed leader
shard_terms() {
	local s g out="{}"
	for s in $(seq 0 $((SHARDS - 1))); do
		g="$(group_state $(shard_pods "$s"))"
		[ -n "$g" ] && out="$(jq -c --arg s "$s" --argjson t "${g##* }" '. + {($s): $t}' <<<"$out")"
	done
	echo "$out"
}
ring_id() { run_job "chaos-ring-$run_id-$1" ring --coordinator "$COORD_URL" | jq -r '"\(.ringVersion)/\(.checksum)"' 2>/dev/null; }
bootstrap_jobs() { kn get jobs -o name 2>/dev/null | grep -c '^job.batch/storage-bootstrap-'; }

# fault_restart_coordinator: delete the coordinator pod; healed once a new one is Ready
fault_restart_coordinator() {
	local ring_before terms_before jobs_before old uid back=false deadline
	ring_before="$(ring_id before)" terms_before="$(shard_terms)" jobs_before="$(bootstrap_jobs)"
	old="$(kn get pods -l app.kubernetes.io/component=coordinator -o jsonpath='{.items[0].metadata.name}')"
	uid="$(kn get pod "$old" -o jsonpath='{.metadata.uid}')"
	fault_started_at="$(now_ms)"
	kn delete pod "$old" --wait=false >/dev/null || no_verdict "kubectl delete pod $old"
	deadline=$(($(now_ms) + RECOVERY_MAX_SECONDS * 1000))
	while [ "$(now_ms)" -lt "$deadline" ]; do
		if kn get pods -l app.kubernetes.io/component=coordinator -o json 2>/dev/null | jq -e --arg u "$uid" \
			'any(.items[]; .metadata.uid != $u and any(.status.conditions[]?; .type == "Ready" and .status == "True"))' >/dev/null; then
			back=true
			break
		fi
		sleep 0.5
	done
	fault_healed_at="$(now_ms)"
	recovery_ms="$fault_healed_at"
	$back || failed "coordinator: no new coordinator Ready within ${RECOVERY_MAX_SECONDS}s"
	sleep 5 # heartbeats repopulate the node table (UP after one heartbeat)
	harness_coordinator="$(jq -cn --arg rb "$ring_before" --arg ra "$(ring_id after)" --argjson tb "$terms_before" \
		--argjson ta "$(shard_terms)" --argjson jb "$jobs_before" --argjson ja "$(bootstrap_jobs)" --argjson back "$back" \
		'{ringBefore: $rb, ringAfter: $ra, termsBefore: $tb, termsAfter: $ta, bootstrapJobsBefore: $jb, bootstrapJobsAfter: $ja, back: $back}')"
}

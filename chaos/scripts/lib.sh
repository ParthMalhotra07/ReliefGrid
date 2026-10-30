# shellcheck shell=bash
# Failure harness (S2.3), shared by kill-leader.sh, kill-follower.sh, restart-shard.sh and
# isolate-replica.sh: each sets EXPERIMENT and calls chaos_main "$@".
#
# One run (v3.1 §26): a seeded plan; the load as an in-cluster Job (invcheck load against the
# coordinator Service, ledger on PVC chaos-ledger); after the seeded delay the fault on one
# shard; its heal; a bounded settle phase; then invcheck verify and invcheck check as Jobs.
# Everything observed goes into one JSON report. Exit 0: every assertion passed; 1: one
# failed (the report's failures say which); 2: no verdict (the harness itself failed).
#
# Environment: CHAOS_CONTEXT (kind-reliefgrid), CHAOS_NAMESPACE (reliefgrid), TOOLS_IMAGE
# (reliefgrid/tools:dev), ELECTION_MAX_SECONDS (10), ISOLATE_SECONDS (20), CUT_CHECK_SECONDS (8).

set -uo pipefail

CTX="${CHAOS_CONTEXT:-kind-reliefgrid}"
NS="${CHAOS_NAMESPACE:-reliefgrid}"
KIND_CLUSTER="${CTX#kind-}"
TOOLS_IMAGE="${TOOLS_IMAGE:-reliefgrid/tools:dev}"
COORD_URL="http://storage-coordinator.$NS.svc.cluster.local:8080"
PVC=chaos-ledger
SHARDS=3
ISOLATED_LABEL=reliefgrid.io/chaos-isolated
ELECTION_MAX_SECONDS="${ELECTION_MAX_SECONDS:-10}"
ISOLATE_SECONDS="${ISOLATE_SECONDS:-20}"
CUT_CHECK_SECONDS="${CUT_CHECK_SECONDS:-8}"
RECOVERY_MAX_SECONDS=60 # any experiment: fault start -> an agreed leader
WORKERS=4 KEYS=4 INTERVAL_MS=50

chaos_usage() {
	cat <<EOF
usage: chaos/scripts/$EXPERIMENT.sh --seed N [--out FILE] [--shard N] [--settle-max SECONDS]
                                 [--inject lost-write]${ROLE_FLAG:+ [--role leader|follower]} [--plan]

$DESCRIPTION

  --seed N            required; the same seed gives the same plan (shard, target, timings)
  --out FILE          the JSON report (default: chaos-$EXPERIMENT-<seed>.json)
  --shard N           the shard to fault (default: chosen by the seed)
  --settle-max S      bound on the settle phase after the heal (default 120)
  --inject lost-write overwrite one acknowledged write before verifying: the run must fail
  --plan              print the plan as JSON and touch nothing
Exit 0: passed; 1: an assertion failed; 2: no verdict.
EOF
}

# Milliseconds by arithmetic: uutils date (Ubuntu 25.10+) ignores the width in %3N.
now_ms() { echo $(($(date +%s%N) / 1000000)); }
k() { kubectl --context "$CTX" "$@"; }
kn() { k -n "$NS" "$@"; }
log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
st() { timeout 5 kubectl --context "$CTX" -n "$NS" exec "$1" -c storagenode -- /storagectl status 2>/dev/null; }
field() { jq -r "$2 // empty" <<<"$1" 2>/dev/null; }
pod_of() { printf '%s' "${1%%/*}"; } # a serverId's pod name
shard_pods() { printf 'storage-shard-%s-0 storage-shard-%s-1 storage-shard-%s-2' "$1" "$1" "$1"; }
raft_addr() { printf '%s.storage-shard-%s.%s.svc.cluster.local:%s' "$1" "$shard" "$NS" "$2"; } # raft_addr <pod> <port>
dial() { kn exec "$1" -c storagenode -- /storagectl dial --addr "$2" --timeout 3s >/dev/null 2>&1; }
iso_ms() { local s; s="$(date -d "$1" +%s 2>/dev/null)" && echo $((s * 1000)); } # RFC 3339 (whole seconds) -> unix ms

# seeded <label>: a non-negative number from the experiment, the seed and label
seeded() { printf '%s:%s:%s' "$EXPERIMENT" "$seed" "$1" | sha256sum | cut -c1-8 | { read -r h; echo $((16#$h)); }; }

# note <message>: a harness observation, kept for errorsObserved
note() {
	log "note: $1"
	jq -cn --argjson at "$(now_ms)" --arg m "$1" '{at: $at, source: "harness", message: $m}' >>"$work/notes"
}
failed() { # failed <assertion that did not hold>
	log "FAILED: $1"
	failures+=("$1")
}

parse_args() {
	seed="" out="" shard_flag="" settle_max=120 plan_only=0 inject="" role_flag=""
	while [ $# -gt 0 ]; do
		case "$1" in
		--seed) seed="${2:-}"; shift 2 ;;
		--out) out="${2:-}"; shift 2 ;;
		--shard) shard_flag="${2:-}"; shift 2 ;;
		--settle-max) settle_max="${2:-}"; shift 2 ;;
		--inject) inject="${2:-}"; shift 2 ;;
		--role) role_flag="${2:-}"; shift 2 ;;
		--plan) plan_only=1; shift ;;
		-h | --help) chaos_usage; exit 0 ;;
		*) echo "$EXPERIMENT: unknown flag $1" >&2; chaos_usage >&2; exit 2 ;;
		esac
	done
	[[ "$seed" =~ ^[0-9]+$ ]] || { echo "$EXPERIMENT: --seed N is required" >&2; exit 2; }
	[ -z "$shard_flag" ] || [[ "$shard_flag" =~ ^[0-2]$ ]] || { echo "$EXPERIMENT: --shard must be 0..2" >&2; exit 2; }
	[[ "$settle_max" =~ ^[0-9]+$ ]] && [ "$settle_max" -gt 0 ] || { echo "$EXPERIMENT: --settle-max must be a positive number of seconds" >&2; exit 2; }
	[ -z "$inject" ] || [ "$inject" = lost-write ] || { echo "$EXPERIMENT: --inject takes lost-write" >&2; exit 2; }
	[ -z "$role_flag" ] || { [ -n "${ROLE_FLAG:-}" ] && [[ "$role_flag" =~ ^(leader|follower)$ ]]; } ||
		{ echo "$EXPERIMENT: --role is leader or follower, for isolate-replica only" >&2; exit 2; }
	out="${out:-chaos-$EXPERIMENT-$seed.json}"
}

# make_plan: everything the seed decides. The same seed and flags give the same plan.
make_plan() {
	shard="${shard_flag:-$(($(seeded shard) % SHARDS))}"
	case "$EXPERIMENT" in
	kill-leader) role=leader budget=75 ;;
	kill-follower) role=follower budget=75 ;;
	restart-shard) role=all budget=150 ;;
	isolate-replica)
		role="$role_flag"
		[ -n "$role" ] || { [ $(($(seeded role) % 2)) = 0 ] && role=leader || role=follower; }
		budget=$((ISOLATE_SECONDS + 75))
		;;
	esac
	pick=$(($(seeded pick) % 2))                 # which follower, in pod-name order
	fault_delay_ms=$((8000 + $(seeded delay) % 7000)) # load start -> fault start
	load_seconds=$((fault_delay_ms / 1000 + 1 + budget))
	prefix="chaos-$EXPERIMENT-s$seed"
	plan="$(jq -cn --arg e "$EXPERIMENT" --argjson seed "$seed" --argjson shard "$shard" --arg role "$role" \
		--argjson pick "$pick" --argjson delay "$fault_delay_ms" --argjson load "$load_seconds" \
		--argjson settle "$settle_max" --arg prefix "$prefix" --argjson w "$WORKERS" --argjson k "$KEYS" --argjson iv "$INTERVAL_MS" \
		'{experiment: $e, seed: $seed, shard: $shard, role: $role, pick: $pick, faultDelayMs: $delay, loadSeconds: $load,
		  settleMaxSeconds: $settle, prefix: $prefix, workers: $w, keys: $k, intervalMs: $iv}')"
}

# group_state <pods...>: "<leader pod> <term>" if exactly one of the reachable pods is Leader
# and every reachable pod names it; "" otherwise.
group_state() {
	local p s lead="" n=0 term="" ids=""
	for p in "$@"; do
		s="$(st "$p")" || continue
		[ -n "$s" ] || continue
		if [ "$(field "$s" .state)" = Leader ]; then
			n=$((n + 1))
			lead="$p" term="$(field "$s" .term)"
		fi
		ids+="$(pod_of "$(field "$s" .leaderId)") "
	done
	[ "$n" = 1 ] || return 0
	for p in $ids; do [ "$p" = "$lead" ] || return 0; done
	[ "$(wc -w <<<"$ids")" -ge 2 ] && echo "$lead $term"
}

# one_group: the whole shard is one group, every member caught up at one appliedIndex, with
# the manifest's serverIds. Prints why not.
one_group() {
	local p s lead="" n=0 ids="" applied=""
	for p in $(shard_pods "$shard"); do
		s="$(st "$p")"
		[ -n "$s" ] || { echo "$p unreachable"; return 1; }
		[ "$(field "$s" .state)" = Leader ] && n=$((n + 1))
		[ -z "$lead" ] && lead="$(field "$s" .leaderId)"
		[ -n "$lead" ] && [ "$(field "$s" .leaderId)" = "$lead" ] || { echo "$p names '$(field "$s" .leaderId)', others '$lead'"; return 1; }
		[ "$(field "$s" .caughtUp)" = true ] || { echo "$p not caught up"; return 1; }
		[ -z "$applied" ] && applied="$(field "$s" .appliedIndex)"
		[ "$(field "$s" .appliedIndex)" = "$applied" ] || { echo "$p at appliedIndex $(field "$s" .appliedIndex), others $applied"; return 1; }
		ids+="$(field "$s" .serverId)"$'\n'
	done
	[ "$n" = 1 ] || { echo "$n leaders"; return 1; }
	[ "$(sort <<<"$ids" | grep .)" = "$(kn get configmap "storage-shard-$shard-manifest" -o jsonpath='{.data.peerIds}' | jq -r '.[]' | sort)" ] ||
		{ echo "serverIds differ from the manifest's peerIds"; return 1; }
}

ensure_pvc() {
	kn get pvc "$PVC" >/dev/null 2>&1 && return 0
	kn apply -f - >/dev/null <<EOF || return 1
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: $PVC
  labels: {reliefgrid.io/chaos: "true"}
spec:
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: 64Mi}}
EOF
}

# start_job <name> <invcheck args...>: /invcheck as a Job on the infra worker, with the
# ledger PVC at /ledger. Kept for an hour after it finishes, for inspection.
start_job() {
	local name="$1"
	shift
	kn delete job "$name" --ignore-not-found --wait >/dev/null 2>&1
	printf '%s\n' "$@" | jq -Rs 'split("\n")[:-1]' | jq --arg name "$name" --arg image "$TOOLS_IMAGE" --arg run "$run_id" '{
		apiVersion: "batch/v1", kind: "Job",
		metadata: {name: $name, labels: {"reliefgrid.io/chaos": "true", "reliefgrid.io/chaos-run": $run}},
		spec: {backoffLimit: 0, ttlSecondsAfterFinished: 3600, template: {
			metadata: {labels: {"reliefgrid.io/chaos": "true"}},
			spec: {
				restartPolicy: "Never",
				nodeSelector: {role: "infra"},
				tolerations: [{key: "role", operator: "Equal", value: "infra", effect: "NoSchedule"}],
				securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532},
				volumes: [{name: "ledger", persistentVolumeClaim: {claimName: "chaos-ledger"}}],
				containers: [{name: "invcheck", image: $image, imagePullPolicy: "Never",
					command: (["/invcheck"] + .), volumeMounts: [{name: "ledger", mountPath: "/ledger"}]}]}}}}' |
		kn apply -f - >/dev/null
}
job_done() { # job_done <name>: the Job succeeded or failed
	local s
	s="$(kn get job "$1" -o jsonpath='{.status.succeeded}{.status.failed}' 2>/dev/null)"
	[ -n "$s" ] && [ "$s" != 0 ]
}
job_exit() { kn get pods -l "job-name=$1" -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null; }
job_json() { kn logs "job/$1" --tail=-1 2>/dev/null | awk '/^\{/ { f = 1 } f'; } # the JSON a Job printed
# run_job <name> <args...>: start it, wait for it (10 min), print its JSON; returns its exit code
run_job() {
	local name="$1" i code
	start_job "$@" || { note "could not create Job $name"; return 99; }
	for ((i = 0; i < 1200; i++)); do
		job_done "$name" && break
		sleep 0.5
	done
	job_done "$name" || { note "Job $name did not finish within 10 minutes"; return 98; }
	job_json "$name"
	code="$(job_exit "$name")"
	return "${code:-97}"
}

# pod_ready <pod> <old uid>: a new pod (another uid) under the name is Ready
pod_ready() {
	local got
	got="$(kn get pod "$1" -o jsonpath='{.metadata.uid} {.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)"
	[ -n "$got" ] && [ "${got%% *}" != "$2" ] && [ "${got##* }" = True ]
}

# wait_recovery <min term> <exclusive: 1 if the term must be higher> <pods...>: sets
# recovery_leader, recovery_term and recovery_ms once one leader is agreed among <pods>.
wait_recovery() {
	local min="$1" higher="$2" g deadline
	shift 2
	deadline=$(($(now_ms) + RECOVERY_MAX_SECONDS * 1000))
	while [ "$(now_ms)" -lt "$deadline" ]; do
		g="$(group_state "$@")"
		if [ -n "$g" ] && { [ "${g##* }" -gt "$min" ] || { [ "$higher" = 0 ] && [ "${g##* }" -ge "$min" ]; }; }; then
			recovery_leader="${g%% *}" recovery_term="${g##* }" recovery_ms="$(now_ms)"
			return 0
		fi
		sleep 0.3
	done
	return 1
}

fault_pod_delete() { # fault_pod_delete <pods...>: delete them, heal once each is back and Ready
	local p uids=() i all=0 deadline
	for p in "$@"; do uids+=("$(kn get pod "$p" -o jsonpath='{.metadata.uid}')"); done
	fault_started_at="$(now_ms)"
	kn delete pod "$@" --wait=false >/dev/null || note "kubectl delete pod $* failed"
	local others=()
	for p in $(shard_pods "$shard"); do [[ " $* " == *" $p "* ]] || others+=("$p"); done
	if [ "$role" = follower ]; then
		wait_recovery "$target_term" 0 "${others[@]}" || failed "recovery: no leader agreed within ${RECOVERY_MAX_SECONDS}s"
	elif [ "$role" = leader ]; then
		wait_recovery "$target_term" 1 "${others[@]}" "$target_pod" || failed "recovery: no new leader within ${RECOVERY_MAX_SECONDS}s"
	fi
	deadline=$(($(now_ms) + budget * 1000))
	while [ "$(now_ms)" -lt "$deadline" ]; do
		all=1
		for i in "${!uids[@]}"; do pod_ready "${@:i+1:1}" "${uids[$i]}" || all=0; done
		[ "$all" = 1 ] && break
		sleep 0.5
	done
	[ "$all" = 1 ] || failed "heal: $* not back and Ready within ${budget}s"
	fault_healed_at="$(now_ms)"
	if [ "$role" = all ]; then
		wait_recovery "$target_term" 1 "$@" || failed "recovery: the restarted shard agreed on no leader within ${RECOVERY_MAX_SECONDS}s of the heal"
	fi
}

# cut_taken: the isolated replica has lost Raft contact (as seen by both sides)
cut_taken() {
	local s
	if [ "$role" = leader ]; then
		[ -n "$(group_state "${peers[@]}")" ] && [ "$(group_state "${peers[@]}" | cut -d' ' -f2)" -gt "$target_term" ]
	else
		s="$(st "$target_pod")"
		[ -n "$s" ] && { [ -z "$(field "$s" .leaderId)" ] || [ "$(field "$s" .state)" != Follower ]; }
	fi
}
wait_cut() { # wait_cut <seconds>
	local deadline=$(($(now_ms) + $1 * 1000))
	while [ "$(now_ms)" -lt "$deadline" ]; do
		cut_taken && return 0
		sleep 0.3
	done
	return 1
}
# clear_conntrack: delete the isolated pod's conntrack entries on every kind node, so that
# connections opened before the cut no longer pass it
clear_conntrack() {
	local ip node
	ip="$(kn get pod "$target_pod" -o jsonpath='{.status.podIP}')"
	[ -n "$ip" ] || { note "no pod IP for $target_pod"; return 1; }
	for node in $(kind get nodes --name "$KIND_CLUSTER" 2>/dev/null); do
		docker exec "$node" conntrack -D -s "$ip" >/dev/null 2>&1
		docker exec "$node" conntrack -D -d "$ip" >/dev/null 2>&1
	done
	return 0
}

fault_isolate() {
	local s t_c1 t_c2 m_c1 m_c2 m_leader
	kn label pod "$target_pod" "$ISOLATED_LABEL=true" --overwrite >/dev/null || note "could not label $target_pod"
	fault_started_at="$(now_ms)"
	isolation_method=networkpolicy
	if ! wait_cut "$CUT_CHECK_SECONDS"; then
		note "the NetworkPolicy cut did not take within ${CUT_CHECK_SECONDS}s; clearing $target_pod's conntrack entries"
		clear_conntrack
		isolation_method=networkpolicy+conntrack
		wait_cut $((CUT_CHECK_SECONDS * 2)) || failed "isolation: $target_pod kept Raft contact (method $isolation_method)"
	fi
	if [ "$role" = leader ]; then
		wait_recovery "$target_term" 1 "${peers[@]}" || failed "recovery: the majority elected no leader within ${RECOVERY_MAX_SECONDS}s"
	else
		wait_recovery "$target_term" 0 "${peers[@]}" || failed "recovery: the majority agreed on no leader within ${RECOVERY_MAX_SECONDS}s"
	fi
	m_leader="${recovery_leader:-${peers[0]}}"
	t_c1="$(field "$(st "$target_pod")" .commitIndex)" m_c1="$(field "$(st "$m_leader")" .commitIndex)"
	iso_peer_raft=false iso_target_raft=false iso_peer_data=false
	dial "$m_leader" "$(raft_addr "$target_pod" 7000)" && iso_peer_raft=true
	dial "$target_pod" "$(raft_addr "$m_leader" 7000)" && iso_target_raft=true
	dial "$m_leader" "$(raft_addr "$target_pod" 8080)" && iso_peer_data=true
	while [ "$(now_ms)" -lt $((fault_started_at + ISOLATE_SECONDS * 1000)) ]; do sleep 0.5; done
	t_c2="$(field "$(st "$target_pod")" .commitIndex)" m_c2="$(field "$(st "$m_leader")" .commitIndex)"
	iso_target_adv=false iso_major_adv=false
	[ "${t_c2:-0}" -gt "${t_c1:-0}" ] && iso_target_adv=true
	[ "${m_c2:-0}" -gt "${m_c1:-0}" ] && iso_major_adv=true
	kn label pod "$target_pod" "$ISOLATED_LABEL-" >/dev/null || note "could not remove $ISOLATED_LABEL from $target_pod"
	fault_healed_at="$(now_ms)"
	$iso_peer_raft && failed "isolation: $m_leader still reached $target_pod:7000"
	$iso_target_raft && failed "isolation: $target_pod still reached $m_leader:7000"
	$iso_peer_data || failed "isolation: $target_pod's data port 8080 was closed too"
	$iso_target_adv && failed "isolation: the isolated $target_pod advanced its commit index ($t_c1 -> $t_c2)"
	$iso_major_adv || failed "isolation: the majority's commit index did not advance ($m_c1 -> $m_c2)"
	[ "$role" = leader ] && [ "${recovery_leader:-}" = "$target_pod" ] && failed "isolation: the isolated leader still led"
	isolation="$(jq -cn --argjson a "$iso_peer_raft" --argjson b "$iso_target_raft" --argjson c "$iso_peer_data" \
		--argjson d "$iso_target_adv" --argjson e "$iso_major_adv" --arg m "$isolation_method" \
		'{peerToTargetRaft: $a, targetToPeerRaft: $b, peerToTargetData: $c, targetCommitAdvanced: $d, majorityCommitAdvanced: $e, method: $m}')"
}

write_report() { # write_report <pass true|false>
	local errors
	errors="$(
		{
			jq -c '.ledger.errors[]? | {at, source: "load", message: "\(.operationId) \(.key) attempt \(.attempt): \(.error)"}' <<<"$verify_json" 2>/dev/null
			cat "$work/notes" 2>/dev/null
		} | jq -cs 'sort_by(.at)'
	)"
	jq -n --arg e "$EXPERIMENT" --argjson seed "$seed" --arg run "$run_id" --argjson pass "$1" \
		--argjson failures "$(printf '%s\n' "${failures[@]+"${failures[@]}"}" | jq -Rsc 'split("\n") | map(select(. != ""))')" \
		--argjson plan "$plan" \
		--argjson target "$(jq -cn --argjson s "$shard" --arg p "${target_pod:-}" --arg id "${target_id:-}" --arg r "$role" --argjson t "${target_term:-0}" \
			'{shard: $s, pod: $p, serverId: $id, role: $r, term: $t}')" \
		--argjson fault "$(jq -cn --arg k "$fault_kind" --arg m "$fault_mechanism" --argjson a "${fault_started_at:-0}" --argjson b "${fault_healed_at:-0}" \
			'{kind: $k, mechanism: $m, startedAt: $a, healedAt: $b}')" \
		--argjson recovery "$(jq -cn --arg l "${recovery_leader:-}" --argjson t "${recovery_term:-0}" \
			--argjson s "$(awk -v a="${fault_started_at:-0}" -v b="${recovery_ms:-0}" 'BEGIN { printf "%.1f", (b > 0 ? (b - a) / 1000 : -1) }')" \
			'{leader: $l, term: $t, seconds: $s}')" \
		--argjson settle "$(jq -cn --argjson max "$settle_max" --argjson s "${settle_seconds:--1}" --argjson ok "${settle_ok:-false}" --arg d "${settle_detail:-}" \
			--argjson at "${settle_started_at:-0}" '{maxSeconds: $max, startedAt: $at, seconds: $s, ok: $ok, detail: $d}')" \
		--argjson timings "$(jq -cn --argjson a "${load_started_at:-0}" --argjson b "${fault_started_at:-0}" --argjson c "${fault_healed_at:-0}" \
			--argjson d "${settled_at:-0}" --argjson e "${load_finished_at:-0}" \
			'{loadStartedAt: $a, faultStartedAt: $b, faultHealedAt: $c, settledAt: $d, loadFinishedAt: $e}')" \
		--argjson load "$(jq -cn --arg j "$load_job" --arg u "$COORD_URL" --argjson x "${load_exit:--1}" --argjson v "$verify_json" \
			'{job: $j, coordinator: $u, exitCode: $x, acked: ($v.ledger.acked // 0), unknown: ($v.ledger.unknown // 0),
			  rejected: ($v.ledger.rejected // 0), ackedDuringFault: ($v.ledger.ackedInWindow // 0)}')" \
		--argjson errors "$errors" \
		--argjson ledger "$(jq -cn --arg pvc "$PVC" --arg p "$ledger_path" --arg j "${verify_job:-}" --argjson x "${verify_exit:--1}" --argjson v "$verify_json" \
			'{pvc: $pvc, path: $p, job: $j, exitCode: $x, checked: ($v.verdict.checked // 0), lost: ($v.verdict.lost // []),
			  resolution: ($v.resolution // {}), incomplete: ($v.incomplete // null)}')" \
		--argjson invcheck "$(jq -cn --arg j "${check_job:-}" --argjson x "${check_exit:--1}" --argjson c "$check_json" \
			'{job: $j, exitCode: $x, verdict: ($c.verdict // null), quiescent: ($c.report.quiescent // false),
			  violations: ($c.report.violations // []), incomplete: ($c.incomplete // null)}')" \
		--argjson isolation "${isolation:-null}" --argjson injected "${injected:-null}" \
		'{experiment: $e, seed: $seed, runId: $run, pass: $pass, failures: $failures, plan: $plan, target: $target,
		  fault: $fault, recovery: $recovery, settle: $settle, timings: $timings, load: $load, errorsObserved: $errors,
		  ledger: $ledger, invcheck: $invcheck}
		  + (if $isolation then {isolation: $isolation} else {} end)
		  + (if $injected then {injected: $injected} else {} end)' >"$out.tmp" && mv "$out.tmp" "$out"
}

# no_verdict <why>: the harness could not finish; write what it has and exit 2
no_verdict() {
	note "no verdict: $1"
	failed "no verdict: $1"
	write_report false
	log "report: $out (no verdict)"
	exit 2
}

chaos_main() {
	parse_args "$@"
	make_plan
	if [ "$plan_only" = 1 ]; then
		echo "$plan"
		return 0
	fi
	failures=()
	work="$(mktemp -d)"
	load_job=""
	# Never leave the load writing: a run that stops early deletes its load Job.
	trap '[ -n "$load_job" ] && ! job_done "$load_job" && kn delete job "$load_job" --wait=false >/dev/null 2>&1; rm -rf "$work"' EXIT
	: >"$work/notes"
	run_id="$EXPERIMENT-s$seed-$(date +%s)"
	load_job="chaos-load-$run_id" verify_job="" check_job=""
	ledger_path="/ledger/$run_id.json"
	fault_kind="" fault_mechanism="" target_pod="" target_id="" target_term=0
	verify_json="{}" check_json="{}" isolation="" injected="" recovery_leader="" recovery_term="" recovery_ms=""
	load_started_at="" settle_started_at="" fault_started_at="" fault_healed_at="" settled_at="" load_finished_at="" load_exit="" verify_exit="" check_exit=""
	case "$EXPERIMENT" in
	kill-leader | kill-follower) fault_kind=pod-delete fault_mechanism="kubectl delete pod (graceful)" ;;
	restart-shard) fault_kind=shard-restart fault_mechanism="kubectl delete pod of all three replicas (graceful)" ;;
	isolate-replica) fault_kind=raft-isolation fault_mechanism="label $ISOLATED_LABEL=true, excluded by NetworkPolicy storage-shard-$shard-raft" ;;
	esac
	log "$EXPERIMENT: plan $plan"

	# The starting point: one caught-up group, and the target by role.
	local i
	for ((i = 0; i < 120; i++)); do
		why="$(one_group)" && break
		sleep 0.5
	done
	[ "$i" -lt 120 ] || no_verdict "shard $shard is not one caught-up group before the run: $why"
	local p s leader="" followers=()
	for p in $(shard_pods "$shard"); do
		s="$(st "$p")"
		if [ "$(field "$s" .state)" = Leader ]; then
			leader="$p" target_term="$(field "$s" .term)"
		else
			followers+=("$p")
		fi
	done
	case "$role" in
	leader | all) target_pod="$leader" ;;
	follower) target_pod="${followers[$((pick % ${#followers[@]}))]}" ;;
	esac
	target_id="$(field "$(st "$target_pod")" .serverId)"
	peers=()
	for p in $(shard_pods "$shard"); do [ "$p" = "$target_pod" ] || peers+=("$p"); done
	log "target $target_pod ($role, term $target_term)"

	# The load.
	ensure_pvc || no_verdict "could not create PVC $PVC"
	start_job "$load_job" load --coordinator "$COORD_URL" --ledger "$ledger_path" --prefix "$prefix-$(date +%s)" \
		--workers "$WORKERS" --keys "$KEYS" --ops 0 --duration "${load_seconds}s" --interval "${INTERVAL_MS}ms" \
		--timeout "$((load_seconds + 120))s" || no_verdict "could not create the load Job"
	local started=""
	for ((i = 0; i < 240; i++)); do
		started="$(kn get pods -l "job-name=$load_job" -o jsonpath='{.items[0].status.containerStatuses[0].state.running.startedAt}' 2>/dev/null)"
		[ -n "$started" ] && break
		job_done "$load_job" && break
		sleep 0.5
	done
	[ -n "$started" ] || no_verdict "the load Job never started running"
	load_started_at="$(iso_ms "$started")"
	log "load running since $started; fault in ${fault_delay_ms}ms"
	while [ "$(now_ms)" -lt $((load_started_at + fault_delay_ms)) ]; do sleep 0.2; done

	# The fault and its heal.
	case "$EXPERIMENT" in
	kill-leader | kill-follower) fault_pod_delete "$target_pod" ;;
	restart-shard) fault_pod_delete $(shard_pods "$shard") ;;
	isolate-replica) fault_isolate ;;
	esac
	log "fault $fault_kind on $target_pod: $(((fault_healed_at - fault_started_at) / 1000))s; recovery ${recovery_leader:-none} term ${recovery_term:-?}"
	[ "$EXPERIMENT" = kill-leader ] && [ -n "${recovery_ms:-}" ] && [ $((recovery_ms - fault_started_at)) -gt $((ELECTION_MAX_SECONDS * 1000)) ] &&
		failed "recovery: the new leader took $(((recovery_ms - fault_started_at) / 1000))s, over ${ELECTION_MAX_SECONDS}s"

	# The writers stop: the load runs its planned duration (its own --timeout bounds it).
	local deadline=$((load_started_at + (load_seconds + 120) * 1000))
	while [ "$(now_ms)" -lt "$deadline" ] && ! job_done "$load_job"; do sleep 0.5; done
	job_done "$load_job" || no_verdict "the load Job did not finish within $((load_seconds + 120))s"

	# Settle (v3.1 §26: writers paused, then converge): from the later of the heal and the
	# load's end, the shard must become one caught-up group within the bound.
	settle_started_at="$(now_ms)"
	deadline=$((settle_started_at + settle_max * 1000))
	settle_ok=false settle_detail="not checked"
	while [ "$(now_ms)" -lt "$deadline" ]; do
		if settle_detail="$(one_group)"; then
			settle_ok=true settle_detail="shard $shard is one caught-up group"
			break
		fi
		sleep 0.5
	done
	settled_at="$(now_ms)"
	settle_seconds="$(awk -v a="$settle_started_at" -v b="$settled_at" 'BEGIN { printf "%.1f", (b - a) / 1000 }')"
	$settle_ok || failed "settle: not settled within ${settle_max}s: $settle_detail"
	load_exit="$(job_exit "$load_job")"
	load_finished_at="$(iso_ms "$(kn get pods -l "job-name=$load_job" -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.finishedAt}')")"
	[ "${load_exit:-1}" = 0 ] || failed "load: invcheck load exited ${load_exit:-?}"
	[ "${load_finished_at:-0}" -gt "${fault_healed_at:-0}" ] || failed "load: the load finished before the fault healed"

	# The deliberate loss, if asked for.
	if [ "$inject" = lost-write ]; then
		injected="$(run_job "chaos-inject-$run_id" inject --confirm --coordinator "$COORD_URL" --ledger "$ledger_path" --json)" ||
			no_verdict "invcheck inject failed: $injected"
		injected="$(jq -c . <<<"$injected")" || no_verdict "invcheck inject printed no JSON"
		log "injected: $injected"
	fi

	# The verdicts.
	verify_job="chaos-verify-$run_id"
	verify_json="$(run_job "$verify_job" verify --coordinator "$COORD_URL" --ledger "$ledger_path" --json \
		--window "$fault_started_at:$fault_healed_at")"
	verify_exit=$?
	jq -e . <<<"$verify_json" >/dev/null 2>&1 || { verify_json="{}"; }
	verify_json="$(jq -c . <<<"$verify_json")"
	check_job="chaos-check-$run_id"
	check_json="$(run_job "$check_job" check --coordinator "$COORD_URL" --json)"
	check_exit=$?
	jq -e . <<<"$check_json" >/dev/null 2>&1 || { check_json="{}"; }
	check_json="$(jq -c . <<<"$check_json")"
	case "$verify_exit" in
	0) ;;
	1) failed "ledger: $(jq '.verdict.lost | length' <<<"$verify_json") acknowledged write(s) lost" ;;
	*) no_verdict "invcheck verify reached no verdict (exit $verify_exit): $(jq -r '.incomplete // "no output"' <<<"$verify_json")" ;;
	esac
	case "$check_exit" in
	0) ;;
	1 | 3) failed "invcheck: $(jq -r '.verdict // "no verdict"' <<<"$check_json") (exit $check_exit)" ;;
	*) no_verdict "invcheck check reached no verdict (exit $check_exit): $(jq -r '.incomplete // "no output"' <<<"$check_json")" ;;
	esac
	[ "$(jq '.ledger.ackedInWindow // 0' <<<"$verify_json")" -gt 0 ] || failed "load: no write was acknowledged during the fault"

	if [ "${#failures[@]}" = 0 ]; then
		write_report true
		log "PASSED: report $out"
		return 0
	fi
	write_report false
	log "FAILED (${#failures[@]}): report $out"
	return 1
}

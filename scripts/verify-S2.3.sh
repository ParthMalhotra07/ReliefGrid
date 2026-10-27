#!/usr/bin/env bash
# Verify check for S2.3 (failure harness).
# Run from the repo root, against the local "reliefgrid" kind cluster:
#   scripts/verify-S2.3.sh
#   SEED=41 ELECTION_MAX_SECONDS=15 scripts/verify-S2.3.sh
# It builds and loads the storage and coordinator images, installs (or upgrades)
# deploy/helm/storage, waits for every shard to be one caught-up group, then runs the
# experiments under load and checks each JSON report and the cluster independently of it.
# It never recreates the cluster. Writes evidence to docs/evidence/S2.3/failure-harness-<host>.txt
# and copies every report to docs/evidence/S2.3/reports/. Prints one PASS/FAIL line per
# check and exits non-zero if any fails.
#
# Interface under test:
#   chaos/scripts/kill-leader.sh, kill-follower.sh, restart-shard.sh, isolate-replica.sh
#     common flags: --seed N (required), --out FILE (the JSON report), --shard N (default:
#     chosen by the seed), --settle-max SECONDS (default 120), --plan (print the plan as JSON
#     and touch nothing), --inject lost-write, --help. isolate-replica also --role
#     leader|follower (default: chosen by the seed); kill-follower picks the follower by seed.
#     Same seed -> same plan (shard, role, target choice, fault delay, key prefix).
#     Exit 0: every assertion passed; 1: an assertion failed (the report says which);
#     2: the harness could not reach a verdict. The report is written in every case.
#     Each run:
#       1. starts the load as a Job in namespace reliefgrid on the infra worker:
#          /invcheck load --coordinator http://storage-coordinator.reliefgrid.svc.cluster.local:8080
#          --ledger /ledger/<runId>.json, image reliefgrid/storagenode:dev (which now also ships
#          /invcheck), ledger on PVC chaos-ledger; no kubectl port-forward anywhere;
#       2. after the seeded fault delay, starts the fault on shard <shard>:
#          kill-leader      delete the leader's pod (healed: the pod is back and Ready)
#          kill-follower    delete one follower's pod (healed: likewise)
#          restart-shard    delete all three pods of the shard (healed: all back and Ready)
#          isolate-replica  cut the replica's Raft port (7000) in both directions with the
#                           chart's NetworkPolicies: the replica is labelled
#                           reliefgrid.io/chaos-isolated=true, which the Raft rule of
#                           NetworkPolicy storage-shard-N excludes as both target and peer;
#                           its data (8080) and mgmt (8081) ports stay open; healed by
#                           removing the label. The fault lasts long enough for an election.
#       3. a bounded settle phase (--settle-max) after the heal: the load Job finishes, the
#          shard is one group (one leader every member names, serverIds = manifest peerIds),
#          every member caughtUp at one appliedIndex;
#       4. /invcheck verify --ledger ... --json and then /invcheck check --json, each as a Job
#          against the same Service URL; with --inject lost-write, before verify the harness
#          first commits a write (through the coordinator) that overwrites the key of one
#          acknowledged update with a payload no acknowledged write produced.
#   invcheck load ledger (loadtest/ledger): every entry also has "at" (unix ms when its outcome
#     was known), and the ledger has "errors": [{at, operationId, key, attempt, error}] for
#     every failed attempt, including ones a retry later fixed (v3.1 §26: record temporary
#     errors rather than hiding them).
#   Report (JSON object):
#     experiment, seed, runId, pass (bool), failures [string]
#     plan {shard, role, faultDelayMs, settleMaxSeconds, prefix}
#     target {shard, pod, serverId, role: leader|follower|all, term}
#     fault {kind: pod-delete|shard-restart|raft-isolation, mechanism, startedAt, healedAt} (unix ms)
#     recovery {leader, term, seconds}: fault start -> one leader that every reachable member names
#     settle {maxSeconds, seconds, ok, detail}
#     timings {loadStartedAt, faultStartedAt, faultHealedAt, settledAt, loadFinishedAt} (unix ms)
#     load {job, coordinator, exitCode, acked, unknown, rejected, ackedDuringFault}
#       (ackedDuringFault: acknowledged with at in [faultStartedAt, faultHealedAt])
#     errorsObserved [{at, source: load|harness, message}]
#     ledger {pvc, path, job, exitCode, checked, lost [...], resolution {...}}
#     invcheck {job, exitCode, verdict, quiescent, violations [...]}
#     isolation (isolate-replica only) {peerToTargetRaft, targetToPeerRaft, peerToTargetData,
#       targetCommitAdvanced, majorityCommitAdvanced}: booleans measured during the fault
#       (raft reachability by storagectl dial; commit advance across the fault window)
#     injected (only with --inject) {kind: lost-write, key, operationId}
set -uo pipefail

CLUSTER=reliefgrid
CTX="kind-$CLUSTER"
NS=reliefgrid
RELEASE=storage
CHART=deploy/helm/storage
IMAGE=reliefgrid/storagenode:dev
COORD_URL="http://storage-coordinator.$NS.svc.cluster.local:8080"
SHARDS=(0 1 2)
EXPERIMENTS=(kill-leader kill-follower restart-shard isolate-replica)
SEED="${SEED:-23}"
SETTLE_MAX="${SETTLE_MAX:-120}"
ELECTION_MAX_SECONDS="${ELECTION_MAX_SECONDS:-10}"
RUN_TIMEOUT="${RUN_TIMEOUT:-900}" # one experiment, start to report
INSTALL_TIMEOUT="${INSTALL_TIMEOUT:-5m}"
EVIDENCE="docs/evidence/S2.3/failure-harness-$(hostname -s).txt"
REPORTS="docs/evidence/S2.3/reports"

failures=0 passes=0
r="" # the last experiment's report (run_experiment)
tmp="$(mktemp -d)"
results="$tmp/results"
: >"$results"
trap 'rm -rf "$tmp"' EXIT

pass() {
	printf 'PASS  %s\n' "$1" | tee -a "$results"
	passes=$((passes + 1))
}
fail() { # fail <description> [detail]
	{
		printf 'FAIL  %s\n' "$1"
		[ -n "${2:-}" ] && sed 's/^/      /' <<<"$2"
	} | tee -a "$results"
	failures=$((failures + 1))
}
check() { # check <description> <command...>
	local desc="$1"
	shift
	if "$@" >/dev/null 2>&1; then pass "$desc"; else fail "$desc"; fi
}
jcheck() { # jcheck <description> <file> <jq filter that must yield true>
	if jq -e "$3" "$2" >/dev/null 2>&1; then pass "$1"; else fail "$1" "$(jq -c "$3" "$2" 2>&1 | head -c 400)"; fi
}

k() { kubectl --context "$CTX" "$@"; }
kn() { k -n "$NS" "$@"; }
pod_names() { printf 'storage-shard-%s-0 storage-shard-%s-1 storage-shard-%s-2' "$1" "$1" "$1"; }
st() { timeout 5 kubectl --context "$CTX" -n "$NS" exec "$1" -c storagenode -- /storagectl status 2>/dev/null; }
field() { jq -r "$2 // empty" <<<"$1" 2>/dev/null; }

write_evidence() {
	local code_paths code_tree code_dirty result
	code_paths=(go.mod go.sum cmd internal loadtest chaos deploy/helm/storage scripts/verify-S2.3.sh)
	code_tree="$(git ls-tree -r HEAD -- "${code_paths[@]}" | git hash-object --stdin)"
	code_dirty="$(git status --porcelain -- "${code_paths[@]}" | grep -q . && echo yes || echo no)"
	if [ "$failures" -eq 0 ]; then result="PASSED ($passes/$passes checks)"; else result="FAILED ($failures of $((passes + failures)) checks failed)"; fi
	mkdir -p "$(dirname "$EVIDENCE")" "$REPORTS"
	cp "$tmp"/report-*.json "$REPORTS"/ 2>/dev/null
	{
		echo "# S2.3 failure harness"
		echo "result:     $result"
		echo "host:       $(hostname -s)"
		echo "date:       $(date -u +%Y-%m-%dT%H:%M:%SZ)"
		echo "seed base:  $SEED"
		echo "code tree:  ${code_tree:0:12} (git ls-tree -r HEAD -- ${code_paths[*]} | git hash-object --stdin)"
		echo "code dirty: $code_dirty"
		echo
		echo "## checks"
		cat "$results"
		echo
		echo "## reports (summary)"
		for f in "$tmp"/report-*.json; do
			[ -e "$f" ] || continue
			jq -c '{experiment, seed, pass, target: .target.pod, recovery: .recovery.seconds, settle: .settle.seconds,
				acked: .load.acked, ackedDuringFault: .load.ackedDuringFault, unknown: .load.unknown,
				errors: (.errorsObserved | length), lost: (.ledger.lost | length), invcheck: .invcheck.verdict}' "$f" 2>/dev/null ||
				echo "$(basename "$f"): not JSON"
		done
		echo
		echo "## kubectl get pods -o wide"
		kn get pods -o wide
	} 2>&1 | sed 's/[[:space:]]*$//' >"$EVIDENCE"
	echo "evidence: $EVIDENCE ($result)"
}
finish() {
	write_evidence
	echo
	if [ "$failures" -eq 0 ]; then
		echo "verify-S2.3: all checks passed"
		exit 0
	fi
	echo "verify-S2.3: $failures check(s) failed"
	exit 1
}
abort() {
	fail "$@"
	finish
}

# one_group <shard>: one Leader every member names, serverIds = manifest peerIds, all caught
# up at one appliedIndex. Prints why not.
one_group() {
	local shard="$1" p s lead="" n=0 ids="" applied=""
	for p in $(pod_names "$shard"); do
		s="$(st "$p")"
		[ -n "$s" ] || { echo "$p unreachable"; return 1; }
		[ "$(field "$s" .state)" = Leader ] && n=$((n + 1))
		[ -z "$lead" ] && lead="$(field "$s" .leaderId)"
		[ -n "$lead" ] && [ "$(field "$s" .leaderId)" = "$lead" ] || { echo "$p names '$(field "$s" .leaderId)', others '$lead'"; return 1; }
		[ "$(field "$s" .caughtUp)" = true ] || { echo "$p not caught up: $(jq -c '{appliedIndex, commitIndex, leaderCommitIndex}' <<<"$s")"; return 1; }
		[ -z "$applied" ] && applied="$(field "$s" .appliedIndex)"
		[ "$(field "$s" .appliedIndex)" = "$applied" ] || { echo "$p at appliedIndex $(field "$s" .appliedIndex), others $applied"; return 1; }
		ids+="$(field "$s" .serverId)"$'\n'
	done
	[ "$n" -eq 1 ] || { echo "$n pods are Leader"; return 1; }
	[ "$(sort <<<"$ids" | grep .)" = "$(kn get configmap "storage-shard-$shard-manifest" -o jsonpath='{.data.peerIds}' | jq -r '.[]' | sort)" ] ||
		{ echo "serverIds differ from the manifest's peerIds"; return 1; }
}
wait_group() { # wait_group <shard> <seconds>
	local i why
	for ((i = 0; i < $2 * 2; i++)); do
		why="$(one_group "$1")" && return 0
		sleep 0.5
	done
	echo "$why"
	return 1
}

# run_job <name> <args...>: run /invcheck <args> as a Job on the infra worker with PVC
# chaos-ledger at /ledger; wait; print its logs; return its exit code.
run_job() {
	local name="$1" code
	shift
	kn delete job "$name" --ignore-not-found --wait >/dev/null 2>&1
	jq -n --arg name "$name" --arg image "$IMAGE" --args '$ARGS.positional' -- "$@" |
		jq --arg name "$name" --arg image "$IMAGE" '{
			apiVersion: "batch/v1", kind: "Job", metadata: {name: $name, labels: {"reliefgrid.io/verify": "S2.3"}},
			spec: {backoffLimit: 0, template: {spec: {
				restartPolicy: "Never",
				nodeSelector: {role: "infra"},
				tolerations: [{key: "role", value: "infra", effect: "NoSchedule"}],
				securityContext: {runAsNonRoot: true, runAsUser: 65532, runAsGroup: 65532},
				volumes: [{name: "ledger", persistentVolumeClaim: {claimName: "chaos-ledger"}}],
				containers: [{name: "invcheck", image: $image, imagePullPolicy: "Never",
					command: (["/invcheck"] + .), volumeMounts: [{name: "ledger", mountPath: "/ledger"}]}]}}}}' |
		kn apply -f - >/dev/null || return 99
	kn wait --for=jsonpath='{.status.conditions[?(@.status=="True")].type}' "job/$name" --timeout=10m >/dev/null 2>&1
	kn logs "job/$name" --tail=-1 2>/dev/null
	code="$(kn get pods -l "job-name=$name" -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.exitCode}' 2>/dev/null)"
	return "${code:-98}"
}

echo "== 0. preconditions"
for t in kubectl helm jq docker kind git; do command -v "$t" >/dev/null || abort "tool $t is installed"; done
k get nodes >/dev/null 2>&1 || abort "kind cluster $CLUSTER is running (make cluster-up)"
make -s storage-image coordinator-image >"$tmp/build.log" 2>&1 || abort "images build and load into kind" "$(tail -20 "$tmp/build.log")"
helm --kube-context "$CTX" upgrade --install "$RELEASE" "$CHART" -n "$NS" --create-namespace --wait --timeout "$INSTALL_TIMEOUT" >"$tmp/helm.log" 2>&1 ||
	abort "helm upgrade --install $RELEASE" "$(tail -20 "$tmp/helm.log")"
for s in "${SHARDS[@]}"; do
	why="$(wait_group "$s" 180)" && pass "start: shard-$s is one caught-up group" || abort "start: shard-$s is one caught-up group" "$why"
done

echo "== 1. interface"
for e in "${EXPERIMENTS[@]}"; do
	f="chaos/scripts/$e.sh"
	check "$f exists and is executable" test -x "$f"
	check "$f --help exits 0" "$f" --help
done
docker run --rm --entrypoint /invcheck "$IMAGE" >/dev/null 2>&1
rc=$?
[ "$rc" = 64 ] && pass "$IMAGE ships /invcheck (usage exit 64)" || fail "$IMAGE ships /invcheck (usage exit 64)" "exit $rc"
if [ ! -d chaos/scripts ]; then
	fail "chaos/ never port-forwards (the load and checks run in-cluster)" "no chaos/scripts directory"
elif grep -rn "port-forward" chaos/ >/dev/null 2>&1; then
	fail "chaos/ never port-forwards (the load and checks run in-cluster)" "$(grep -rn port-forward chaos/)"
else
	pass "chaos/ never port-forwards (the load and checks run in-cluster)"
fi

echo "== 2. seeded plans"
for e in "${EXPERIMENTS[@]}"; do
	f="chaos/scripts/$e.sh"
	a="$("$f" --seed 7 --plan 2>/dev/null)" b="$("$f" --seed 7 --plan 2>/dev/null)"
	if jq -e '.seed == 7 and (.shard | type == "number") and (.faultDelayMs | type == "number") and (.prefix | type == "string")' <<<"$a" >/dev/null 2>&1 && [ "$a" = "$b" ]; then
		pass "$e: --seed 7 --plan is a JSON plan, the same both times"
	else
		fail "$e: --seed 7 --plan is a JSON plan, the same both times" "first:  $a"$'\n'"second: $b"
	fi
	shards="$(for i in $(seq 1 12); do "$f" --seed "$i" --plan 2>/dev/null | jq -r .shard; done | sort -u | grep -c .)"
	[ "$shards" -ge 2 ] && pass "$e: seeds 1..12 choose $shards different shards" || fail "$e: seeds 1..12 choose at least 2 different shards" "$shards"
done

# run_experiment <experiment> <seed> <expected exit> [flags...]: runs it, checks the exit code
# and sets r to its report, $tmp/report-<experiment>[-<tag>].json. Never call it in $(...):
# its PASS/FAIL lines and counts must reach this shell.
run_experiment() {
	local e="$1" seed="$2" want="$3" out rc
	shift 3
	out="$tmp/report-$e${TAG:+-$TAG}.json"
	timeout "$RUN_TIMEOUT" "chaos/scripts/$e.sh" --seed "$seed" --settle-max "$SETTLE_MAX" --out "$out" "$@" >"$tmp/$e.log" 2>&1
	rc=$?
	[ "$rc" = "$want" ] && pass "$e (seed $seed${*:+, $*}): exit $want" || fail "$e (seed $seed${*:+, $*}): exit $want" "exit $rc; last lines:"$'\n'"$(tail -15 "$tmp/$e.log")"
	r="$out"
	[ -s "$out" ] && jq -e . "$out" >/dev/null 2>&1 && return 0
	fail "$e: the report is JSON" "$(head -c 300 "$out" 2>/dev/null || echo "no report at $out")"
	return 1
}

# common_checks <experiment> <report>: what every clean run must show, from the report.
common_checks() {
	local e="$1" r="$2"
	jcheck "$e: report pass, no failures" "$r" '.pass == true and (.failures | length) == 0'
	jcheck "$e: report names experiment, seed, runId and target" "$r" \
		"(.experiment == \"$e\") and (.seed | type == \"number\") and (.runId | type == \"string\") and (.target.pod | type == \"string\") and (.target.serverId | type == \"string\")"
	jcheck "$e: fault started under load and healed before the load finished" "$r" \
		'.timings.loadStartedAt < .timings.faultStartedAt and .timings.faultStartedAt < .timings.faultHealedAt and .timings.faultHealedAt < .timings.loadFinishedAt and .fault.startedAt == .timings.faultStartedAt'
	jcheck "$e: settled within its bound" "$r" '.settle.ok == true and .settle.seconds <= .settle.maxSeconds and .settle.maxSeconds > 0'
	jcheck "$e: load ran as a Job against the coordinator Service and finished cleanly" "$r" \
		"(.load.job | type == \"string\") and .load.coordinator == \"$COORD_URL\" and .load.exitCode == 0 and .load.acked > 0"
	jcheck "$e: writes were acknowledged during the fault" "$r" '.load.ackedDuringFault > 0'
	jcheck "$e: errorsObserved is a list of {at, source, message}" "$r" \
		'(.errorsObserved | type == "array") and all(.errorsObserved[]; (.at | type == "number") and (.source == "load" or .source == "harness") and (.message | type == "string"))'
	jcheck "$e: ledger: zero acknowledged writes lost" "$r" '.ledger.exitCode == 0 and (.ledger.lost | length) == 0 and .ledger.checked >= .load.acked'
	jcheck "$e: invcheck check clean and quiescent" "$r" '.invcheck.exitCode == 0 and .invcheck.verdict == "clean" and .invcheck.quiescent == true'
}

# independent_checks <experiment> <report>: the cluster and the ledger, without trusting the report.
independent_checks() {
	local e="$1" r="$2" shard job args why out rc
	shard="$(jq -r .target.shard "$r")"
	why="$(wait_group "$shard" 30)" && pass "$e: shard-$shard is one caught-up group afterwards" || fail "$e: shard-$shard is one caught-up group afterwards" "$why"
	job="$(jq -r .load.job "$r")"
	args="$(kn get job "$job" -o jsonpath='{.spec.template.spec.containers[0].command}' 2>/dev/null)"
	if jq -e --arg u "$COORD_URL" 'index("/invcheck") == 0 and index("load") == 1 and any(.[]; . == $u or . == "--coordinator=" + $u)' <<<"${args:-null}" >/dev/null 2>&1; then
		pass "$e: Job $job runs /invcheck load against $COORD_URL"
	else
		fail "$e: Job $job runs /invcheck load against $COORD_URL" "${args:-no such Job}"
	fi
	out="$(run_job "s23-verify-$e" verify --coordinator "$COORD_URL" --ledger "$(jq -r .ledger.path "$r")" --json)"
	rc=$?
	[ "$rc" = 0 ] && jq -e '.ok == true and (.verdict.lost | length) == 0' <<<"$out" >/dev/null 2>&1 &&
		pass "$e: an independent invcheck verify Job finds zero lost" || fail "$e: an independent invcheck verify Job finds zero lost" "exit $rc: $(head -c 600 <<<"$out")"
	[ -z "$(kn get pods -l reliefgrid.io/chaos-isolated -o name 2>/dev/null)" ] && pass "$e: no pod is left isolated" ||
		fail "$e: no pod is left isolated" "$(kn get pods -l reliefgrid.io/chaos-isolated -o name)"
}

echo "== 3. kill-leader under load"
if run_experiment kill-leader $((SEED + 1)) 0; then
	common_checks kill-leader "$r"
	jcheck "kill-leader: the target was the leader and a leader at a higher term took over within ${ELECTION_MAX_SECONDS}s" "$r" \
		".target.role == \"leader\" and .fault.kind == \"pod-delete\" and .recovery.term > .target.term and .recovery.seconds <= $ELECTION_MAX_SECONDS"
	independent_checks kill-leader "$r"
fi

echo "== 4. kill-follower under load"
if run_experiment kill-follower $((SEED + 2)) 0; then
	common_checks kill-follower "$r"
	jcheck "kill-follower: the target was a follower and the leader kept its term" "$r" \
		'.target.role == "follower" and .fault.kind == "pod-delete" and .recovery.term == .target.term'
	independent_checks kill-follower "$r"
fi

echo "== 5. restart-shard under load"
if run_experiment restart-shard $((SEED + 3)) 0; then
	common_checks restart-shard "$r"
	jcheck "restart-shard: all three pods restarted and the group re-formed at a higher term" "$r" \
		'.target.role == "all" and .fault.kind == "shard-restart" and .recovery.term > .target.term'
	independent_checks restart-shard "$r"
fi

echo "== 6. isolate the leader's Raft port under load"
if run_experiment isolate-replica $((SEED + 4)) 0 --role leader; then
	common_checks isolate-replica "$r"
	jcheck "isolate-replica: Raft cut both ways, data port open, by the chart's NetworkPolicy" "$r" \
		'.fault.kind == "raft-isolation" and .isolation.peerToTargetRaft == false and .isolation.targetToPeerRaft == false and .isolation.peerToTargetData == true'
	jcheck "isolate-replica: the isolated old leader committed nothing while the majority moved on under a new leader" "$r" \
		'.target.role == "leader" and .isolation.targetCommitAdvanced == false and .isolation.majorityCommitAdvanced == true and .recovery.leader != .target.pod and .recovery.term > .target.term'
	independent_checks isolate-replica "$r"
	shard="$(jq -r .target.shard "$r")" pod="$(jq -r .target.pod "$r")"
	peer="$(for p in $(pod_names "$shard"); do [ "$p" != "$pod" ] && echo "$p" && break; done)"
	check "isolate-replica: after the heal $peer reaches $pod:7000 again" \
		kn exec "$peer" -c storagenode -- /storagectl dial --addr "$pod.storage-shard-$shard.$NS.svc.cluster.local:7000" --timeout 3s
fi

echo "== 7. an injected lost write is caught"
if TAG=inject run_experiment kill-follower $((SEED + 5)) 1 --inject lost-write; then
	jcheck "inject: report fails, naming the ledger" "$r" '.pass == false and any(.failures[]; test("ledger|lost"; "i"))'
	jcheck "inject: the ledger names the injected key as lost" "$r" \
		'. as $r | $r.injected.kind == "lost-write" and ($r.injected.key | type == "string") and any($r.ledger.lost[]; .entry.key == $r.injected.key) and $r.ledger.exitCode == 1'
	out="$(run_job s23-verify-inject verify --coordinator "$COORD_URL" --ledger "$(jq -r .ledger.path "$r")" --json)"
	rc=$?
	[ "$rc" = 1 ] && jq -e --arg k "$(jq -r .injected.key "$r")" 'any(.verdict.lost[]; .entry.key == $k)' <<<"$out" >/dev/null 2>&1 &&
		pass "inject: an independent invcheck verify Job also reports the key lost (exit 1)" ||
		fail "inject: an independent invcheck verify Job also reports the key lost (exit 1)" "exit $rc: $(head -c 600 <<<"$out")"
fi

finish

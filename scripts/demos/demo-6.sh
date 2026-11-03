#!/usr/bin/env bash
# demo-6 (owner S, goal S3.2): v3.1 §28 row 6.
#
# What to show:   Stop the leader storage pod, continue reads/writes through a surviving replica
# Acceptance:     Majority-backed service resumes; acknowledged-write ledger passes
#
# Seed/reset: from the gate's clean deployment (or any healthy one: nothing is assumed about
# existing data). DEMO6_SHARD (default 1) is the shard whose leader is stopped; DEMO6_SEED
# (6) names the load's keys (demo6-<seed>-<time>). The load, the ledger check and the
# invariant check are the failure harness's in-cluster Jobs (chaos/scripts/lib.sh, S2.3:
# invcheck from the tools image against the coordinator Service, the ledger on PVC
# chaos-ledger); nothing goes through a port-forward from this host. DEMO6_LOAD_SECONDS (40)
# of writes; the leader is stopped DEMO6_WARMUP_SECONDS (8) after the load starts.
#
# "Stopped" means its worker is cordoned and the pod deleted, so the StatefulSet's
# replacement stays Pending (its PV and the required anti-affinity both pin it to that
# worker). It stays down to the end: the ledger and invariant checks run while it is down,
# so their reads go through the surviving majority too. On PASS the stopped pod and the
# cordon are handed to demo 7 through an annotation on that worker (DEMO_HANDOFF_ANNOTATION),
# which demo 7 finds from any run; on any other exit the worker is uncordoned and the shard
# heals.
#
# It refuses to start, changing nothing, if any shard is already missing a replica (a pending
# hand-off included): stopping another would cost that shard its majority.
#
# Assertions:
#   1. Placement holds before the stop (make placement-check): one replica per data worker.
#   2. The load Job is running when the leader stops.
#   3. The leader is stopped: its replacement stays Pending, unscheduled.
#   4. A new leader, another pod, at a higher term, within DEMO6_ELECTION_SECONDS (10).
#   5. The load finishes (exit 0) with acknowledged writes, and some were acknowledged while
#      the old leader was down (invcheck verify --window from the stop to the load's end).
#   6. invcheck verify: every acknowledged write survived (0 lost).
#   7. invcheck check: the stored state is complete, clean and quiescent. A check that was not
#      quiescent (exit 3: the app wrote during the walk) is rerun for DEMO6_QUIESCE_SECONDS (90).
#   8. The old leader stayed down throughout: still the same Pending replacement after the
#      checks, so every write and read above was the surviving majority's.
. "$(dirname "$0")/lib.sh"
demo_begin 6 "Stop a shard's leader under load; a surviving majority keeps serving"
CHAOS_CONTEXT="${KUBE_CONTEXT:-kind-$CLUSTER}" CHAOS_NAMESPACE="${NAMESPACE:-reliefgrid}"
# The harness's Job helpers (ensure_pvc, start_job, run_job, job_*); kube.sh, sourced after
# it, keeps its own k, kn, st, field and group_state.
# shellcheck source=chaos/scripts/lib.sh
. "$REPO_ROOT/chaos/scripts/lib.sh"
# shellcheck source=scripts/demos/kube.sh
. "$(dirname "$0")/kube.sh"

SHARD="${DEMO6_SHARD:-1}"
SEED="${DEMO6_SEED:-6}"
LOAD_SECONDS="${DEMO6_LOAD_SECONDS:-40}"
WARMUP_SECONDS="${DEMO6_WARMUP_SECONDS:-8}"
ELECTION_SECONDS="${DEMO6_ELECTION_SECONDS:-10}"
run_id="demo6-$(date +%s)" work="$DEMO_TMP" # the harness's names: Job labels, its notes
load_job="$run_id-load" ledger_path="/ledger/$run_id.json"
demo_reset "healthy deployment; load Job $load_job (seed $SEED, ${LOAD_SECONDS}s, 4 writers, ledger $PVC:$ledger_path); stop shard $SHARD's leader after ${WARMUP_SECONDS}s (cordon its worker, delete the pod)"
missing="$(missing_replicas)"
case $? in
2) fail "preflight: $missing"; demo_end ;;
esac
if [ -n "$missing" ]; then
	first="${missing%%$'\n'*}"
	fail "refuse: ${first%%:*} is missing a replica; demo 6 needs all three replicas of every shard (run demo 7 to finish a pending hand-off)" "$missing"
	demo_end
fi
require_preflight

# Whatever happens, the worker is uncordoned and a running load stopped on exit, unless the
# stopped pod is handed to demo 7.
handed_off=""
restore() { [ -n "${STOP_NODE:-}" ] && [ -z "$handed_off" ] && demo_uncordon "$STOP_NODE"; }
stop_load() { job_done "$load_job" || kn delete job "$load_job" --ignore-not-found --wait=false >/dev/null; }
demo_on_exit restore
demo_on_exit stop_load

# --- 1, 2: placement and the load -------------------------------------------------------------
check "placement: one replica of every shard per data worker (make placement-check)" make -s -C "$REPO_ROOT" placement-check
ensure_pvc || { fail "load: PVC $PVC for the ledger"; demo_end; }
start_job "$load_job" load --coordinator "$COORD_URL" --ledger "$ledger_path" --prefix "demo6-$SEED-$(date +%s)" \
	--workers 4 --keys 4 --ops 0 --duration "${LOAD_SECONDS}s" --interval 50ms --timeout "$((LOAD_SECONDS + 120))s" ||
	{ fail "load: create Job $load_job"; demo_end; }
started=""
for ((i = 0; i < 240; i++)); do
	started="$(kn get pods -l "job-name=$load_job" -o jsonpath='{.items[0].status.containerStatuses[0].state.running.startedAt}' 2>/dev/null)"
	[ -n "$started" ] && break
	job_done "$load_job" && break
	sleep 0.5
done
[ -n "$started" ] || { fail "load: Job $load_job running" "$(kn get pods -l "job-name=$load_job" -o wide 2>&1)"; demo_end; }
load_started_ms="$(iso_ms "$started")"
while [ "$(now_ms)" -lt $((load_started_ms + WARMUP_SECONDS * 1000)) ]; do sleep 0.2; done

# --- 3: stop the leader ------------------------------------------------------------------------
if stop_leader_pod "$SHARD"; then
	pass "stop: $STOP_POD ($STOP_ID, leader at term $STOP_TERM) deleted with $STOP_NODE cordoned; its replacement is Pending"
else
	fail "stop: the leader is stopped and stays Pending" "$STOP_WHY"
	demo_end
fi
job_done "$load_job" && fail "load: running when the leader stopped" "it had already finished" ||
	pass "load: $load_job running when the leader stopped ($(((STOP_MS - load_started_ms) / 1000)) s in)"

# --- 4: election -------------------------------------------------------------------------------
if g="$(wait_leader "$SHARD" "$ELECTION_SECONDS" $((STOP_TERM + 1)) "$STOP_POD")"; then
	pass "election: ${g% *} leads shard $SHARD at term ${g#* } (was $STOP_POD at $STOP_TERM), $(($(now_ms) - STOP_MS)) ms after the stop"
else
	fail "election: another pod leads shard $SHARD at a higher term within ${ELECTION_SECONDS}s" "group: $(group_state "$SHARD")"
fi

# --- 5: the load -------------------------------------------------------------------------------
deadline=$((load_started_ms + (LOAD_SECONDS + 120) * 1000))
while [ "$(now_ms)" -lt "$deadline" ] && ! job_done "$load_job"; do sleep 0.5; done
load_exit="$(job_exit "$load_job")"
load_end_ms="$(iso_ms "$(kn get pods -l "job-name=$load_job" -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.finishedAt}' 2>/dev/null)")"
run "load Job output" kn logs "job/$load_job" --tail=20

# --- 6, 7: the ledger and the invariants, with the old leader still down -------------------------
verify_json="$(run_job "$run_id-verify" verify --coordinator "$COORD_URL" --ledger "$ledger_path" --json --window "$STOP_MS:${load_end_ms:-$(now_ms)}")"
verify_exit=$?
run "invcheck verify (Job $run_id-verify, window from the stop to the load's end)" jq '{ok, ledger: (.ledger | del(.errors)), resolution, verdict: (.verdict | {checked, keys, unknown, lost: (.lost | length)})}' <<<"$verify_json"
acked="$(jq '.ledger.acked // 0' <<<"$verify_json" 2>/dev/null)"
in_window="$(jq '.ledger.ackedInWindow // 0' <<<"$verify_json" 2>/dev/null)"
[ "${load_exit:-x}" = 0 ] && [ "${acked:-0}" -gt 0 ] && [ "${in_window:-0}" -gt 0 ] &&
	pass "load: exit 0, $acked writes acknowledged, $in_window of them while $STOP_POD was down" ||
	fail "load: exit 0 with writes acknowledged while the leader was down" "exit ${load_exit:-?}, acked ${acked:-?}, in the window ${in_window:-?}"
[ "$verify_exit" = 0 ] && pass "ledger: every acknowledged write survived (invcheck verify, $(jq '.verdict.checked' <<<"$verify_json") checked)" ||
	fail "ledger: every acknowledged write survived (invcheck verify exit $verify_exit)" "$(jq -c '.verdict.lost[:3] // .incomplete' <<<"$verify_json" 2>/dev/null)"
# invcheck check exits 3 when a write committed during its walk (not quiescent): with the app
# installed its background writes can do that at any time, so only that exit is retried, with a
# new Job, for DEMO6_QUIESCE_SECONDS (90). Violations (1) or an incomplete read (2) fail at once.
check_once() { run_job "$run_id-check-$RETRY_ATTEMPT" check --coordinator "$COORD_URL" --json; }
check_json="$(retry_on 3 "${DEMO6_QUIESCE_SECONDS:-90}" check_once)"
check_exit=$?
retried="$(cat "$DEMO_TMP/retried" 2>/dev/null | grep -c '^check_once ')"
run "invcheck check (Jobs $run_id-check-N; $retried retried as not quiescent)" jq '{ok, verdict, incomplete}' <<<"$check_json"
[ "$check_exit" = 0 ] && pass "invariants: complete, clean and quiescent (invcheck check)" ||
	fail "invariants: complete, clean and quiescent (invcheck check exit $check_exit)" "$(jq -c '.report.violations[:3] // .incomplete' <<<"$check_json" 2>/dev/null)"

# --- 8: it stayed down ------------------------------------------------------------------------------
still_stopped "$STOP_POD" "$STOP_UID" && pass "stopped throughout: $STOP_POD is still the same Pending replacement after the checks" ||
	fail "stopped throughout: the old leader stayed down" "$(kn get pod "$STOP_POD" -o wide 2>&1)"

# --- hand-off to demo 7 ---------------------------------------------------------------------------------
if [ "$DEMO_FAILURES" -eq 0 ]; then
	value="shard=$SHARD pod=$STOP_POD node=$STOP_NODE serverId=$STOP_ID uid=$STOP_UID term=$STOP_TERM"
	if k annotate node "$STOP_NODE" "$DEMO_HANDOFF_ANNOTATION=$value" --overwrite >/dev/null; then
		handed_off=1
		demo_note "handed to demo 7: node/$STOP_NODE $DEMO_HANDOFF_ANNOTATION=\"$value\" ($STOP_POD stopped, $STOP_NODE cordoned)"
	else
		demo_note "could not annotate $STOP_NODE: $STOP_POD is restarted instead of handed to demo 7"
	fi
fi
demo_end

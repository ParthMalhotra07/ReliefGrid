#!/usr/bin/env bash
# Verify check for S4.2 (M7 admission gate and storagectl replace; closes #20 and #21).
# Run from the repo root against the local "reliefgrid" kind cluster:
#   scripts/verify-S4.2.sh
#   SHARD=2 POD=storage-shard-2-0 scripts/verify-S4.2.sh
# It builds and loads the images, upgrades deploy/helm/storage, then wipes one replica's disk
# under load and replaces it with storagectl, crashing the tool at every phase. It never
# recreates the cluster. Evidence: docs/evidence/S4.2/admission-replace-<host>.txt. One
# PASS/FAIL line per check; exits non-zero if any fails.
#
# Interface under test:
#   storage pods (deploy/helm/storage)
#     env MANIFEST_NAME=storage-shard-N-manifest and POD_NAMESPACE; Role storage-admission:
#       get on the three manifest ConfigMaps by name, nothing else (the pods' service account)
#     a disk is admitted (M7) iff: the manifest is PENDING (fresh install), or its ServerID is
#       in peerIds, or admitted[<podName>] == <incarnation>; a disk with Raft state is an
#       ordinary restart unless its ServerID is in removed. An empty disk with an unreadable
#       manifest is not admitted.
#     until admitted the pod serves only management (8081): /mgmt/status {state: "Unadmitted",
#       admitted: false, admissionReason, incarnation, serverId}; /readyz 503; Raft (7000) and
#       data (8080) are not listening; operator POSTs answer 409 UNADMITTED
#   GET /mgmt/status       also configuration {index, committed, servers [{id, address,
#                          suffrage: Voter|Nonvoter}]} (#21)
#   POST /mgmt/membership  operator token; {op: remove|add-nonvoter|add-voter, id, address,
#                          prevIndex}: 200 {index}; 409 NOT_LEADER / CONFIG_CHANGED; 503 outcome
#                          unknown (re-read)
#   contracts/internal.yaml Heartbeat.configuration (RaftConfiguration), additive; the coordinator
#                          admits a registration (admitted, /admin/nodes) iff its ServerID is in
#                          the shard's committed configuration as the leader reports it, and never
#                          routes to an unadmitted node
#   manifest ConfigMap     also admitted (JSON {podName: incarnation}), removed (JSON [serverId]),
#                          replace (JSON {podName, oldId, newId, phase, configIndex}) while one runs
#   make storage-replace SHARD=N POD=<pod> [CRASH_AT=<step>]
#                          runs /storagectl replace --shard N --pod <pod> as Job storage-replace-<pod>
#                          (operator-labelled, the bootstrap service account); exit 0 done (or
#                          already done), 2 refused (nothing changed), 1 error/crash (resume by
#                          running it again). STORAGECTL_CRASH_AT=<step> (tests and this Verify
#                          only) stops it right after that step's effect, before its record.
#                          Steps: planned, removed, recorded-removed, admitted, nonvoter,
#                          recorded-nonvoter, voter, recorded-voter
#   storagectl replace logs one line per phase: "phase=<PLANNED|REMOVED|ADMITTED|NONVOTER|VOTER>",
#                          then "replaced" naming the new voters
#   storagectl bootstrap   the fence also needs the leader's committed configuration to be exactly
#                          the manifest's peerIds as voters; after a replace, a re-run verifies
#                          against the updated peerIds
set -uo pipefail

CLUSTER=reliefgrid
CTX="kind-$CLUSTER"
NS=reliefgrid
RELEASE=storage
CHART=deploy/helm/storage
SHARD="${SHARD:-1}"
POD="${POD:-storage-shard-$SHARD-2}"
STEPS=(planned removed recorded-removed admitted nonvoter recorded-nonvoter voter recorded-voter)
INSTALL_TIMEOUT="${INSTALL_TIMEOUT:-5m}"
UNADMITTED_HOLD="${UNADMITTED_HOLD:-20}" # seconds the wiped pod is watched before replace
EVIDENCE="docs/evidence/S4.2/admission-replace-$(hostname -s).txt"

failures=0 passes=0
tmp="$(mktemp -d)"
results="$tmp/results"
: >"$results"
load_job=""
trap '[ -n "$load_job" ] && kn delete job "$load_job" --wait=false >/dev/null 2>&1; rm -rf "$tmp"' EXIT

pass() {
	printf 'PASS  %s\n' "$1" | tee -a "$results"
	passes=$((passes + 1))
}
fail() {
	{
		printf 'FAIL  %s\n' "$1"
		[ -n "${2:-}" ] && sed 's/^/      /' <<<"$2"
	} | tee -a "$results"
	failures=$((failures + 1))
}
check() {
	local d="$1"
	shift
	if "$@" >/dev/null 2>&1; then pass "$d"; else fail "$d"; fi
}
k() { kubectl --context "$CTX" "$@"; }
kn() { k -n "$NS" "$@"; }
pods() { printf 'storage-shard-%s-0 storage-shard-%s-1 storage-shard-%s-2' "$SHARD" "$SHARD" "$SHARD"; }
st() { timeout 5 kubectl --context "$CTX" -n "$NS" exec "$1" -c storagenode -- /storagectl status 2>/dev/null; }
field() { jq -r "$2 // empty" <<<"$1" 2>/dev/null; }
raft_addr() { printf '%s.storage-shard-%s.%s.svc.cluster.local:%s' "$1" "$SHARD" "$NS" "$2"; }
dial() { kn exec "$1" -c storagenode -- /storagectl dial --addr "$2" --timeout 3s >/dev/null 2>&1; }
manifest() { kn get configmap "storage-shard-$SHARD-manifest" -o json | jq -c .data; }
mfield() { jq -r "$1 // empty" <<<"$(manifest)"; }
leader_pod() { local p; for p in $(pods); do [ "$(field "$(st "$p")" .state)" = Leader ] && echo "$p" && return; done; }
# committed_voters <pod>: the committed configuration's voters as that pod reports it, sorted
committed_voters() {
	jq -r 'select(.configuration.committed == true) | [.configuration.servers[] | select(.suffrage == "Voter") | .id] | sort | join(" ")' <<<"$(st "$1")" 2>/dev/null
}
voter_count() { jq -r '[.configuration.servers[]? | select(.suffrage == "Voter")] | length' <<<"$(st "$(leader_pod)")" 2>/dev/null; }

finish() {
	local result
	if [ "$failures" -eq 0 ]; then result="PASSED ($passes/$passes checks)"; else result="FAILED ($failures of $((passes + failures)) checks failed)"; fi
	mkdir -p "$(dirname "$EVIDENCE")"
	{
		echo "# S4.2 admission gate and storagectl replace"
		echo "result:     $result"
		echo "host:       $(hostname -s)"
		echo "date:       $(date -u +%Y-%m-%dT%H:%M:%SZ)"
		echo "code tree:  $(git ls-tree -r HEAD -- go.mod go.sum cmd internal deploy/helm/storage contracts scripts/verify-S4.2.sh | git hash-object --stdin | cut -c1-12)"
		echo "code dirty: $(git status --porcelain -- cmd internal deploy/helm/storage contracts scripts/verify-S4.2.sh | grep -q . && echo yes || echo no)"
		echo "shard/pod:  $SHARD/$POD"
		echo
		echo "## checks"
		cat "$results"
		echo
		echo "## manifest"
		manifest | jq .
		echo
		echo "## replace Job logs"
		cat "$tmp"/replace-*.log 2>/dev/null
	} | sed 's/[[:space:]]*$//' >"$EVIDENCE"
	echo "evidence: $EVIDENCE ($result)"
	[ "$failures" -eq 0 ] && { echo "verify-S4.2: all checks passed"; exit 0; }
	echo "verify-S4.2: $failures check(s) failed"
	exit 1
}
abort() {
	fail "$@"
	finish
}

# wait_for <seconds> <command...>: until it succeeds
wait_for() {
	local i n=$(($1 * 2))
	shift
	for ((i = 0; i < n; i++)); do "$@" >/dev/null 2>&1 && return 0; sleep 0.5; done
	return 1
}
group_ok() { # three committed voters every member names, all caught up
	local p s lead=""
	for p in $(pods); do
		s="$(st "$p")"
		[ "$(field "$s" .caughtUp)" = true ] || return 1
		[ -z "$lead" ] && lead="$(field "$s" .leaderId)"
		[ "$(field "$s" .leaderId)" = "$lead" ] || return 1
	done
	[ "$(voter_count)" = 3 ]
}
replace_job() { # replace_job <crash step or ""> -> exit code of the Job
	local name="storage-replace-$POD" code
	kn delete job "$name" --ignore-not-found --wait >/dev/null 2>&1
	make -s storage-replace SHARD="$SHARD" POD="$POD" CRASH_AT="$1" >/dev/null 2>&1 || return 98
	kn wait --for=jsonpath='{.status.conditions[?(@.status=="True")].type}' "job/$name" --timeout=5m >/dev/null 2>&1
	kn logs "job/$name" --tail=-1 >"$tmp/replace-${1:-final}.log" 2>&1
	code="$(kn get pods -l "job-name=$name" -o jsonpath='{.items[0].status.containerStatuses[0].state.terminated.exitCode}')"
	return "${code:-97}"
}

echo "== 0. install and #21"
make -s storage-image coordinator-image tools-image >"$tmp/build.log" 2>&1 || abort "images build" "$(tail -20 "$tmp/build.log")"
helm --kube-context "$CTX" upgrade --install "$RELEASE" "$CHART" -n "$NS" --wait --wait-for-jobs --timeout "$INSTALL_TIMEOUT" >"$tmp/helm.log" 2>&1 ||
	abort "helm upgrade --install" "$(tail -20 "$tmp/helm.log")"
wait_for 180 group_ok && pass "shard-$SHARD: three committed voters, caught up" || abort "shard-$SHARD: three committed voters, caught up"
want="$(mfield '.peerIds | fromjson | sort | join(" ")')"
for p in $(pods); do
	[ "$(committed_voters "$p")" = "$want" ] && pass "#21: $p reports the committed configuration = manifest peerIds" ||
		fail "#21: $p reports the committed configuration = manifest peerIds" "$(st "$p" | jq -c .configuration)"
done
job="$(kn get jobs -l app.kubernetes.io/component=storage-bootstrap -o name | tail -1)"
kn logs "$job" --tail=-1 2>/dev/null | grep -q "configuration" && pass "#21: the bootstrap fence names the committed configuration" ||
	fail "#21: the bootstrap fence names the committed configuration"
role="$(kn get role storage-admission -o json 2>/dev/null)"
jq -e '.rules | length == 1 and .[0].resources == ["configmaps"] and .[0].verbs == ["get"] and (.[0].resourceNames | length) == 3' <<<"$role" >/dev/null 2>&1 &&
	pass "Role storage-admission: get on the three manifests only" || fail "Role storage-admission: get on the three manifests only" "$role"

echo "== 1. an ordinary restart (intact disk) needs no admission"
before="$(field "$(st "$POD")" .serverId)"
kn delete pod "$POD" --wait=false >/dev/null
wait_for 120 group_ok && [ "$(field "$(st "$POD")" .serverId)" = "$before" ] &&
	pass "restart: $POD back as $before without any manifest change" || fail "restart: $POD back as $before without any manifest change"
[ -z "$(mfield '.admitted')" ] && pass "restart: nothing written to admitted" || fail "restart: nothing written to admitted" "$(manifest)"

echo "== 2. load"
# The S2.3 harness's Job helpers: invcheck load as an in-cluster Job, ledger on PVC chaos-ledger.
load_job="s42-load-$(date +%s)" work="$tmp" run_id="$load_job"
: >"$work/notes"
# shellcheck source=chaos/scripts/lib.sh
. chaos/scripts/lib.sh
ensure_pvc || abort "PVC chaos-ledger"
start_job "$load_job" load --coordinator "$COORD_URL" --ledger "/ledger/$load_job.json" --prefix "$load_job" \
	--workers 2 --keys 4 --ops 0 --duration 900s --interval 100ms --timeout 1200s || abort "the load Job starts"

echo "== 3. wipe $POD: the empty disk stays unadmitted"
old_id="$(field "$(st "$POD")" .serverId)"
term0="$(field "$(st "$(leader_pod)")" .term)"
pvc="$(kn get pod "$POD" -o jsonpath='{.spec.volumes[?(@.persistentVolumeClaim)].persistentVolumeClaim.claimName}')"
kn delete pvc "$pvc" --wait=false >/dev/null
kn delete pod "$POD" --wait=false >/dev/null
wait_for 180 bash -c "[ \"\$(kubectl --context $CTX -n $NS exec $POD -c storagenode -- /storagectl status 2>/dev/null | jq -r .state)\" = Unadmitted ]" &&
	pass "wipe: $POD is back, Unadmitted" || abort "wipe: $POD is back, Unadmitted" "$(st "$POD")"
s="$(st "$POD")"
new_id="$(field "$s" .serverId)"
[ "$new_id" != "$old_id" ] && [ "$(field "$s" .hasState)" = false ] && [ "$(field "$s" .admitted)" = false ] &&
	pass "wipe: new incarnation $new_id, no Raft state, not admitted ($(field "$s" .admissionReason))" ||
	fail "wipe: new incarnation, no Raft state, not admitted" "$s"
peer="$(leader_pod)"
for i in $(seq 1 "$UNADMITTED_HOLD"); do
	dial "$peer" "$(raft_addr "$POD" 7000)" && { fail "wipe: $peer reached $POD:7000 before admission (after ${i}s)"; break; }
	dial "$peer" "$(raft_addr "$POD" 8080)" && { fail "wipe: $peer reached $POD:8080 before admission (after ${i}s)"; break; }
	sleep 1
done
[ "$i" -eq "$UNADMITTED_HOLD" ] && pass "wipe: the old voter's address takes no Raft or data traffic for ${UNADMITTED_HOLD}s"
term1="$(field "$(st "$(leader_pod)")" .term)"
[ "$term1" = "$term0" ] && pass "wipe: no election while the wiped pod waits (term $term0)" || fail "wipe: no election while the wiped pod waits" "term $term0 -> $term1"
code="$(kn get pod "$POD" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')"
[ "$code" = False ] && pass "wipe: $POD is not Ready" || fail "wipe: $POD is not Ready" "$code"
cport="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
kn port-forward svc/storage-coordinator "$cport:8080" >/dev/null 2>&1 &
pf=$!
wait_for 10 curl -s -o /dev/null "http://127.0.0.1:$cport/healthz"
admin="$(curl -s -m 5 -H "Authorization: Bearer $(kn get secret storage-operator-token -o jsonpath='{.data.token}' | base64 -d)" "http://127.0.0.1:$cport/admin/nodes")"
kill "$pf" 2>/dev/null
jq -e --arg p "$POD" '.nodes[] | select(.nodeId == $p) | .admitted == false' <<<"$admin" >/dev/null 2>&1 &&
	pass "wipe: the coordinator lists $POD unadmitted" || fail "wipe: the coordinator lists $POD unadmitted" "$admin"

echo "== 4. refusals change nothing"
other="storage-shard-$SHARD-0"
[ "$other" = "$POD" ] && other="storage-shard-$SHARD-1"
cfg0="$(st "$(leader_pod)" | jq -c .configuration)"
POD="$other" replace_job ""
rc=$?
[ "$rc" = 2 ] && [ "$(st "$(leader_pod)" | jq -c .configuration)" = "$cfg0" ] &&
	pass "refuse: replacing $other (still running) exits 2, configuration unchanged" ||
	fail "refuse: replacing $other (still running) exits 2, configuration unchanged" "exit $rc"

echo "== 5. replace, crashing after every step"
for step in "${STEPS[@]}"; do
	replace_job "$step"
	rc=$?
	ph="$(mfield '.replace | fromjson? | .phase')"
	n="$(voter_count)"
	cfg="$(st "$(leader_pod)" | jq -c '[.configuration.servers[].id]')"
	if [ "$rc" = 1 ] && [ "${n:-9}" -le 3 ]; then
		pass "crash at $step: exit 1, phase ${ph:-none}, $n voters"
	else
		fail "crash at $step: exit 1, at most 3 voters" "exit $rc, phase $ph, voters $n, config $cfg"
	fi
	adm="$(mfield '.admitted | fromjson? | .["'"$POD"'"]')"
	if [ -n "$adm" ] && jq -e --arg o "$old_id" 'index($o)' <<<"$cfg" >/dev/null; then
		fail "crash at $step: admitted while $old_id is still in the configuration" "$cfg"
	fi
	if [ "$step" = recorded-removed ]; then # a leader change in the two-voter phase
		kn exec "$(leader_pod)" -c storagenode -- /storagectl transfer-leader >/dev/null 2>&1 &&
			pass "leader change mid-replace (after $step)" || fail "leader change mid-replace (after $step)"
	fi
done
replace_job ""
rc=$?
[ "$rc" = 0 ] && pass "replace: the final run completes" || fail "replace: the final run completes" "exit $rc: $(tail -5 "$tmp/replace-final.log")"
for ph in PLANNED REMOVED ADMITTED NONVOTER VOTER; do
	cat "$tmp"/replace-*.log | grep -q "phase=$ph" && pass "replace: phase $ph logged" || fail "replace: phase $ph logged"
done

echo "== 6. after: three voters, the manifest, the coordinator, the data"
wait_for 120 group_ok && pass "after: three committed voters, all caught up" || fail "after: three committed voters, all caught up"
want="$(mfield '.peerIds | fromjson | sort | join(" ")')"
[ "$(committed_voters "$(leader_pod)")" = "$want" ] && grep -q -- "$new_id" <<<"$want" && ! grep -q -- "$old_id" <<<"$want" &&
	pass "after: manifest peerIds = committed voters, with $new_id and without $old_id" ||
	fail "after: manifest peerIds = committed voters" "peerIds $want; committed $(committed_voters "$(leader_pod)")"
jq -e --arg o "$old_id" --arg p "$POD" --arg i "${new_id#*/}" '(.removed | fromjson | index($o)) and (.admitted | fromjson | .[$p] == $i) and (.replace == null)' <<<"$(manifest)" >/dev/null 2>&1 &&
	pass "after: removed has $old_id, admitted[$POD] is the new incarnation, no replace in progress" ||
	fail "after: removed, admitted, no replace in progress" "$(manifest)"
wait_for 60 bash -c "[ \"\$(kubectl --context $CTX -n $NS get pod $POD -o jsonpath='{.status.conditions[?(@.type==\"Ready\")].status}')\" = True ]" &&
	pass "after: $POD is Ready" || fail "after: $POD is Ready"
replace_job ""
rc=$?
[ "$rc" = 0 ] && [ "$(voter_count)" = 3 ] && grep -q "already" "$tmp/replace-final.log" &&
	pass "after: a re-run finds the replacement done and changes nothing" || fail "after: a re-run changes nothing" "exit $rc"
helm --kube-context "$CTX" upgrade "$RELEASE" "$CHART" -n "$NS" --wait --wait-for-jobs --timeout "$INSTALL_TIMEOUT" >"$tmp/helm2.log" 2>&1 &&
	pass "#21: helm upgrade after the replacement: the bootstrap Job verifies the updated peerIds" ||
	fail "#21: helm upgrade after the replacement" "$(tail -10 "$tmp/helm2.log")"

echo "== 7. no acknowledged write lost"
kn delete job "$load_job" --wait=false >/dev/null 2>&1 # stops the writers; its ledger is saved on SIGTERM
wait_for 60 bash -c "! kubectl --context $CTX -n $NS get pods -l job-name=$load_job -o jsonpath='{.items[0].status.phase}' | grep -q Running"
out="$(run_job "s42-verify-$load_job" verify --coordinator "$COORD_URL" --ledger "/ledger/$load_job.json" --json)"
rc=$?
[ "$rc" = 0 ] && jq -e '.ok and (.verdict.lost | length) == 0 and .verdict.checked > 0' <<<"$out" >/dev/null 2>&1 &&
	pass "ledger: $(jq '.verdict.checked' <<<"$out") acknowledged writes, 0 lost" || fail "ledger: 0 acknowledged writes lost" "exit $rc: $(head -c 600 <<<"$out")"
out="$(run_job "s42-check-$load_job" check --coordinator "$COORD_URL" --json)"
rc=$?
[ "$rc" = 0 ] && pass "invcheck: clean and quiescent" || fail "invcheck: clean and quiescent" "exit $rc: $(head -c 400 <<<"$out")"
load_job=""

finish

#!/usr/bin/env bash
# Verify check for S0.3 (Raft spike). Run from the repo root with the kind cluster up:
#   spike/raft-kill.sh
#   ELECTION_MAX_SECONDS=5 LOAD_OPS=5000 KEEP_NAMESPACE=1 spike/raft-kill.sh
# It builds the spike image, loads it into kind, deploys a fresh three-pod group in
# namespace raft-spike, bootstraps it once, kills the leader and checks recovery.
# Writes evidence to docs/evidence/spike/raft-<host>.txt. Exits non-zero if any
# check fails; prints one PASS/FAIL line per check.
#
# Interface under test (all under spike/raft/):
#   Dockerfile                 builds the binary /raft-spike (context: repo root)
#   k8s/storage-spike.yaml     Namespace raft-spike, headless Service storage-spike
#                              (publishNotReadyAddresses), StatefulSet storage-spike
#                              (3 replicas, Parallel, role=data, one pod per node,
#                              PVC "data" for log/stable/snapshots), image
#                              reliefgrid/raft-spike:dev with imagePullPolicy Never,
#                              container "spike", raft :7000, http :8080, GET /healthz
#   k8s/bootstrap-job.yaml     Job storage-spike-bootstrap (backoffLimit 0): checks that
#                              no peer has Raft state, then bootstraps one pod with the
#                              explicit three-voter DNS configuration; otherwise it exits
#                              non-zero and logs "refused"
#   /raft-spike ctl ...        client for the pod's own HTTP endpoint (localhost:8080),
#                              run through kubectl exec; prints JSON on stdout:
#     ctl status               GET /status  {id, advertise, state, leader, leaderId, term,
#                                            appliedIndex, commitIndex, lastIndex,
#                                            servers:[{id,address,suffrage}],
#                                            memory:{rssBytes,cgroupBytes}}
#     ctl value <key>          GET /value   {key, value, version}  (local FSM read)
#     ctl cas <key> <expectedVersion> <value>
#                              POST /cas    {key, value, version}
#                              exit 3 on a version conflict, 4 when not the leader
#     ctl load --ops N --concurrency C
#                              sequential CAS writes on per-worker keys through the leader
#                                           {ops, errors, seconds, opsPerSecond}
#     ctl bootstrap            asks this pod to bootstrap; exit non-zero, "refused" on
#                              stderr, when it already has Raft state
#   FINDINGS.md                the spike's findings (sections checked below)
set -uo pipefail

CLUSTER=reliefgrid
CTX="kind-$CLUSTER"
NS=raft-spike
APP=storage-spike
IMAGE=reliefgrid/raft-spike:dev
DIR=spike/raft
ELECTION_MAX_SECONDS="${ELECTION_MAX_SECONDS:-10}"
REJOIN_MAX_SECONDS="${REJOIN_MAX_SECONDS:-120}"
PRE_BOOTSTRAP_SECONDS="${PRE_BOOTSTRAP_SECONDS:-15}"
LOAD_OPS="${LOAD_OPS:-2000}"
LOAD_CONCURRENCY="${LOAD_CONCURRENCY:-8}"
EVIDENCE="docs/evidence/spike/raft-$(hostname -s).txt"

failures=0
pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; [ -n "${2:-}" ] && printf '      %s\n' "$2"; failures=$((failures + 1)); }
check() { # check <description> <command...>
	local desc="$1"
	shift
	if "$@" >/dev/null 2>&1; then pass "$desc"; else fail "$desc"; fi
}
finish() {
	if [ "${KEEP_NAMESPACE:-0}" != 1 ]; then
		k delete namespace "$NS" --ignore-not-found --wait=false >/dev/null 2>&1
	fi
	echo
	if [ "$failures" -eq 0 ]; then
		echo "raft-kill: all checks passed"
	else
		echo "raft-kill: $failures check(s) failed"
		exit 1
	fi
	exit 0
}
abort() { # abort <description> [detail]: a failure that makes later checks meaningless
	fail "$@"
	finish
}

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

k() { kubectl --context "$CTX" "$@"; }
kn() { k -n "$NS" "$@"; }
pods() { printf '%s-0 %s-1 %s-2' "$APP" "$APP" "$APP"; }
fqdn() { printf '%s.%s.%s.svc.cluster.local:7000' "$1" "$APP" "$NS"; }
ctl() { # ctl <pod> <args...>: run the spike client inside the pod
	local pod="$1"
	shift
	kn exec "$pod" -c spike -- /raft-spike ctl "$@"
}
st() { ctl "$1" status 2>/dev/null; }                # status JSON, empty if unreachable
field() { jq -r "$2 // empty" <<<"$1" 2>/dev/null; } # field <json> <jq path>
now() { date +%s.%N; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN { printf "%.1f", b - a }'; }
leaders() { # print "<pod> <term>" for every reachable pod in state Leader
	local p s
	for p in "$@"; do
		s="$(st "$p")"
		[ "$(field "$s" .state)" = Leader ] && echo "$p $(field "$s" .term)"
	done
}
wait_one_leader() { # wait_one_leader <seconds> <pods...>: print "<pod> <term>" or nothing
	local deadline=$((SECONDS + $1)) l
	shift
	while [ "$SECONDS" -lt "$deadline" ]; do
		l="$(leaders "$@")"
		[ "$(grep -c . <<<"$l")" -eq 1 ] && { echo "$l"; return 0; }
		sleep 0.5
	done
	return 1
}
dns_only() { # dns_only <address>: an FQDN in this namespace on :7000, not an IP
	[[ "$1" =~ ^$APP-[0-2]\.$APP\.$NS\.svc\.cluster\.local:7000$ ]]
}

for tool in kind kubectl docker jq; do
	command -v "$tool" >/dev/null || abort "tools: $tool is installed"
done
kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || abort "cluster: '$CLUSTER' is running (make cluster-up)"

# --- 1. Pins and files ------------------------------------------------------------
mod_version() { awk -v m="$1" '$1 == m { print $2 } $1 == "require" && $2 == m { print $3 }' go.mod; }
[ "$(mod_version github.com/hashicorp/raft)" = v1.8.0 ] &&
	pass "go.mod: hashicorp/raft pinned to v1.8.0 (M2)" ||
	fail "go.mod: hashicorp/raft pinned to v1.8.0 (M2)" "got '$(mod_version github.com/hashicorp/raft)'"
for m in github.com/hashicorp/raft-boltdb/v2 modernc.org/sqlite; do
	v="$(mod_version "$m")"
	[[ "$v" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] && pass "go.mod: $m pinned to a release ($v)" || fail "go.mod: $m pinned to a release" "got '$v'"
done
for f in Dockerfile k8s/storage-spike.yaml k8s/bootstrap-job.yaml FINDINGS.md; do
	check "files: $DIR/$f exists" test -f "$DIR/$f"
done
# The startup path must not bootstrap: the call lives only in a *bootstrap*.go file
# (sections 4 and 10 check the runtime behaviour).
calls="$(grep -rl --include='*.go' 'BootstrapCluster(' "$DIR" 2>/dev/null)"
stray="$(grep -v '/[^/]*bootstrap[^/]*\.go$' <<<"$calls" | grep .)"
if [ -n "$calls" ] && [ -z "$stray" ]; then
	pass "code: BootstrapCluster is called only from a *bootstrap*.go file"
else
	fail "code: BootstrapCluster is called only from a *bootstrap*.go file" "calls in: $(tr '\n' ' ' <<<"${stray:-none}")"
fi

# --- 2. Build and load the image --------------------------------------------------
if docker build -q -f "$DIR/Dockerfile" -t "$IMAGE" . >"$tmp/build.out" 2>&1; then
	pass "image: $IMAGE builds"
else
	abort "image: $IMAGE builds" "$(tail -5 "$tmp/build.out" | tr '\n' '|')"
fi
check "image: kind load docker-image $IMAGE" kind load docker-image --name "$CLUSTER" "$IMAGE"

# --- 3. Deploy a fresh group ------------------------------------------------------
k delete namespace "$NS" --ignore-not-found --wait=true >/dev/null 2>&1
if k apply -f "$DIR/k8s/storage-spike.yaml" >"$tmp/apply.out" 2>&1; then
	pass "deploy: storage-spike.yaml applies"
else
	abort "deploy: storage-spike.yaml applies" "$(tr '\n' '|' <"$tmp/apply.out")"
fi
sts() { kn get statefulset "$APP" -o jsonpath="$1" 2>/dev/null; }
[ "$(sts '{.spec.replicas}')" = 3 ] && pass "statefulset: 3 replicas" || fail "statefulset: 3 replicas" "got '$(sts '{.spec.replicas}')'"
[ "$(sts '{.spec.podManagementPolicy}')" = Parallel ] && pass "statefulset: podManagementPolicy Parallel" || fail "statefulset: podManagementPolicy Parallel"
[ "$(sts '{.spec.serviceName}')" = "$APP" ] && pass "statefulset: serviceName $APP" || fail "statefulset: serviceName $APP"
[ "$(sts '{.spec.volumeClaimTemplates[*].metadata.name}')" = data ] && pass "statefulset: one PVC template named data" || fail "statefulset: one PVC template named data"
svc() { kn get service "$APP" -o jsonpath="$1" 2>/dev/null; }
[ "$(svc '{.spec.clusterIP}')" = None ] && pass "service: headless" || fail "service: headless"
[ "$(svc '{.spec.publishNotReadyAddresses}')" = true ] && pass "service: publishNotReadyAddresses true" || fail "service: publishNotReadyAddresses true"

if kn wait --for=condition=Ready pod -l "app=$APP" --timeout=180s >/dev/null 2>&1; then
	pass "pods: 3 Ready within 3m"
else
	kn get pods -o wide
	abort "pods: 3 Ready within 3m"
fi
nodes=""
for p in $(pods); do
	n="$(kn get pod "$p" -o jsonpath='{.spec.nodeName}' 2>/dev/null)"
	[ "$(k get node "$n" -o jsonpath='{.metadata.labels.role}' 2>/dev/null)" = data ] &&
		pass "placement: $p runs on a role=data node ($n)" || fail "placement: $p runs on a role=data node" "node '$n'"
	nodes+="$n"$'\n'
	[ "$(kn get pvc "data-$p" -o jsonpath='{.status.phase}' 2>/dev/null)" = Bound ] &&
		pass "storage: PVC data-$p is Bound" || fail "storage: PVC data-$p is Bound"
done
[ "$(sort -u <<<"$nodes" | grep -c .)" -eq 3 ] && pass "placement: one pod per data node" || fail "placement: one pod per data node" "$(tr '\n' ' ' <<<"$nodes")"

# --- 4. No self-bootstrap, DNS advertise ------------------------------------------
echo "waiting ${PRE_BOOTSTRAP_SECONDS}s to confirm no pod bootstraps itself..."
sleep "$PRE_BOOTSTRAP_SECONDS"
for p in $(pods); do
	s="$(st "$p")"
	[ -n "$s" ] || { fail "status: $p answers ctl status"; continue; }
	if [ "$(field "$s" .state)" != Leader ] && [ -z "$(field "$s" .leader)" ] &&
		[ "$(field "$s" .term)" = 0 ] && [ "$(field "$s" .lastIndex)" = 0 ]; then
		pass "no self-bootstrap: $p has no leader, term 0, empty log"
	else
		fail "no self-bootstrap: $p has no leader, term 0, empty log" "$s"
	fi
	[ "$(field "$s" .advertise)" = "$(fqdn "$p")" ] &&
		pass "advertise: $p advertises $(fqdn "$p")" || fail "advertise: $p advertises $(fqdn "$p")" "got '$(field "$s" .advertise)'"
done

# --- 5. Bootstrap once, exactly one leader -----------------------------------------
k apply -f "$DIR/k8s/bootstrap-job.yaml" >/dev/null 2>&1
if kn wait --for=condition=Complete job/"$APP-bootstrap" --timeout=120s >/dev/null 2>&1; then
	pass "bootstrap: Job completes"
else
	kn logs job/"$APP-bootstrap" 2>&1 | tail -5
	abort "bootstrap: Job completes"
fi
if l="$(wait_one_leader 30 $(pods))"; then
	read -r leader term <<<"$l"
	pass "election: exactly one leader within 30s ($leader, term $term)"
else
	abort "election: exactly one leader within 30s" "leaders: $(leaders $(pods) | tr '\n' ' ')"
fi
for p in $(pods); do
	s="$(st "$p")"
	[ "$(field "$s" .leaderId)" = "$leader" ] && [ "$(field "$s" .leader)" = "$(fqdn "$leader")" ] &&
		pass "election: $p sees leader $leader at its DNS address" ||
		fail "election: $p sees leader $leader at its DNS address" "leaderId='$(field "$s" .leaderId)' leader='$(field "$s" .leader)'"
done
s="$(st "$leader")"
servers="$(jq -r '.servers[] | "\(.id) \(.address) \(.suffrage)"' <<<"$s" 2>/dev/null | sort)"
want="$(for p in $(pods); do echo "$p $(fqdn "$p") Voter"; done)"
[ "$servers" = "$want" ] && pass "config: three voters, each at its DNS address" || fail "config: three voters, each at its DNS address" "$(tr '\n' '|' <<<"$servers")"
bad=""
for a in $(jq -r '.servers[].address' <<<"$s" 2>/dev/null); do dns_only "$a" || bad+="$a "; done
[ -z "$bad" ] && pass "config: no server address is a pod IP" || fail "config: no server address is a pod IP" "$bad"

# --- 6. CAS through the leader ------------------------------------------------------
out="$(ctl "$leader" cas spike 0 alpha 2>&1)"
[ "$(field "$out" .version)" = 1 ] && [ "$(field "$out" .value)" = alpha ] &&
	pass "cas: version 0 -> 1 (alpha) through the leader" || fail "cas: version 0 -> 1 (alpha) through the leader" "$out"
ctl "$leader" cas spike 0 stale >/dev/null 2>&1
rc=$?
[ "$rc" -eq 3 ] && pass "cas: a stale expected version is a conflict (exit 3)" || fail "cas: a stale expected version is a conflict (exit 3)" "exit $rc"
follower="$(for p in $(pods); do [ "$p" != "$leader" ] && echo "$p"; done | head -1)"
ctl "$follower" cas spike 1 elsewhere >/dev/null 2>&1
rc=$?
[ "$rc" -eq 4 ] && pass "cas: a follower refuses writes (exit 4)" || fail "cas: a follower refuses writes (exit 4)" "exit $rc"
for p in $(pods); do
	ok=""
	for _ in $(seq 20); do
		v="$(ctl "$p" value spike 2>/dev/null)"
		[ "$(field "$v" .value)/$(field "$v" .version)" = alpha/1 ] && { ok=1; break; }
		sleep 0.5
	done
	[ -n "$ok" ] && pass "replication: $p reads alpha@1" || fail "replication: $p reads alpha@1" "$v"
done

# --- 7. Load: no spurious elections with default timeouts ---------------------------
load="$(ctl "$leader" load --ops "$LOAD_OPS" --concurrency "$LOAD_CONCURRENCY" 2>&1)"
[ "$(field "$load" .ops)" = "$LOAD_OPS" ] && [ "$(field "$load" .errors)" = 0 ] &&
	pass "load: $LOAD_OPS CAS writes, 0 errors ($(field "$load" .opsPerSecond) ops/s)" ||
	fail "load: $LOAD_OPS CAS writes, 0 errors" "$load"
for p in $(pods); do
	s="$(st "$p")"
	[ "$(field "$s" .term)" = "$term" ] && [ "$(field "$s" .leaderId)" = "$leader" ] &&
		pass "load: $p still at term $term under leader $leader" ||
		fail "load: $p still at term $term under leader $leader" "term $(field "$s" .term), leader '$(field "$s" .leaderId)'"
done
statuses_before="$(for p in $(pods); do st "$p"; done)"

# --- 8. Kill the leader -------------------------------------------------------------
old="$leader"
old_term="$term"
old_pvc="$(kn get pvc "data-$old" -o jsonpath='{.metadata.uid}' 2>/dev/null)"
survivors="$(for p in $(pods); do [ "$p" != "$old" ] && echo "$p"; done | tr '\n' ' ')"
t0="$(now)"
kn delete pod "$old" --wait=false >/dev/null 2>&1
election=""
deadline=$((SECONDS + ELECTION_MAX_SECONDS + 30)) # keep measuring past the limit
while [ "$SECONDS" -lt "$deadline" ]; do
	l="$(leaders $survivors)"
	if [ "$(grep -c . <<<"$l")" -eq 1 ] && [ "$(cut -d' ' -f2 <<<"$l")" -gt "$old_term" ]; then
		election="$(since "$t0")"
		read -r leader term <<<"$l"
		break
	fi
	sleep 0.2
done
if [ -z "$election" ]; then
	abort "failover: a survivor becomes leader at a higher term" "none within $((ELECTION_MAX_SECONDS + 30))s"
fi
if awk -v e="$election" -v m="$ELECTION_MAX_SECONDS" 'BEGIN { exit !(e <= m) }'; then
	pass "failover: $leader elected at term $term in ${election}s (limit ${ELECTION_MAX_SECONDS}s)"
else
	fail "failover: new leader elected within ${ELECTION_MAX_SECONDS}s" "took ${election}s ($leader, term $term)"
fi

# --- 9. The value survives; the group still writes ----------------------------------
v="$(ctl "$leader" value spike 2>/dev/null)"
[ "$(field "$v" .value)/$(field "$v" .version)" = alpha/1 ] &&
	pass "durability: new leader $leader reads alpha@1" || fail "durability: new leader $leader reads alpha@1" "$v"
out="$(ctl "$leader" cas spike 1 beta 2>&1)"
[ "$(field "$out" .version)" = 2 ] &&
	pass "failover: 2/3 group commits CAS 1 -> 2 (beta)" || fail "failover: 2/3 group commits CAS 1 -> 2 (beta)" "$out"

# --- 10. The old pod rejoins as a follower and catches up ----------------------------
if kn wait --for=condition=Ready pod "$old" --timeout="${REJOIN_MAX_SECONDS}s" >/dev/null 2>&1; then
	pass "rejoin: $old is Ready again"
else
	abort "rejoin: $old is Ready again within ${REJOIN_MAX_SECONDS}s"
fi
[ "$(kn get pvc "data-$old" -o jsonpath='{.metadata.uid}' 2>/dev/null)" = "$old_pvc" ] &&
	pass "rejoin: $old reattached the same PVC" || fail "rejoin: $old reattached the same PVC"
caught=""
deadline=$((SECONDS + REJOIN_MAX_SECONDS))
while [ "$SECONDS" -lt "$deadline" ]; do
	s="$(st "$old")"
	ls_="$(st "$leader")"
	if [ "$(field "$s" .state)" = Follower ] && [ "$(field "$s" .leaderId)" = "$leader" ] &&
		[ -n "$(field "$s" .appliedIndex)" ] && [ "$(field "$s" .appliedIndex)" = "$(field "$ls_" .appliedIndex)" ]; then
		caught=1
		break
	fi
	sleep 1
done
[ -n "$caught" ] &&
	pass "rejoin: $old is a follower of $leader at applied index $(field "$s" .appliedIndex)" ||
	fail "rejoin: $old is a follower of $leader at the leader's applied index" \
		"old: $(field "$s" .state) leader='$(field "$s" .leaderId)' applied=$(field "$s" .appliedIndex); leader applied=$(field "$ls_" .appliedIndex)"
[ "$(field "$s" .term)" = "$term" ] && pass "rejoin: $old is at the cluster term $term" || fail "rejoin: $old is at the cluster term $term" "got $(field "$s" .term)"
v="$(ctl "$old" value spike 2>/dev/null)"
[ "$(field "$v" .value)/$(field "$v" .version)" = beta/2 ] &&
	pass "rejoin: $old reads beta@2" || fail "rejoin: $old reads beta@2" "$v"

# --- 11. A second bootstrap is refused ----------------------------------------------
kn delete job "$APP-bootstrap" --wait=true >/dev/null 2>&1
k apply -f "$DIR/k8s/bootstrap-job.yaml" >/dev/null 2>&1
jobstate=""
for _ in $(seq 60); do
	jobstate="$(kn get job "$APP-bootstrap" -o jsonpath='{.status.succeeded}/{.status.failed}' 2>/dev/null)"
	[ "$jobstate" != / ] && [ -n "$jobstate" ] && break
	sleep 1
done
[ "$jobstate" = /1 ] && pass "re-bootstrap: the Job fails" || fail "re-bootstrap: the Job fails" "succeeded/failed = '$jobstate'"
kn logs job/"$APP-bootstrap" >"$tmp/rejob.log" 2>&1
grep -qi refused "$tmp/rejob.log" && pass "re-bootstrap: the Job logs 'refused'" || fail "re-bootstrap: the Job logs 'refused'" "$(tail -3 "$tmp/rejob.log" | tr '\n' '|')"
for p in $(pods); do
	if ctl "$p" bootstrap >"$tmp/boot-$p" 2>&1; then
		fail "re-bootstrap: $p refuses a direct bootstrap" "it exited 0"
	elif grep -qi refused "$tmp/boot-$p"; then
		pass "re-bootstrap: $p refuses a direct bootstrap"
	else
		fail "re-bootstrap: $p refuses a direct bootstrap" "$(tr '\n' '|' <"$tmp/boot-$p")"
	fi
done
sleep 3
l="$(leaders $(pods))"
[ "$l" = "$leader $term" ] && pass "re-bootstrap: still one leader ($leader) at term $term" || fail "re-bootstrap: still one leader ($leader) at term $term" "$(tr '\n' ' ' <<<"$l")"
for p in $(pods); do
	s="$(st "$p")"
	[ "$(jq '.servers | length' <<<"$s" 2>/dev/null)" = 3 ] && [ "$(field "$s" .leaderId)" = "$leader" ] &&
		pass "re-bootstrap: $p keeps the three-voter config under $leader" ||
		fail "re-bootstrap: $p keeps the three-voter config under $leader" "$s"
done
v="$(ctl "$leader" value spike 2>/dev/null)"
[ "$(field "$v" .value)/$(field "$v" .version)" = beta/2 ] &&
	pass "re-bootstrap: value is still beta@2" || fail "re-bootstrap: value is still beta@2" "$v"

# --- 12. Evidence and findings -------------------------------------------------------
mkdir -p "$(dirname "$EVIDENCE")"
{
	echo "# Raft spike, S0.3"
	echo "host:        $(hostname -s)"
	echo "date:        $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "kernel:      $(uname -r)"
	echo "commit:      $(git rev-parse --short HEAD)"
	echo "raft:        $(mod_version github.com/hashicorp/raft)"
	echo "election:    ${election}s from leader pod delete to a survivor reporting Leader (term $old_term -> $term)"
	echo "             old leader $old, new leader $leader; polled through kubectl exec (~0.2-0.5s resolution)"
	echo "load:        $load"
	echo
	echo "## kubectl get pods -o wide"
	kn get pods -o wide
	echo
	echo "## pod memory (from /status, after load and failover)"
	for p in $(pods); do
		s="$(st "$p")"
		printf '%s rss=%s cgroup=%s\n' "$p" \
			"$(numfmt --to=iec "$(field "$s" .memory.rssBytes)" 2>/dev/null)" \
			"$(numfmt --to=iec "$(field "$s" .memory.cgroupBytes)" 2>/dev/null)"
	done
	echo
	echo "## statuses before the kill"
	echo "$statuses_before"
	echo
	echo "## statuses at the end"
	for p in $(pods); do st "$p"; done
} 2>&1 | sed 's/[[:space:]]*$//' >"$EVIDENCE"
cat "$EVIDENCE"
check "evidence: $EVIDENCE records the election time" grep -Eq '^election: +[0-9.]+s' "$EVIDENCE"
rows="$(grep -cE "^$APP-[0-2] rss=[0-9.]+[KMG]i? cgroup=[0-9.]+[KMG]i?$" "$EVIDENCE")"
[ "$rows" -eq 3 ] && pass "evidence: RAM recorded for all three pods" || fail "evidence: RAM recorded for all three pods" "got $rows rows"

for section in 'Election time' 'DNS advertise' 'Spurious elections' 'Pod RAM' 'Plan impact'; do
	check "FINDINGS.md: has a '## $section' section" grep -Eq "^## $section" "$DIR/FINDINGS.md"
done
for m in M2 M3 M4; do
	check "FINDINGS.md: says what the spike means for $m" grep -q "\\b$m\\b" "$DIR/FINDINGS.md"
done

finish
